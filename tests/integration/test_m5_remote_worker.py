from __future__ import annotations

import json
import os
import re
import select
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
import hashlib
import urllib.error
import urllib.request
from urllib.parse import urljoin
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.evolution.autonomous import ImprovementCampaignService
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.remote.control import provision_worker
from agentic_runtime.remote.protocol import RemoteClientError, WorkerClient, WorkerIdentity
from agentic_runtime.remote.worker import BwrapSandboxRunner, DEFAULT_RESOURCE_POLICY, RemoteWorker


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def db_connect():
    db = connect(os.environ["M3_TEST_DATABASE_URL"])
    db.autocommit = True
    role = os.environ.get("M3_TEST_DATABASE_ROLE")
    if role:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", role):
            raise ValueError("unsafe test role")
        db.execute(f'SET ROLE "{role}"')
    return db


class RemoteWorkerProcessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get("M3_TEST_DATABASE_URL"):
            raise RuntimeError("M3_TEST_DATABASE_URL is required")
        cls.db = db_connect()

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def setUp(self):
        self.db.execute("TRUNCATE runtime.campaigns, runtime.workers, runtime.executors CASCADE")
        self.db.execute("TRUNCATE runtime.worker_identities CASCADE")
        self.temp = tempfile.TemporaryDirectory(prefix="agentic-m5-")
        root = Path(self.temp.name)
        self.artifact_root = root / "artifacts"
        self.worker_root = root / "worker"
        self.home = root / "home"
        self.home.mkdir(mode=0o700)
        self.server, self.port = self._start_control_plane()

    def tearDown(self):
        if getattr(self, "server", None):
            self.server.terminate()
            try:
                self.server.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.server.kill(); self.server.wait(timeout=3)
            if self.server.stdout: self.server.stdout.close()
        self.temp.cleanup()

    def _start_control_plane(self):
        sock = socket.socket(); sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]; sock.close()
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
               "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1",
               "M5_CONTROL_DATABASE_URL": os.environ["M5_CONTROL_DATABASE_URL"],
               "M5_CONTROL_DATABASE_ROLE": os.environ.get("M5_CONTROL_DATABASE_ROLE", "agentic_runtime_runtime"),
               "M5_CONTROL_ADMIN_TOKEN": os.environ["M5_CONTROL_ADMIN_TOKEN"],
               "M5_ARTIFACT_ROOT": str(self.artifact_root), "M5_LISTEN_HOST": "127.0.0.1",
               "M5_LISTEN_PORT": str(port), "M5_LEASE_SECONDS": "2",
               "M5_HEARTBEAT_SUSPECT_SECONDS": "5"}
        proc = subprocess.Popen([sys.executable, "-m", "agentic_runtime.remote.control_server"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            start_new_session=True)
        deadline = time.monotonic() + 10
        lines = []
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError(f"control plane exited: {proc.stdout.read()}")
            ready, _, _ = select.select([proc.stdout], [], [], .1)
            if ready:
                line = proc.stdout.readline(); lines.append(line)
                if line.startswith("READY:"):
                    return proc, port
        proc.terminate(); proc.wait(timeout=3)
        raise AssertionError(f"control plane did not become ready: {''.join(lines)}")

    def _campaign_task(self,worker_class="REMOTE_PERSISTENT"):
        campaign, goal, task, worker_id = ident("camp"), ident("goal"), ident("task"), ident("worker")
        co = Coordinator(self.db)
        co.create_campaign(campaign, idempotency_key=ident("key"), description="M5 neutral remote execution",
            created_by="m5-test", budget={"max_cost": 10, "max_wall_time": 60})
        co.create_goal(goal, campaign, description="Produce a verified structured result",
            mission_ref="mission:generic", created_by="m5-test")
        contract = {"type": "object", "required": ["ok", "value"], "properties": {
            "ok": {"type": "boolean"}, "value": {"type": "string"}}}
        co.create_task(task, campaign, task_type="generic_agent_task", idempotency_key=ident("task-key"),
            goal_id=goal, required_capabilities=["generic_agent_task"],
            budget={"resource_class": "deterministic_compute_job"}, output_contract=contract,
            created_by="m5-test")
        token = provision_worker(self.db, worker_id=worker_id,
            capabilities=["generic_agent_task"], task_types=["generic_agent_task"],
            resource_classes=["deterministic_compute_job"],
            max_resource_policy={"cpu_seconds": 10, "memory_bytes": 1073741824,
                "process_count": 128, "wall_time_seconds": 15, "max_sandbox_bytes": 33554432},
            allowed_tools=["filesystem"], ttl_seconds=600, created_by="m5-test",worker_class=worker_class)
        return task, worker_id, token

    def _remote_identity(self, worker_id: str):
        token = provision_worker(self.db, worker_id=worker_id,
            capabilities=["generic_agent_task"], task_types=["generic_agent_task"],
            resource_classes=["deterministic_compute_job"],
            max_resource_policy={"cpu_seconds": 10, "memory_bytes": 1073741824,
                "process_count": 128, "wall_time_seconds": 15, "max_sandbox_bytes": 33554432},
            ttl_seconds=600, created_by="m5-test")
        client = WorkerClient(f"http://127.0.0.1:{self.port}", WorkerIdentity.new_instance(worker_id, token))
        client.register(software_version="m5-test/1", capabilities=["generic_agent_task"],
            task_types=["generic_agent_task"], resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds": 10, "memory_bytes": 1073741824,
                "process_count": 128, "wall_time_seconds": 15, "max_sandbox_bytes": 33554432})
        return token, client

    def _worker_env(self, worker_id: str, token: str, **overrides):
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1",
            "HOME": str(self.home), "LANG": "C.UTF-8",
            "M5_CONTROL_ENDPOINT": f"http://127.0.0.1:{self.port}",
            "M5_WORKER_ID": worker_id, "M5_WORKER_TOKEN": token,
            "M5_WORKER_ROOT": str(self.worker_root),
            "M5_WORKER_CAPABILITIES": '["generic_agent_task"]',
            "M5_WORKER_TASK_TYPES": '["generic_agent_task"]',
            "M5_WORKER_RESOURCE_CLASSES": '["deterministic_compute_job"]',
            "M5_WORKER_RESOURCE_POLICY": json.dumps({"cpu_seconds": 10, "memory_bytes": 1073741824,
                "process_count": 128, "wall_time_seconds": 15, "max_sandbox_bytes": 33554432}),
            "M5_WORKER_ONCE": "1"}
        env.update(overrides)
        return env

    def _run_worker(self, worker_id: str, token: str, **overrides):
        return subprocess.run([sys.executable, "-m", "agentic_runtime.remote.worker"], cwd=ROOT,
            env=self._worker_env(worker_id,token,**overrides), capture_output=True, text=True, timeout=30, check=False)

    def test_worker_class_is_persisted_and_instance_capacity_is_enforced(self):
        first_task,worker_id,token=self._campaign_task(worker_class="LOCAL")
        row=self.db.execute("SELECT campaign_id,goal_id FROM runtime.tasks WHERE task_id=%s",(first_task,)).fetchone()
        second_task=ident("task")
        Coordinator(self.db).create_task(second_task,row["campaign_id"],task_type="generic_agent_task",
            idempotency_key=ident("task-key"),goal_id=row["goal_id"],
            required_capabilities=["generic_agent_task"],
            budget={"resource_class":"deterministic_compute_job"},created_by="capacity-test")
        client=WorkerClient(f"http://127.0.0.1:{self.port}",WorkerIdentity.new_instance(worker_id,token))
        registration=client.register(software_version="m6-test/1",capabilities=["generic_agent_task"],
            task_types=["generic_agent_task"],resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds":10,"memory_bytes":1073741824,"process_count":128,
                "wall_time_seconds":15,"max_sandbox_bytes":33554432},worker_class="LOCAL",max_concurrency=1)
        self.assertEqual(registration["worker_class"],"LOCAL")
        leased=client.claim("capacity-first")["task"]
        self.assertIsNotNone(leased)
        self.assertIsNone(client.claim("capacity-overflow")["task"])
        persisted=self.db.execute("SELECT worker_class,max_concurrency FROM runtime.worker_instances WHERE worker_instance_id=%s",
            (client.identity.worker_instance_id,)).fetchone()
        self.assertEqual((persisted["worker_class"],persisted["max_concurrency"]),("LOCAL",1))
        queued=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(second_task,)).fetchone()["status"]
        self.assertEqual(queued,"QUEUED")

    def test_two_node_remote_task_persists_attempt_artifact_and_acceptance(self):
        task, worker_id, token = self._campaign_task()
        worker = self._run_worker(worker_id, token)
        self.assertEqual(worker.returncode, 0, worker.stderr + worker.stdout)
        result = json.loads(worker.stdout.strip().splitlines()[-1])
        self.assertEqual(result["status"], "ACCEPTED")
        state = self.db.execute("""SELECT t.status,t.result_refs,t.result_hash,a.attempt_id,
            a.worker_id,a.worker_instance_id,a.lease_epoch,a.status AS attempt_status,s.status AS sandbox_status
            FROM runtime.tasks t JOIN runtime.attempts a USING(task_id)
            JOIN runtime.sandboxes s ON s.attempt_id=a.attempt_id WHERE t.task_id=%s""", (task,)).fetchone()
        self.assertEqual(state["status"], "ACCEPTED")
        self.assertEqual(state["attempt_status"], "ACCEPTED")
        self.assertEqual(state["worker_id"], worker_id)
        self.assertTrue(state["worker_instance_id"].startswith("wi_"))
        self.assertEqual(state["sandbox_status"], "DESTROYED")
        self.assertTrue(self.db.execute("SELECT bool_and(schema_valid) AS valid FROM runtime.model_runs WHERE task_id=%s AND attempt_id=%s",
            (task,state["attempt_id"])).fetchone()["valid"])
        artifact = self.db.execute("SELECT worker_id,worker_instance_id,lease_epoch,verification_status FROM runtime.artifacts WHERE artifact_id=%s",
            (state["result_refs"][0],)).fetchone()
        self.assertEqual(artifact["worker_id"], worker_id)
        self.assertEqual(artifact["worker_instance_id"], state["worker_instance_id"])
        self.assertEqual(artifact["lease_epoch"], state["lease_epoch"])
        self.assertEqual(artifact["verification_status"], "VERIFIED")
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            pending=self.db.execute("SELECT count(*) AS n FROM runtime.outbox WHERE delivered_at IS NULL").fetchone()["n"]
            if pending==0: break
            time.sleep(.05)
        self.assertEqual(pending,0)
        self.assertGreaterEqual(self.db.execute("SELECT count(*) AS n FROM runtime.worker_heartbeats WHERE worker_id=%s",
            (worker_id,)).fetchone()["n"], 1)

    def test_durable_executor_profile_is_selected_and_attributed_by_control_plane(self):
        task,worker_id,token=self._campaign_task()
        executor="m7-route-profile-test"
        self.db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,
            configuration_ref,capabilities,location,enabled,metadata)
            VALUES (%s,'remote_worker','deterministic-profile','1','test://profile',
              '[\"generic_agent_task\"]'::jsonb,'REMOTE',true,'{}'::jsonb)""",(executor,))
        self.db.execute("UPDATE runtime.tasks SET metadata=metadata || %s WHERE task_id=%s",
            (json.dumps({"e1_evidence_executor_ref":executor}),task))
        self.db.commit()
        result=self._run_worker(worker_id,token)
        self.assertEqual(result.returncode,0,result.stderr+result.stdout)
        task_row=self.db.execute("SELECT attempt_id FROM runtime.attempts WHERE task_id=%s",(task,)).fetchone()
        route=self.db.execute("SELECT selected_executor_ref,policy_ref FROM runtime.routing_decisions WHERE task_id=%s AND attempt_id=%s",
            (task,task_row["attempt_id"])).fetchone()
        model=self.db.execute("SELECT executor_id FROM runtime.model_runs WHERE task_id=%s AND attempt_id=%s",
            (task,task_row["attempt_id"])).fetchone()
        self.assertEqual(route["selected_executor_ref"],executor)
        self.assertEqual(model["executor_id"],executor)

    def test_authentication_scope_protocol_and_forbidden_operation_are_rejected(self):
        worker_id = ident("worker")
        token, registered = self._remote_identity(worker_id)
        bad = WorkerClient(f"http://127.0.0.1:{self.port}", WorkerIdentity.new_instance(worker_id, "invalid-token"))
        with self.assertRaises(RemoteClientError) as exc:
            bad.register(software_version="m5-test/1", capabilities=["generic_agent_task"],
                task_types=["generic_agent_task"], resource_classes=["deterministic_compute_job"],
                resource_policy={})
        self.assertEqual(exc.exception.status, 401)
        unknown=WorkerClient(f"http://127.0.0.1:{self.port}",WorkerIdentity.new_instance(ident("unknown"),"random-token"))
        with self.assertRaises(RemoteClientError) as exc:
            unknown.register(software_version="m5-test/1",capabilities=[],task_types=[],resource_classes=[],resource_policy={})
        self.assertEqual(exc.exception.status,401)
        scoped = WorkerClient(f"http://127.0.0.1:{self.port}", WorkerIdentity.new_instance(worker_id, token))
        with self.assertRaises(RemoteClientError) as exc:
            scoped.register(software_version="m5-test/1", capabilities=["governance"],
                task_types=["generic_agent_task"], resource_classes=["deterministic_compute_job"],
                resource_policy={})
        self.assertEqual(exc.exception.status, 403)
        with self.assertRaises(RemoteClientError) as exc:
            scoped.register(software_version="m5-test/1",capabilities=["generic_agent_task"],
                task_types=["generic_agent_task"],resource_classes=["remote_api_call"],resource_policy={})
        self.assertEqual(exc.exception.status,403)
        with self.assertRaises(RemoteClientError) as exc:
            scoped.register(software_version="m5-test/1",capabilities=["generic_agent_task"],
                allowed_tools=["filesystem"],task_types=["generic_agent_task"],
                resource_classes=["deterministic_compute_job"],resource_policy={})
        self.assertEqual(exc.exception.status,403)
        self.db.execute("UPDATE runtime.worker_identities SET token_expires_at=now()-interval '1 second' WHERE worker_id=%s",(worker_id,))
        with self.assertRaises(RemoteClientError) as exc:
            scoped.register(software_version="m5-test/1",capabilities=["generic_agent_task"],
                task_types=["generic_agent_task"],resource_classes=["deterministic_compute_job"],resource_policy={})
        self.assertEqual(exc.exception.status,401)
        self.db.execute("UPDATE runtime.worker_identities SET token_expires_at=now()+interval '10 minutes' WHERE worker_id=%s",(worker_id,))
        with self.assertRaises(RemoteClientError) as exc:
            registered.register(software_version="m5-test/1",capabilities=["generic_agent_task"],
                task_types=["generic_agent_task"],resource_classes=["deterministic_compute_job"],resource_policy={})
        self.assertEqual(exc.exception.status,409)
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}/v1/register",
            data=json.dumps({"protocol_version":"99.0","software_version":"m5-test/1",
                "capabilities":[],"task_types":[],"resource_classes":[],"resource_policy":{}}).encode(),
            headers={"Authorization":"Bearer "+token,"X-Worker-ID":worker_id,
                "X-Worker-Instance-ID":"wi_"+uuid.uuid4().hex,"X-Agentic-Protocol":"99.0",
                "Content-Type":"application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(request, timeout=3)
        self.assertEqual(exc.exception.code, 426)
        oversized=urllib.request.Request(f"http://127.0.0.1:{self.port}/v1/register",data=b"x"*65537,
            headers={"Content-Type":"application/json"},method="POST")
        with self.assertRaises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(oversized,timeout=3)
        self.assertEqual(exc.exception.code,413)
        with self.assertRaises(RemoteClientError) as exc:
            scoped.request("POST", "/v1/admin/command", {"command":"DRAIN",
                "worker_instance_id":scoped.identity.worker_instance_id})
        self.assertEqual(exc.exception.status,401)

    def test_stale_worker_is_fenced_after_reassignment_and_new_attempt_wins(self):
        task, _, _ = self._campaign_task()
        token_a, client_a = self._remote_identity(ident("worker-a"))
        _, client_b = self._remote_identity(ident("worker-b"))
        first = client_a.claim("claim-a") ["task"]
        self.assertIsNotNone(first)
        client_a.start_attempt(task, first["attempt_id"], first["lease_epoch"])
        self.db.execute("UPDATE runtime.leases SET lease_until=now()-interval '1 second' WHERE task_id=%s", (task,))
        Coordinator(self.db).reconcile_expired_leases(retry_delay_seconds=0)
        second = client_b.claim("claim-b")["task"]
        self.assertIsNotNone(second)
        self.assertGreater(second["lease_epoch"], first["lease_epoch"])
        with self.assertRaises(RemoteClientError) as exc:
            client_a.cancel_attempt(task, first["attempt_id"], first["lease_epoch"])
        self.assertEqual(exc.exception.status, 409)
        client_b.start_attempt(task, second["attempt_id"], second["lease_epoch"])
        content=json.dumps({"ok":True,"value":"current-attempt"},separators=(",",":" )).encode()
        digest=hashlib.sha256(content).hexdigest()
        artifact=client_b.upload_artifact(task,second["attempt_id"],second["lease_epoch"],content,digest)
        result=client_b.submit_result(task_id=task,attempt_id=second["attempt_id"],
            lease_epoch=second["lease_epoch"],artifact_id=artifact["artifact_id"],
            result_hash=digest,idempotency_key="current-result")
        self.assertEqual(result["status"],"ACCEPTED")
        reconnected=WorkerClient(f"http://127.0.0.1:{self.port}",client_b.identity)
        self.assertEqual(reconnected.submit_result(task_id=task,attempt_id=second["attempt_id"],
            lease_epoch=second["lease_epoch"],artifact_id=artifact["artifact_id"],
            result_hash=digest,idempotency_key="current-result"),result)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"ACCEPTED")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.remote_result_receipts WHERE task_id=%s",(task,)).fetchone()["n"],1)
        self.assertNotEqual(client_a.identity.worker_instance_id,client_b.identity.worker_instance_id)
        with self.assertRaises(RemoteClientError) as exc:
            client_a.cancel_attempt(task,second["attempt_id"],second["lease_epoch"])
        self.assertEqual(exc.exception.status,409)

    def test_corrupt_and_interrupted_artifact_transfer_never_registers_partial_blob(self):
        task, _, _ = self._campaign_task()
        worker_id=ident("worker"); _,client=self._remote_identity(worker_id)
        lease=client.claim("claim-corrupt")["task"]
        client.start_attempt(task,lease["attempt_id"],lease["lease_epoch"])
        content=b'{"ok":true,"value":"valid"}'
        digest=hashlib.sha256(content).hexdigest()
        with self.assertRaises(RemoteClientError) as exc:
            client.upload_artifact(task,lease["attempt_id"],lease["lease_epoch"],b"corrupt",digest)
        self.assertEqual(exc.exception.status,422)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE content_hash=%s",(digest,)).fetchone()["n"],0)
        partial_hash="a"*64
        sock=socket.create_connection(("127.0.0.1",self.port),timeout=3)
        body=b"partial"
        headers=(f"PUT /v1/artifacts/{partial_hash} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            f"Authorization: Bearer {client.identity.token}\r\nX-Worker-ID: {worker_id}\r\n"
            f"X-Worker-Instance-ID: {client.identity.worker_instance_id}\r\nX-Agentic-Protocol: 1.0\r\n"
            f"X-Task-ID: {task}\r\nX-Attempt-ID: {lease['attempt_id']}\r\n"
            f"X-Lease-Epoch: {lease['lease_epoch']}\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n").encode()
        sock.sendall(headers+body); sock.close()
        time.sleep(.1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE content_hash=%s",(partial_hash,)).fetchone()["n"],0)
        artifact=client.upload_artifact(task,lease["attempt_id"],lease["lease_epoch"],content,digest)
        self.assertTrue(artifact["verified"])
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.remote_transfer_rejections WHERE attempt_id=%s",
            (lease["attempt_id"],)).fetchone()["n"],1)
        partial=json.dumps({"task_id":task,"attempt_id":lease["attempt_id"],"lease_epoch":lease["lease_epoch"],
            "artifact_id":artifact["artifact_id"],"result_hash":digest,"idempotency_key":"interrupted-result",
            "usage":{},"protocol_version":"1.0","request_id":"interrupted-result"}).encode()
        sock=socket.create_connection(("127.0.0.1",self.port),timeout=3)
        headers=(f"POST /v1/result HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {client.identity.token}\r\n"
            f"X-Worker-ID: {worker_id}\r\nX-Worker-Instance-ID: {client.identity.worker_instance_id}\r\n"
            f"X-Agentic-Protocol: 1.0\r\nContent-Type: application/json\r\nContent-Length: 4096\r\n"
            "Connection: close\r\n\r\n").encode()
        sock.sendall(headers+partial[:20]); sock.close(); time.sleep(.1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.remote_result_receipts WHERE attempt_id=%s",
            (lease["attempt_id"],)).fetchone()["n"],0)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"RUNNING")
        accepted=client.submit_result(task_id=task,attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],
            artifact_id=artifact["artifact_id"],result_hash=digest,idempotency_key="retry-result")
        self.assertEqual(accepted["status"],"ACCEPTED")

    def test_artifact_store_write_failure_never_accepts_partial_then_retry_succeeds(self):
        task,_,_=self._campaign_task()
        worker_id=ident("worker"); _,client=self._remote_identity(worker_id)
        lease=client.claim("m6d-artifact-write-claim")["task"]
        client.start_attempt(task,lease["attempt_id"],lease["lease_epoch"])
        sandbox="sbx_"+uuid.uuid4().hex
        client.sandbox_created(task,lease["attempt_id"],lease["lease_epoch"],sandbox,DEFAULT_RESOURCE_POLICY)
        content=json.dumps({"ok":True,"value":"write-fault-retry"},separators=(",",":" )).encode()
        digest=hashlib.sha256(content).hexdigest()
        self.artifact_root.chmod(0o500)
        try:
            with self.assertRaises(RemoteClientError) as exc:
                client.upload_artifact(task,lease["attempt_id"],lease["lease_epoch"],content,digest)
            self.assertGreaterEqual(exc.exception.status,500)
        finally:
            self.artifact_root.chmod(0o700)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"RUNNING")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE producer_task_id=%s",(task,)).fetchone()["n"],0)
        self.assertEqual(list(self.artifact_root.rglob(".pending-*")),[])
        artifact=client.upload_artifact(task,lease["attempt_id"],lease["lease_epoch"],content,digest)
        accepted=client.submit_result(task_id=task,attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],
            artifact_id=artifact["artifact_id"],result_hash=digest,idempotency_key="m6d-artifact-write-result")
        self.assertEqual(accepted["status"],"ACCEPTED")
        self.assertEqual(hashlib.sha256(ArtifactStore(self.artifact_root).read(artifact["artifact_id"])).hexdigest(),digest)
        client.cleanup_sandbox(task,lease["attempt_id"],lease["lease_epoch"],sandbox,workspace_absent=True)
        client.reconcile_local([{"sandbox_id":sandbox,"task_id":task,"attempt_id":lease["attempt_id"],
            "lease_epoch":lease["lease_epoch"],"worker_instance_id":client.identity.worker_instance_id,"state":"ABSENT"}])

    def test_worker_unavailable_leaves_task_queued_and_control_restart_recovers(self):
        task,worker_id,token=self._campaign_task()
        unavailable=self._run_worker(worker_id,token,M5_CONTROL_ENDPOINT="http://127.0.0.1:1")
        self.assertEqual(unavailable.returncode,2)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"QUEUED")
        self.server.terminate(); self.server.wait(timeout=3)
        if self.server.stdout: self.server.stdout.close()
        self.server,self.port=self._start_control_plane()
        recovered=self._run_worker(worker_id,token)
        self.assertEqual(recovered.returncode,0,recovered.stdout+recovered.stderr)
        self.assertEqual(json.loads(recovered.stdout.strip().splitlines()[-1])["status"],"ACCEPTED")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"ACCEPTED")

    def test_remote_sandbox_blocks_sibling_workspace_and_network(self):
        sibling=self.worker_root/"other-sandbox"; sibling.mkdir(parents=True)
        secret=sibling/"private.txt"; secret.write_text("not visible to this attempt")
        current=self.worker_root/"attempt-a"; current.mkdir()
        output=BwrapSandboxRunner().execute_json(current,{"value":"probe","probe_network":True,
            "probe_path":str(secret)},DEFAULT_RESOURCE_POLICY)
        value=json.loads(output)
        self.assertFalse(value["cross_sandbox_access"])
        self.assertTrue(value["network_blocked"])

    def test_empty_inventory_never_proves_sandbox_absence(self):
        task, worker_id, _ = self._campaign_task()
        _, client = self._remote_identity(worker_id)
        leased = client.claim("sandbox-reconcile-test")["task"]
        client.start_attempt(task, leased["attempt_id"], leased["lease_epoch"])
        sandbox_id = "sbx_" + uuid.uuid4().hex
        client.sandbox_created(task, leased["attempt_id"], leased["lease_epoch"], sandbox_id,
            {"wall_time_seconds": 1})

        # No report is not evidence of local absence.
        self.assertEqual(client.reconcile_local([])["confirmed_absent"], [])
        state = self.db.execute("SELECT status FROM runtime.sandboxes WHERE sandbox_id=%s",
            (sandbox_id,)).fetchone()
        self.assertEqual(state["status"], "ACTIVE")

        entry = {"sandbox_id": sandbox_id, "task_id": task, "attempt_id": leased["attempt_id"],
            "lease_epoch": leased["lease_epoch"], "worker_instance_id": client.identity.worker_instance_id,
            "state": "ABSENT"}
        self.assertEqual(client.reconcile_local([entry])["confirmed_absent"], [sandbox_id])
        state = self.db.execute("SELECT status FROM runtime.sandboxes WHERE sandbox_id=%s",
            (sandbox_id,)).fetchone()
        self.assertEqual(state["status"], "DESTROYED")

    def test_lost_sandbox_create_response_cleans_local_workspace(self):
        worker = RemoteWorker(endpoint="http://127.0.0.1:1", worker_id="worker-create-loss",
            token="test-token", root=self.worker_root, capabilities=["generic_agent_task"],
            task_types=["generic_agent_task"], resource_classes=["deterministic_compute_job"],
            resource_policy=DEFAULT_RESOURCE_POLICY)

        class LostCreateResponse:
            identity=worker.identity
            def heartbeat(self, _active): return {}
            def claim(self, _key): return {"task":{"task_id":"task-create-loss",
                "attempt_id":"attempt-create-loss","lease_epoch":1,"budget":{},"metadata":{}}}
            def start_attempt(self,*_args): return {}
            def sandbox_created(self,*_args): raise RemoteClientError(503,"connection lost after create request")
            def reconcile_local(self,entries):
                return {"confirmed_absent":[],"unresolved":[],
                    "untracked":[e["sandbox_id"] for e in entries]}

        worker.client=LostCreateResponse()
        with self.assertRaises(RemoteClientError):
            worker.run_once()
        leftovers=[path.name for path in self.worker_root.iterdir() if path.name!=".reconciled"]
        self.assertEqual(leftovers,[])
        self.assertEqual(list(worker.tombstones.glob("*.json")),[])

    def test_remote_unknown_usage_settles_against_reserved_campaign_caps(self):
        task,worker_id,token=self._campaign_task()
        goal=self.db.execute("SELECT goal_id FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["goal_id"]
        campaign=self.db.execute("SELECT campaign_id FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["campaign_id"]
        scope=ident("scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'remote accounting test')",(scope,))
        ImprovementCampaignService(self.db).create(campaign_id=campaign,scope_id=scope,goal_id=goal,
            budget={"max_cost":2,"max_tokens_input":100,"max_tokens_output":100,
                "max_wall_time_seconds":60,"max_attempts":2},created_by="m5-test")
        self.db.execute("UPDATE runtime.tasks SET budget=budget || '{\"max_cost\":2,\"max_tokens_input\":100,\"max_tokens_output\":100,\"max_wall_time_seconds\":60}'::jsonb WHERE task_id=%s",(task,))
        result=self._run_worker(worker_id,token)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        reservation=self.db.execute("SELECT reservation_id,status,usage_status FROM runtime.improvement_reservations WHERE task_id=%s",
            (task,)).fetchone()
        self.assertEqual(reservation["status"],"UNKNOWN")
        self.assertEqual(reservation["usage_status"],"UNKNOWN")
        dimensions={row["dimension"]:row for row in self.db.execute("SELECT dimension,reserved,consumed,released,unknown FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",
            (reservation["reservation_id"],)).fetchall()}
        self.assertEqual(dimensions["monetary_cost"]["consumed"],2)
        self.assertTrue(dimensions["monetary_cost"]["unknown"])
        self.assertEqual(dimensions["tokens_input"]["consumed"],100)
        self.assertEqual(dimensions["tokens_output"]["consumed"],100)

    def test_governance_drain_prevents_new_claims(self):
        task,worker_id,_=self._campaign_task()
        _,client=self._remote_identity(worker_id)
        request=urllib.request.Request(f"http://127.0.0.1:{self.port}/v1/admin/command",
            data=json.dumps({"command":"DRAIN","worker_instance_id":client.identity.worker_instance_id}).encode(),
            headers={"Authorization":"Bearer "+os.environ["M5_CONTROL_ADMIN_TOKEN"],
                "Content-Type":"application/json"},method="POST")
        with urllib.request.urlopen(request,timeout=3) as response:
            command=json.loads(response.read())
        self.assertEqual(command["status"],"PENDING")
        heartbeat=client.heartbeat([])
        self.assertEqual(heartbeat["status"],"DRAINING")
        with self.assertRaises(RemoteClientError) as exc:
            client.claim("drained-claim")
        self.assertEqual(exc.exception.status,409)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"QUEUED")

    def test_drain_during_active_task_is_preserved_until_completion(self):
        task,worker_id,token=self._campaign_task(worker_class="LOCAL")
        self.db.execute("UPDATE runtime.tasks SET metadata=metadata || '{\"worker_sleep_seconds\":5}'::jsonb WHERE task_id=%s",(task,))
        env=self._worker_env(worker_id,token,M5_WORKER_ONCE="",M6_WORKER_CLASS="LOCAL")
        proc=subprocess.Popen([sys.executable,"-m","agentic_runtime.remote.worker"],cwd=ROOT,
            env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
        try:
            deadline=time.monotonic()+8
            instance=None
            while time.monotonic()<deadline:
                row=self.db.execute("SELECT t.status,l.attempt_id,l.lease_epoch,a.worker_instance_id FROM runtime.tasks t LEFT JOIN runtime.leases l USING(task_id) LEFT JOIN runtime.attempts a ON a.attempt_id=l.attempt_id WHERE t.task_id=%s",(task,)).fetchone()
                if row["status"]=="RUNNING" and row["worker_instance_id"]:
                    instance=row["worker_instance_id"]; break
                if proc.poll() is not None: self.fail(proc.stdout.read()+proc.stderr.read())
                time.sleep(.05)
            self.assertIsNotNone(instance,"worker did not acquire a running attempt")
            request=urllib.request.Request(f"http://127.0.0.1:{self.port}/v1/admin/command",
                data=json.dumps({"command":"DRAIN","worker_instance_id":instance}).encode(),
                headers={"Authorization":"Bearer "+os.environ["M5_CONTROL_ADMIN_TOKEN"],
                    "Content-Type":"application/json"},method="POST")
            with urllib.request.urlopen(request,timeout=3) as response:
                self.assertEqual(json.loads(response.read())["status"],"PENDING")
            deadline=time.monotonic()+12
            while time.monotonic()<deadline:
                task_status=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
                instance_status=self.db.execute("SELECT status FROM runtime.worker_instances WHERE worker_instance_id=%s",(instance,)).fetchone()["status"]
                if task_status=="ACCEPTED" and proc.poll() is not None: break
                time.sleep(.05)
            stdout,stderr=proc.communicate(timeout=3)
            self.assertEqual(proc.returncode,0,stdout+stderr)
            self.assertEqual(task_status,"ACCEPTED")
            self.assertEqual(instance_status,"DRAINING")
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                instance_status=self.db.execute("SELECT status FROM runtime.worker_instances WHERE worker_instance_id=%s",(instance,)).fetchone()["status"]
                if instance_status=="OFFLINE": break
                time.sleep(.05)
            self.assertEqual(instance_status,"OFFLINE")
        finally:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
            if proc.stdout: proc.stdout.close()
            if proc.stderr: proc.stderr.close()

    def test_cancel_terminates_nested_remote_sandbox_process_tree(self):
        task,worker_id,token=self._campaign_task()
        self.db.execute("UPDATE runtime.tasks SET metadata=metadata || '{\"worker_spawn_tree_seconds\":20}'::jsonb WHERE task_id=%s",(task,))
        proc=subprocess.Popen([sys.executable,"-m","agentic_runtime.remote.worker"],cwd=ROOT,
            env=self._worker_env(worker_id,token,M5_WORKER_ONCE=""),stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            text=True,start_new_session=True)
        def descendants(root_pid):
            entries={}
            for path in Path('/proc').iterdir():
                if path.name.isdigit():
                    try:
                        raw=(path/'stat').read_text(); tail=raw[raw.rfind(')')+2:].split()
                        entries[int(path.name)]=int(tail[1])
                    except (OSError,ValueError,IndexError): pass
            tree={root_pid}; changed=True
            while changed:
                changed=False
                for pid,parent in entries.items():
                    if parent in tree and pid not in tree: tree.add(pid); changed=True
            return tree-{root_pid}
        try:
            deadline=time.monotonic()+10; lease=None; workspace=None; pids=None
            while time.monotonic()<deadline:
                row=self.db.execute("SELECT t.status,l.attempt_id,l.lease_epoch,a.worker_instance_id FROM runtime.tasks t LEFT JOIN runtime.leases l USING(task_id) LEFT JOIN runtime.attempts a ON a.attempt_id=l.attempt_id WHERE t.task_id=%s",(task,)).fetchone()
                if row["status"]=="RUNNING" and row["attempt_id"]:
                    lease=dict(row)
                    for marker in self.worker_root.glob("*/.agentic-attempt.json"):
                        if json.loads(marker.read_text()).get("task_id")==task:
                            workspace=marker.parent; break
                    if workspace and (workspace/'result.json').exists():
                        try:
                            result=json.loads((workspace/'result.json').read_text())
                            if 'grandchild_pid' in result and len(descendants(proc.pid))>=4:
                                pids=result; break
                        except (ValueError,OSError): pass
                if proc.poll() is not None: self.fail(proc.stdout.read()+proc.stderr.read())
                time.sleep(.05)
            self.assertIsNotNone(pids,"nested sandbox child and grandchild did not become observable")
            payload={"command":"CANCEL_ATTEMPT","worker_instance_id":lease["worker_instance_id"],
                "task_id":task,"attempt_id":lease["attempt_id"],"lease_epoch":lease["lease_epoch"]}
            request=urllib.request.Request(f"http://127.0.0.1:{self.port}/v1/admin/command",
                data=json.dumps(payload).encode(),headers={"Authorization":"Bearer "+os.environ["M5_CONTROL_ADMIN_TOKEN"],
                    "Content-Type":"application/json"},method="POST")
            with urllib.request.urlopen(request,timeout=3) as response: self.assertEqual(response.status,200)
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                state=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
                sandbox=self.db.execute("SELECT status,cleanup_status FROM runtime.sandboxes WHERE task_id=%s",(task,)).fetchone()
                if state=="CANCELLED" and sandbox and sandbox["status"]=="DESTROYED": break
                time.sleep(.05)
            self.assertEqual(state,"CANCELLED")
            self.assertEqual(sandbox["status"],"DESTROYED")
            self.assertFalse(workspace.exists())
            self.assertEqual(descendants(proc.pid),set(),"worker retained sandbox descendants after cancellation")
        finally:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
            if proc.stdout: proc.stdout.close()
            if proc.stderr: proc.stderr.close()

    def test_worker_restart_gets_new_instance_and_reassigns_expired_attempt(self):
        task,worker_id,token=self._campaign_task()
        token,old=self._remote_identity(worker_id)
        first=old.claim("first-process-claim")["task"]
        old.start_attempt(task,first["attempt_id"],first["lease_epoch"])
        restarted=self._run_worker(worker_id,token)
        self.assertEqual(restarted.returncode,0,restarted.stdout+restarted.stderr)
        rows=self.db.execute("SELECT attempt_id,worker_instance_id,lease_epoch,status FROM runtime.attempts WHERE task_id=%s ORDER BY started_at",
            (task,)).fetchall()
        self.assertEqual(len(rows),2)
        self.assertNotEqual(rows[0]["worker_instance_id"],rows[1]["worker_instance_id"])
        self.assertGreater(rows[1]["lease_epoch"],rows[0]["lease_epoch"])
        self.assertEqual(rows[0]["status"],"ABANDONED")
        self.assertEqual(rows[1]["status"],"ACCEPTED")
        old_status=self.db.execute("SELECT status FROM runtime.worker_instances WHERE worker_instance_id=%s",
            (old.identity.worker_instance_id,)).fetchone()["status"]
        self.assertEqual(old_status,"OFFLINE")

    def test_interrupted_terminal_sandbox_cleanup_reconciles_idempotently_after_restart(self):
        task,worker_id,token=self._campaign_task()
        token,old=self._remote_identity(worker_id)
        lease=old.claim("m6d-cleanup-claim")["task"]
        old.start_attempt(task,lease["attempt_id"],lease["lease_epoch"])
        sandbox="sbx_"+uuid.uuid4().hex
        old.sandbox_created(task,lease["attempt_id"],lease["lease_epoch"],sandbox,DEFAULT_RESOURCE_POLICY)
        self.assertEqual(old.cancel_attempt(task,lease["attempt_id"],lease["lease_epoch"])["status"],"CANCELLED")
        self.assertEqual(old.cleanup_sandbox(task,lease["attempt_id"],lease["lease_epoch"],sandbox,
            workspace_absent=False)["status"],"CLEANUP_PENDING")
        pending=self.db.execute("SELECT status,cleanup_status FROM runtime.sandboxes WHERE sandbox_id=%s",(sandbox,)).fetchone()
        self.assertEqual((pending["status"],pending["cleanup_status"]),("CLEANUP_PENDING","WORKER_REPORTED_PRESENT"))
        restarted=WorkerClient(f"http://127.0.0.1:{self.port}",WorkerIdentity.new_instance(worker_id,token))
        restarted.register(software_version="m6d-cleanup-restart/1",capabilities=["generic_agent_task"],
            task_types=["generic_agent_task"],resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds":10,"memory_bytes":1073741824,"process_count":128,
                "wall_time_seconds":15,"max_sandbox_bytes":33554432})
        inventory=[{"sandbox_id":sandbox,"task_id":task,"attempt_id":lease["attempt_id"],
            "lease_epoch":lease["lease_epoch"],"worker_instance_id":old.identity.worker_instance_id,"state":"ABSENT"}]
        first=restarted.reconcile_local(inventory)
        second=restarted.reconcile_local(inventory)
        self.assertEqual(first["confirmed_absent"],[sandbox])
        self.assertEqual(second["confirmed_absent"],[sandbox])
        row=self.db.execute("SELECT status,cleanup_status FROM runtime.sandboxes WHERE sandbox_id=%s",(sandbox,)).fetchone()
        self.assertEqual((row["status"],row["cleanup_status"]),("DESTROYED","RECONCILED_ABSENT"))
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"CANCELLED")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.leases WHERE task_id=%s",(task,)).fetchone()["status"],"RELEASED")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.events WHERE event_type='REMOTE_SANDBOX_RECONCILED_ABSENT' AND correlation_id=%s",(sandbox,)).fetchone()["n"],1)

    def test_worker_restart_reconciles_abandoned_attempt_campaign_reservation(self):
        task,worker_id,token=self._campaign_task()
        row=self.db.execute("SELECT campaign_id,goal_id FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()
        scope=ident("restart-accounting-scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'remote restart accounting test')",(scope,))
        ImprovementCampaignService(self.db).create(campaign_id=row["campaign_id"],scope_id=scope,goal_id=row["goal_id"],
            budget={"max_cost":4,"max_tokens_input":200,"max_tokens_output":200,
                "max_wall_time_seconds":60,"max_attempts":4},created_by="m5-test")
        self.db.execute("UPDATE runtime.tasks SET budget=budget || '{\"max_cost\":2,\"max_tokens_input\":100,\"max_tokens_output\":100,\"max_wall_time_seconds\":30}'::jsonb WHERE task_id=%s",(task,))
        token,old=self._remote_identity(worker_id)
        first=old.claim("restart-accounting-first")['task']
        old.start_attempt(task,first['attempt_id'],first['lease_epoch'])
        restarted=self._run_worker(worker_id,token)
        self.assertEqual(restarted.returncode,0,restarted.stdout+restarted.stderr)
        attempts=self.db.execute("SELECT attempt_id,status FROM runtime.attempts WHERE task_id=%s ORDER BY started_at",(task,)).fetchall()
        self.assertEqual([row["status"] for row in attempts],["ABANDONED","ACCEPTED"])
        reservations=self.db.execute("SELECT attempt_id,status,usage_status FROM runtime.improvement_reservations WHERE task_id=%s ORDER BY created_at",(task,)).fetchall()
        self.assertEqual(len(reservations),2)
        self.assertEqual([row["attempt_id"] for row in reservations],[row["attempt_id"] for row in attempts])
        self.assertEqual([(row["status"],row["usage_status"]) for row in reservations],[("UNKNOWN","UNKNOWN"),("UNKNOWN","UNKNOWN")])
        totals={row["dimension"]:row for row in self.db.execute("""SELECT d.dimension,sum(d.reserved) AS reserved,
            sum(d.consumed) AS consumed,sum(d.released) AS released,bool_or(d.unknown) AS unknown
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.task_id=%s GROUP BY d.dimension""",(task,)).fetchall()}
        self.assertEqual(totals["monetary_cost"]["consumed"],4)
        self.assertEqual(totals["tokens_input"]["consumed"],200)
        self.assertEqual(totals["tokens_output"]["consumed"],200)
        self.assertTrue(totals["monetary_cost"]["unknown"])

    def test_capability_and_resource_class_mismatch_do_not_dispatch_task(self):
        task,worker_id,_=self._campaign_task()
        _,client=self._remote_identity(worker_id)
        self.db.execute("UPDATE runtime.tasks SET required_capabilities='[\"ungranted_capability\"]'::jsonb WHERE task_id=%s",(task,))
        self.assertIsNone(client.claim("wrong-capability")["task"])
        self.db.execute("""UPDATE runtime.tasks SET required_capabilities='[\"generic_agent_task\"]'::jsonb,
            budget=budget || '{\"resource_class\":\"remote_api_call\"}'::jsonb WHERE task_id=%s""",(task,))
        self.assertIsNone(client.claim("wrong-resource-class")["task"])
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"QUEUED")

    def test_control_plane_loss_fences_worker_and_restart_reconciles_sandbox(self):
        task,worker_id,token=self._campaign_task()
        self.db.execute("UPDATE runtime.tasks SET metadata=metadata || '{\"worker_sleep_seconds\":8}'::jsonb WHERE task_id=%s",(task,))
        proc=subprocess.Popen([sys.executable,"-m","agentic_runtime.remote.worker"],cwd=ROOT,
            env=self._worker_env(worker_id,token),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
            start_new_session=True)
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            state=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            sandbox_active=self.db.execute("SELECT 1 FROM runtime.sandboxes WHERE task_id=%s AND status='ACTIVE'",
                (task,)).fetchone()
            if state=="RUNNING" and sandbox_active: break
            if proc.poll() is not None: self.fail(proc.stdout.read()+proc.stderr.read())
            time.sleep(.05)
        self.assertEqual(state,"RUNNING")
        self.server.terminate(); self.server.wait(timeout=3)
        if self.server.stdout: self.server.stdout.close()
        self.server=None
        try:
            stdout,stderr=proc.communicate(timeout=12)
        except subprocess.TimeoutExpired:
            proc.kill(); stdout,stderr=proc.communicate(timeout=3)
            self.fail("worker did not stop after losing control-plane authority")
        self.assertTrue('"status": "STALE"' in stdout or
            '"status": "CONTROL_PLANE_UNAVAILABLE"' in stdout,stdout)
        self.server,self.port=self._start_control_plane()
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            state=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            if state=="RETRY_PENDING": break
            time.sleep(.05)
        self.assertEqual(state,"RETRY_PENDING")
        tombstones=list((self.worker_root/".reconciled").glob("sbx_*.json"))
        self.assertTrue(tombstones,"worker did not persist absent-sandbox reconciliation evidence")
        tombstone=json.loads(tombstones[0].read_text())
        sandbox=self.db.execute("SELECT worker_id,worker_instance_id,lease_epoch,task_id,attempt_id FROM runtime.sandboxes WHERE sandbox_id=%s",
            (tombstone["sandbox_id"],)).fetchone()
        self.assertEqual((sandbox["worker_id"],sandbox["worker_instance_id"],sandbox["lease_epoch"],sandbox["task_id"],sandbox["attempt_id"]),
            (worker_id,tombstone["worker_instance_id"],tombstone["lease_epoch"],tombstone["task_id"],tombstone["attempt_id"]))
        recovered=self._run_worker(worker_id,token)
        self.assertEqual(recovered.returncode,0,recovered.stdout+recovered.stderr)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"],"ACCEPTED")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.sandboxes WHERE task_id=%s AND status='DESTROYED'",
            (task,)).fetchone()["n"],2)


if __name__ == "__main__":
    unittest.main()
