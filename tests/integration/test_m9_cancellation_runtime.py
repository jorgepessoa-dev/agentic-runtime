from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from agentic_runtime.contracts.coordination import PlanNode, PlanVersionProposal
from agentic_runtime.coordinator.coordination import CoordinationService
from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.coordinator.state import TaskState
from agentic_runtime.execution.process_supervisor import ProcessSupervisor
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.remote.control import RemoteControlPlane, provision_worker, serve_local
from agentic_runtime.remote.protocol import RemoteClientError, WorkerClient, WorkerIdentity
from agentic_runtime.remote.worker import RemoteWorker


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class _ControlledTreeRunner:
    """An event-driven native process tree used to exercise remote M9 cancel."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.supervisor = ProcessSupervisor(terminate_grace_seconds=0.2, poll_seconds=0.01)
        self.execution_id: str | None = None
        self.pid: int | None = None
        self.pgid: int | None = None
        self.group_gone = False

    def execute_json(self, workspace, payload, policy, authority_lost, cancel_requested) -> bytes:
        self.execution_id = "m9-cancel-" + uuid.uuid4().hex
        child_code = "import time; time.sleep(30)"
        parent_code = ("import subprocess,sys,time; "
            f"subprocess.Popen([sys.executable,'-c',{child_code!r}]); time.sleep(30)")
        managed = self.supervisor.start(self.execution_id,
            [sys.executable, "-c", parent_code], Path(workspace), {"PATH": os.environ.get("PATH", "")}, 30)
        self.pid = managed.process.pid
        self.pgid = os.getpgid(managed.process.pid)
        self.started.set()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if cancel_requested.is_set():
                stopped = self.supervisor.cancel(self.execution_id)
                self.group_gone = not self.supervisor._group_has_live_process(self.pgid)
                if stopped.state != "CANCELLED" or not self.group_gone:
                    raise RuntimeError("controlled process tree did not terminate")
                self.supervisor.forget(self.execution_id)
                raise PermissionError("M9 cancellation observed")
            if authority_lost.is_set():
                self.supervisor.cancel(self.execution_id)
                self.supervisor.forget(self.execution_id)
                raise PermissionError("worker authority lost")
            time.sleep(0.01)
        self.supervisor.cancel(self.execution_id)
        self.supervisor.forget(self.execution_id)
        raise TimeoutError("M9 cancellation test deadline expired")


@unittest.skipUnless(os.environ.get("M3_TEST_DATABASE_URL"), "M9 cancellation needs disposable PostgreSQL")
class M9CancellationRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dsn = os.environ["M3_TEST_DATABASE_URL"]
        cls.role = os.environ.get("M3_TEST_DATABASE_ROLE")
        cls.db = connect(cls.dsn)
        cls.db.autocommit = True
        if cls.role:
            cls.db.execute(f'SET ROLE "{cls.role}"')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def setUp(self) -> None:
        self.db.execute("TRUNCATE runtime.campaigns,runtime.workers,runtime.executors CASCADE")
        self.db.execute("TRUNCATE runtime.worker_identities CASCADE")
        self.temp = tempfile.TemporaryDirectory(prefix="m9-cancel-runtime-")
        root = Path(self.temp.name)
        service = RemoteControlPlane(self.dsn, str(root / "control-artifacts"),
            database_role=self.role, lease_seconds=10, heartbeat_suspect_seconds=5)
        self.server = serve_local(service)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        self.temp.cleanup()

    def test_plan_cancellation_propagates_idempotently_and_kills_remote_process_tree(self):
        coordinator = Coordinator(self.db)
        campaign = "m9-cancel-campaign-" + uuid.uuid4().hex
        goal = ident("m9cancelgoal")
        coordinator.create_campaign(campaign, idempotency_key=ident("m9cancelkey"),
            description="Bounded generic plan cancellation", created_by="m9-test",
            budget={"max_wall_time_seconds":120}, max_children=6)
        coordinator.create_goal(goal, campaign, description="Cancel one active branch",
            mission_ref="mission:m9-cancellation", created_by="m9-test")
        proposal = PlanVersionProposal(plan_id=ident("m9cancelplan"), goal_id=goal,
            created_by="m9-planner", nodes=(
                PlanNode("active", "transform", "Perform controlled bounded transformation",
                    required_capabilities=("transform",)),
                PlanNode("child", "review", "Review the active branch result",
                    required_capabilities=("review",),depends_on=("active",),
                    parent_node_key="active"),
                PlanNode("grandchild", "summarize", "Summarize the reviewed child",
                    required_capabilities=("summarize",),depends_on=("child",),
                    parent_node_key="child")), estimated_budget={})
        plans = CoordinationService(self.db)
        version_id = plans.propose(ident("m9cancelversion"), proposal)
        tasks = plans.accept(version_id, accepted_by="m9-coordinator-policy")

        worker_id = ident("m9-remote-worker")
        token = provision_worker(self.db, worker_id=worker_id,
            capabilities=["transform"], task_types=["transform"],
            resource_classes=["deterministic_compute_job"],
            max_resource_policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
                "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024},
            ttl_seconds=600, created_by="m9-test", worker_class="LOCAL")
        worker_root = Path(self.temp.name) / "worker"
        worker = RemoteWorker(endpoint=f"http://127.0.0.1:{self.server.server_address[1]}",
            worker_id=worker_id, token=token, root=worker_root, capabilities=["transform"],
            task_types=["transform"], resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
                "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024},
            allowed_tools=[], worker_class="LOCAL", poll_seconds=0.01,
            lease_heartbeat_seconds=0.03)
        worker.start()
        runner = _ControlledTreeRunner()
        worker.runner = runner
        result: dict[str, object] = {}
        thread = threading.Thread(target=lambda: result.setdefault("response",
            worker.run_once(idempotency_key=ident("m9cancelclaim"))), daemon=True)
        thread.start()
        self.assertTrue(runner.started.wait(5), "remote worker did not start the controlled process tree")
        self.assertIsNotNone(runner.pid)
        self.assertIsNotNone(runner.pgid)
        os.kill(runner.pid, 0)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (tasks["active"],)).fetchone()["status"], "RUNNING")

        first = plans.request_plan_cancellation(version_id, requested_by="m9-coordinator-policy")
        second = plans.request_plan_cancellation(version_id, requested_by="m9-coordinator-policy")
        self.assertEqual(len(first["commands"]), 1)
        self.assertFalse(first["commands"][0]["idempotent"])
        self.assertEqual(first["commands"][0]["command_id"], second["commands"][0]["command_id"])
        self.assertTrue(second["commands"][0]["idempotent"])
        self.assertEqual(set(first["cancelled_unstarted"]), {tasks["child"],tasks["grandchild"]})
        for node in ("child","grandchild"):
            self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
                (tasks[node],)).fetchone()["status"], "CANCELLED")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (tasks["active"],)).fetchone()["status"], "RUNNING")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.worker_commands WHERE task_id=%s",
            (tasks["active"],)).fetchone()["n"], 1)

        thread.join(timeout=8)
        self.assertFalse(thread.is_alive(), "remote worker did not acknowledge cancellation")
        self.assertEqual(result["response"]["status"], "CANCELLED")
        self.assertTrue(runner.group_gone)
        attempt = self.db.execute("SELECT attempt_id,lease_epoch,status FROM runtime.attempts WHERE task_id=%s",
            (tasks["active"],)).fetchone()
        self.assertEqual(attempt["status"], "CANCELLED")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.leases WHERE task_id=%s",
            (tasks["active"],)).fetchone()["status"], "RELEASED")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.worker_commands WHERE command_id=%s",
            (first["commands"][0]["command_id"],)).fetchone()["status"], "ACKNOWLEDGED")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE producer_task_id=ANY(%s)",
            ([tasks["active"],tasks["child"],tasks["grandchild"]],)).fetchone()["n"], 0)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.events WHERE task_id=%s AND event_type='TASK_CANCELLED'",
            (tasks["active"],)).fetchone()["n"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.events WHERE correlation_id=%s AND event_type='REMOTE_WORKER_COMMAND_ACKNOWLEDGED'",
            (first["commands"][0]["command_id"],)).fetchone()["n"], 1)

        late = b'{"late_result":true}'
        with self.assertRaises(Exception) as rejected:
            worker.client.upload_artifact(tasks["active"], attempt["attempt_id"], attempt["lease_epoch"],
                late, hashlib.sha256(late).hexdigest(), request_id="m9-late-"+attempt["attempt_id"])
        self.assertEqual(getattr(rejected.exception, "status", None), 409)
        self.assertEqual(self.db.execute("SELECT result_refs FROM runtime.tasks WHERE task_id=%s",
            (tasks["active"],)).fetchone()["result_refs"], [])
        terminal_duplicate = plans.request_plan_cancellation(version_id,
            requested_by="m9-coordinator-policy")
        self.assertEqual(terminal_duplicate["commands"], [])
        self.assertIn(tasks["active"], terminal_duplicate["already_terminal"])
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.worker_commands WHERE task_id=%s",
            (tasks["active"],)).fetchone()["n"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE producer_task_id=%s",
            (tasks["active"],)).fetchone()["n"], 0)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.leases WHERE task_id=%s",
            (tasks["active"],)).fetchone()["status"], "RELEASED")
        forbidden_child = ident("m9postcancelchild")
        with self.assertRaisesRegex(ValueError, "current parent lease"):
            Coordinator(self.db).authorize_child(request_id=ident("m9postcancelrequest"),
                parent_task_id=tasks["active"],parent_attempt_id=attempt["attempt_id"],
                lease_epoch=attempt["lease_epoch"],child_task_id=forbidden_child,
                idempotency_key=ident("m9postcancelkey"),task_type="transform",
                goal="Attempt a late descendant",capabilities=["transform"],input_refs=[],
                requested_budget={"wall_time_seconds":1})
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.tasks WHERE task_id=%s",
            (forbidden_child,)).fetchone()["n"], 0)
        self.db.execute("UPDATE runtime.campaigns SET status='CANCELLED' WHERE campaign_id=%s", (campaign,))

    def test_cancellation_does_not_fake_terminal_state_when_worker_is_offline(self):
        coordinator = Coordinator(self.db)
        campaign = "m9-cancel-orphan-" + uuid.uuid4().hex
        goal = ident("m9cancelgoal")
        coordinator.create_campaign(campaign, idempotency_key=ident("m9cancelkey"),
            description="Bounded offline worker cancellation fixture", created_by="m9-test",
            budget={"max_wall_time_seconds":120}, max_children=6)
        coordinator.create_goal(goal, campaign, description="Retain unresolved cancellation authority",
            mission_ref="mission:m9-cancel-offline", created_by="m9-test")
        proposal = PlanVersionProposal(plan_id=ident("m9cancelplan"), goal_id=goal,
            created_by="m9-planner", nodes=(PlanNode("active", "transform",
                "Attempt controlled work with an offline worker", required_capabilities=("transform",)),),
            estimated_budget={})
        plans = CoordinationService(self.db)
        version_id = plans.propose(ident("m9cancelversion"), proposal)
        task_id = plans.accept(version_id, accepted_by="m9-coordinator-policy")["active"]
        worker_id = ident("m9-offline-worker")
        token = provision_worker(self.db, worker_id=worker_id, capabilities=["transform"],
            task_types=["transform"], resource_classes=["deterministic_compute_job"],
            max_resource_policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
                "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024},
            ttl_seconds=600, created_by="m9-test", worker_class="LOCAL")
        worker = RemoteWorker(endpoint=f"http://127.0.0.1:{self.server.server_address[1]}",
            worker_id=worker_id, token=token, root=Path(self.temp.name)/"offline-worker",
            capabilities=["transform"], task_types=["transform"],
            resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
                "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024},
            allowed_tools=[], worker_class="LOCAL")
        worker.start()
        lease = coordinator.claim(worker_id, worker_instance_id=worker.identity.worker_instance_id)
        self.assertEqual(lease["task_id"], task_id)
        coordinator.transition(task_id, lease["attempt_id"], lease["lease_epoch"],
            TaskState.RUNNING, actor_id=worker_id)
        self.db.execute("UPDATE runtime.worker_instances SET status='OFFLINE' WHERE worker_instance_id=%s",
            (worker.identity.worker_instance_id,))
        result = plans.request_plan_cancellation(version_id, requested_by="m9-coordinator-policy")
        self.assertEqual(result["commands"], [])
        self.assertEqual(result["unresolved"], [{"task_id":task_id,
            "reason":"active lease has no commandable worker instance"}])
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (task_id,)).fetchone()["status"], "RUNNING")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.worker_commands WHERE task_id=%s",
            (task_id,)).fetchone()["n"], 0)
        reconnected=WorkerClient(f"http://127.0.0.1:{self.server.server_address[1]}",
            WorkerIdentity.new_instance(worker_id,token))
        reconnected.register(software_version="m9-cancel-reconnected/1",capabilities=["transform"],
            task_types=["transform"],resource_classes=["deterministic_compute_job"],
            resource_policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
                "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024},worker_class="LOCAL")
        self.assertNotEqual(reconnected.identity.worker_instance_id,worker.identity.worker_instance_id)
        self.db.execute("UPDATE runtime.leases SET lease_until=now()-interval '1 second' WHERE task_id=%s",
            (task_id,))
        reconciled=coordinator.reconcile_runtime()
        self.assertIn(task_id,reconciled["expired_leases_recovered"])
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (task_id,)).fetchone()["status"],"RETRY_PENDING")
        retried=plans.request_plan_cancellation(version_id,requested_by="m9-coordinator-policy")
        self.assertIn(task_id,retried["cancelled_unstarted"])
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (task_id,)).fetchone()["status"],"CANCELLED")
        duplicate=plans.request_plan_cancellation(version_id,requested_by="m9-coordinator-policy")
        self.assertIn(task_id,duplicate["already_terminal"])
        self.assertEqual(self.db.execute("""SELECT count(*) AS n FROM runtime.events
            WHERE task_id=%s AND event_type='TASK_CANCELLED'""",(task_id,)).fetchone()["n"],1)
        with self.assertRaisesRegex(ValueError,"stale or missing lease"):
            coordinator.commit_result(task_id=task_id,attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],artifact_refs=[],result_hash="0"*64)

    def test_reconnected_worker_instance_cannot_complete_old_graph_attempt(self):
        coordinator=Coordinator(self.db)
        campaign="m9-stale-instance-"+uuid.uuid4().hex
        goal=ident("m9stalegoal")
        coordinator.create_campaign(campaign,idempotency_key=ident("m9stalekey"),
            description="Fence old worker instance inside a governed graph",created_by="m9-test",
            budget={"max_wall_time_seconds":120},max_children=6)
        coordinator.create_goal(goal,campaign,description="Reject stale graph result",
            mission_ref="mission:m9-stale-worker-instance",created_by="m9-test")
        proposal=PlanVersionProposal(plan_id=ident("m9staleplan"),goal_id=goal,created_by="planner",
            nodes=(PlanNode("branch","transform","Run fenced graph branch",("m9-stale-cap",)),
                PlanNode("join","join","Remain blocked after stale result",("join",),
                    depends_on=("branch",),dependency_requirements={"branch":"ARTIFACT"})),
            estimated_budget={})
        version=CoordinationService(self.db).propose(ident("m9staleversion"),proposal)
        tasks=CoordinationService(self.db).accept(version,accepted_by="m9-policy")
        worker_id=ident("m9-stale-remote-worker")
        policy={"cpu_seconds":30,"memory_bytes":536870912,"process_count":16,
            "wall_time_seconds":30,"max_sandbox_bytes":8*1024*1024}
        token=provision_worker(self.db,worker_id=worker_id,capabilities=["m9-stale-cap"],
            task_types=["transform"],resource_classes=["deterministic_compute_job"],
            max_resource_policy=policy,ttl_seconds=600,created_by="m9-test",worker_class="LOCAL")
        endpoint=f"http://127.0.0.1:{self.server.server_address[1]}"
        old=WorkerClient(endpoint,WorkerIdentity.new_instance(worker_id,token))
        old.register(software_version="m9-stale/1",capabilities=["m9-stale-cap"],
            task_types=["transform"],resource_classes=["deterministic_compute_job"],resource_policy=policy,
            worker_class="LOCAL")
        lease=old.claim("m9-old-instance-claim")["task"]
        self.assertEqual(lease["task_id"],tasks["branch"])
        old.start_attempt(tasks["branch"],lease["attempt_id"],lease["lease_epoch"])
        content=b'{"branch":"old-instance"}'
        digest=hashlib.sha256(content).hexdigest()
        artifact=old.upload_artifact(tasks["branch"],lease["attempt_id"],lease["lease_epoch"],content,digest)
        old_instance=old.identity.worker_instance_id
        new=WorkerClient(endpoint,WorkerIdentity.new_instance(worker_id,token))
        new.register(software_version="m9-stale/2",capabilities=["m9-stale-cap"],
            task_types=["transform"],resource_classes=["deterministic_compute_job"],resource_policy=policy,
            worker_class="LOCAL")
        self.assertNotEqual(old_instance,new.identity.worker_instance_id)
        with self.assertRaises(RemoteClientError) as rejected:
            old.submit_result(task_id=tasks["branch"],attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],artifact_id=artifact["artifact_id"],
                result_hash=digest,idempotency_key="m9-stale-result")
        self.assertEqual(rejected.exception.status,409)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (tasks["branch"],)).fetchone()["status"],"RETRY_PENDING")
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (tasks["join"],)).fetchone()["status"],"QUEUED")
        self.assertEqual(self.db.execute("""SELECT count(*) AS n FROM runtime.events
            WHERE task_id=%s AND event_type='TASK_RESULT_COMMITTED'""",(tasks["branch"],)).fetchone()["n"],0)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.worker_instances WHERE worker_instance_id=%s",
            (old_instance,)).fetchone()["status"],"OFFLINE")
        current=new.claim("m9-current-instance-claim")["task"]
        self.assertEqual(current["task_id"],tasks["branch"])
        self.assertGreater(current["lease_epoch"],lease["lease_epoch"])
        new.start_attempt(tasks["branch"],current["attempt_id"],current["lease_epoch"])
        self.assertEqual(new.cancel_attempt(tasks["branch"],current["attempt_id"],
            current["lease_epoch"])["status"],"CANCELLED")

    def test_terminal_mission_cannot_accept_new_plan_work(self):
        coordinator = Coordinator(self.db)
        campaign = "m9-terminal-plan-" + uuid.uuid4().hex
        goal = ident("m9terminalgoal")
        coordinator.create_campaign(campaign, idempotency_key=ident("m9terminalkey"),
            description="Terminal mission rejects new coordination work", created_by="m9-test",
            budget={"max_wall_time_seconds":120}, max_children=6)
        coordinator.create_goal(goal, campaign, description="Terminal plan guard",
            mission_ref="mission:m9-terminal-plan", created_by="m9-test")
        service=CoordinationService(self.db)
        for goal_state,campaign_state in (("ACHIEVED","ACTIVE"),("ABANDONED","ACTIVE"),
                ("ACTIVE","COMPLETED"),("ACTIVE","CANCELLED")):
            with self.subTest(goal_state=goal_state,campaign_state=campaign_state):
                self.db.execute("UPDATE runtime.goals SET status='ACTIVE' WHERE goal_id=%s",(goal,))
                self.db.execute("UPDATE runtime.campaigns SET status='ACTIVE' WHERE campaign_id=%s",(campaign,))
                proposal=PlanVersionProposal(plan_id=ident("m9terminalplan"),goal_id=goal,
                    created_by="m9-planner",nodes=(PlanNode("node","transform",
                        "Attempt work after mission terminalization",required_capabilities=("transform",)),),
                    estimated_budget={})
                proposed=service.propose(ident("m9terminalversion"),proposal)
                self.db.execute("UPDATE runtime.goals SET status=%s WHERE goal_id=%s",(goal_state,goal))
                self.db.execute("UPDATE runtime.campaigns SET status=%s WHERE campaign_id=%s",(campaign_state,campaign))
                with self.assertRaisesRegex(ValueError,"active goal and campaign"):
                    service.propose(ident("m9terminalversion"),proposal)
                with self.assertRaisesRegex(ValueError,"must remain active"):
                    service.accept(proposed,accepted_by="coordinator-policy")
                self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.tasks WHERE goal_id=%s",
                    (goal,)).fetchone()["n"],0)

        # A competing acceptance must wait for the goal lock and recheck the
        # terminal decision after commit, rather than materialize stale work.
        self.db.execute("UPDATE runtime.goals SET status='ACTIVE' WHERE goal_id=%s",(goal,))
        self.db.execute("UPDATE runtime.campaigns SET status='ACTIVE' WHERE campaign_id=%s",(campaign,))
        proposal=PlanVersionProposal(plan_id=ident("m9terminalrace"),goal_id=goal,
            created_by="m9-planner",nodes=(PlanNode("node","transform","Bounded terminal race",
                required_capabilities=("transform",)),),estimated_budget={})
        version=service.propose(ident("m9terminalraceversion"),proposal)
        competing=connect(os.environ["M3_TEST_DATABASE_URL"]);competing.autocommit=True
        if self.role: competing.execute(f'SET ROLE "{self.role}"')
        outcome=[]; started=threading.Event()
        def accept_concurrently():
            started.set()
            try: CoordinationService(competing).accept(version,accepted_by="coordinator-policy")
            except ValueError as exc: outcome.append(str(exc))
        thread=threading.Thread(target=accept_concurrently)
        try:
            with self.db.transaction():
                self.db.execute("SELECT 1 FROM runtime.goals WHERE goal_id=%s FOR UPDATE",(goal,))
                self.db.execute("UPDATE runtime.goals SET status='ABANDONED' WHERE goal_id=%s",(goal,))
                thread.start();self.assertTrue(started.wait(2))
                deadline=time.monotonic()+3
                while time.monotonic()<deadline:
                    waiting=self.db.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s AND NOT granted) AS waiting",
                        (competing.info.backend_pid,)).fetchone()
                    if waiting["waiting"]: break
                    time.sleep(0.01)
                else: self.fail("competing plan acceptance never waited on terminal authority")
            thread.join(3);self.assertFalse(thread.is_alive())
            self.assertEqual(len(outcome),1);self.assertIn("must remain active",outcome[0])
            self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.tasks WHERE goal_id=%s",
                (goal,)).fetchone()["n"],0)
        finally:
            thread.join(3);competing.close()



if __name__ == "__main__":
    unittest.main()
