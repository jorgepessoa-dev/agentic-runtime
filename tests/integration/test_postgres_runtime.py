from __future__ import annotations

import os
import hashlib
import json
import re
import select
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.coordinator.state import TaskState
from agentic_runtime.evolution.controller import EvolutionController
from agentic_runtime.evolution.evaluator import canonical_json, evaluate_routing_genome
from agentic_runtime.persistence.postgres import apply_migrations, connect
from agentic_runtime.persistence.outbox import OutboxDispatcher
from agentic_runtime.adapters.fakes import FakeHarnessAdapter
from agentic_runtime.adapters.sandbox import GitWorktreeSandboxAdapter
from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.contracts.execution import ExecutionRequest, ExecutorClass
from agentic_runtime.contracts.plan import PlanProposal, ProposedTask


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def connect_test_database():
    db=connect(os.environ["M3_TEST_DATABASE_URL"])
    db.autocommit=True
    role=os.environ.get("M3_TEST_DATABASE_ROLE")
    if role:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*",role):
            db.close()
            raise ValueError("M3_TEST_DATABASE_ROLE must be a simple SQL identifier")
        db.execute(f'SET ROLE "{role}"')
    return db


class PostgreSQLRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not os.environ.get("M3_TEST_DATABASE_URL"):
            raise RuntimeError("M3_TEST_DATABASE_URL must point to the dedicated PostgreSQL test database")
        cls.dsn = os.environ["M3_TEST_DATABASE_URL"]
        cls.connection = connect_test_database()
        apply_migrations(cls.connection)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()

    def setUp(self) -> None:
        # Each integration case gets empty logical state while retaining the
        # freshly migrated schema and its migration ledger.
        with self.connection.transaction():
            self.connection.execute("TRUNCATE runtime.campaigns, runtime.workers, runtime.executors CASCADE")
            self.connection.execute("TRUNCATE evolution.scopes CASCADE")

    def campaign_and_goal(self):
        campaign, goal = ident("camp"), ident("goal")
        coordinator = Coordinator(self.connection)
        coordinator.create_campaign(campaign,idempotency_key=ident("campaign-key"),
            description="M3 test campaign",created_by="integration-test",
            budget={"max_cost":100,"max_wall_time":1000})
        coordinator.create_goal(goal,campaign,description="Verify a generic transformation",
                                mission_ref="mission:test",created_by="integration-test")
        return coordinator,campaign,goal

    def test_competing_workers_get_unique_lease_and_stale_epoch_is_fenced(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="transform",idempotency_key=ident("task-key"),
                                goal_id=goal,budget={"max_cost":10})
        workers=[ident("worker"),ident("worker")]
        for worker in workers: coordinator.register_worker(worker,["transform"])
        barrier=threading.Barrier(2)
        results=[]
        errors=[]
        def claim(worker):
            try:
                db=connect_test_database()
                barrier.wait()
                results.append(Coordinator(db).claim(worker,lease_seconds=0))
                db.close()
            except Exception as exc: errors.append(exc)
        threads=[threading.Thread(target=claim,args=(worker,)) for worker in workers]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertFalse(errors)
        claimed=[result for result in results if result]
        self.assertEqual(len(claimed),1)
        first=claimed[0]
        coordinator.reconcile_expired_leases(retry_delay_seconds=0)
        second=coordinator.claim(next(w for w in workers if w!=first["worker_id"]))
        self.assertIsNotNone(second)
        self.assertGreater(second["lease_epoch"],first["lease_epoch"])
        with self.assertRaisesRegex(ValueError,"stale or missing lease"):
            coordinator.commit_result(task_id=task,attempt_id=first["attempt_id"],
                lease_epoch=first["lease_epoch"],artifact_refs=["sha256:"+"0"*64],
                result_hash="0"*64,actor_id=first["worker_id"])
        with self.assertRaisesRegex(ValueError,"stale"):
            coordinator.transition(task,first["attempt_id"],first["lease_epoch"],
                                    TaskState.RUNNING,
                                    actor_id=first["worker_id"])

    def test_worker_process_death_expires_and_reconciles_lease(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task,worker=ident("task"),ident("worker")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),
                                goal_id=goal,required_capabilities=["inspect"])
        coordinator.register_worker(worker,["inspect"])
        env=os.environ.copy(); env["M3_TEST_WORKER_ID"]=worker
        code="""import os,time
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.coordinator.service import Coordinator
db=connect(os.environ['M3_TEST_DATABASE_URL']); db.autocommit=True
db.execute('SET ROLE ' + os.environ['M3_TEST_DATABASE_ROLE'])
lease=Coordinator(db).claim(os.environ['M3_TEST_WORKER_ID'],lease_seconds=1)
print(lease['attempt_id'],flush=True)
time.sleep(30)
"""
        worker_process=subprocess.Popen([sys.executable,"-c",code],env=env,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            ready,_,_=select.select([worker_process.stdout],[],[],5)
            self.assertTrue(ready,"worker process did not report its claimed attempt")
            attempt_id=worker_process.stdout.readline().strip()
            self.assertTrue(attempt_id.startswith("att_"))
            worker_process.kill()
            worker_process.wait(timeout=5)
            time.sleep(1.1)
            report=coordinator.reconcile_runtime(scan_id=ident("worker-death"))
            state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            attempt=self.connection.execute("SELECT status FROM runtime.attempts WHERE attempt_id=%s",
                                            (attempt_id,)).fetchone()["status"]
            self.assertEqual(state,"RETRY_PENDING")
            self.assertEqual(attempt,"ABANDONED")
            self.assertEqual(report["expired_leases_recovered"],[task])
        finally:
            if worker_process.poll() is None:
                worker_process.kill(); worker_process.wait(timeout=5)
            if worker_process.stdout: worker_process.stdout.close()
            if worker_process.stderr: worker_process.stderr.close()

    def test_cancelled_task_releases_lease_and_survives_reconciliation(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task,worker=ident("task"),ident("worker")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),
                                goal_id=goal,required_capabilities=["inspect"])
        coordinator.register_worker(worker,["inspect"])
        lease=coordinator.claim(worker,lease_seconds=0)
        self.assertIsNotNone(lease)
        coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.CANCELLED,
                                actor_id="test-canceller")
        status=self.connection.execute("SELECT status FROM runtime.leases WHERE task_id=%s",(task,)).fetchone()["status"]
        self.assertEqual(status,"RELEASED")
        worker_status=self.connection.execute("SELECT status FROM runtime.workers WHERE worker_id=%s",(worker,)).fetchone()["status"]
        self.assertEqual(worker_status,"IDLE")
        stale_worker=ident("stale-busy-worker")
        coordinator.register_worker(stale_worker,["inspect"])
        self.connection.execute("UPDATE runtime.workers SET status='BUSY' WHERE worker_id=%s",(stale_worker,))
        report=coordinator.reconcile_runtime(scan_id=ident("cancel-reconcile"))
        state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
        self.assertEqual(state,"CANCELLED")
        self.assertEqual(report["expired_leases_recovered"],[])
        self.assertEqual(report["stale_busy_workers_reconciled"],[stale_worker])
        self.assertEqual(self.connection.execute("SELECT status FROM runtime.workers WHERE worker_id=%s",(stale_worker,)).fetchone()["status"],"IDLE")
        self.assertEqual(self.connection.execute("SELECT status FROM runtime.leases WHERE task_id=%s",(task,)).fetchone()["status"],"RELEASED")

    def test_two_distinct_tasks_can_be_claimed_concurrently(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        tasks=[ident("task"),ident("task")]
        for task in tasks:
            coordinator.create_task(task,campaign,task_type="transform",idempotency_key=ident("key"),goal_id=goal)
        workers=[ident("worker"),ident("worker")]
        for worker in workers: coordinator.register_worker(worker,["transform"])
        barrier=threading.Barrier(2); results=[]; errors=[]
        def claim(worker):
            db=connect_test_database()
            try:
                barrier.wait()
                results.append(Coordinator(db).claim(worker))
            except Exception as exc: errors.append(exc)
            finally: db.close()
        threads=[threading.Thread(target=claim,args=(worker,)) for worker in workers]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertFalse(errors)
        self.assertEqual(len({result["task_id"] for result in results if result}),2)

    def test_plan_task_event_and_outbox_are_durable(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        opportunity=ident("opportunity"); hypothesis=ident("hypothesis")
        coordinator.create_opportunity(opportunity,goal,kind="UNCERTAINTY",
            description="A neutral uncertainty worth checking",observation_refs=["event://observation"])
        coordinator.create_hypothesis(hypothesis,opportunity,
            statement="A deterministic alternative may reduce repeated failures",
            falsification_ref="artifact://falsification-plan",created_by="planner-proposal")
        self.assertEqual(self.connection.execute("SELECT status FROM runtime.hypotheses WHERE hypothesis_id=%s",
            (hypothesis,)).fetchone()["status"],"PROPOSED")
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),goal_id=goal)
        event=self.connection.execute("SELECT count(*) AS n FROM runtime.events WHERE task_id=%s AND event_type='TASK_QUEUED'",
                                      (task,)).fetchone()["n"]
        self.assertEqual(event,1)

    def test_advisory_idempotency_serializes_duplicate_campaign_creation(self):
        key=ident("campaign-key")
        barrier=threading.Barrier(2); results=[]; errors=[]
        def create(campaign_id):
            db=connect_test_database()
            try:
                barrier.wait()
                results.append(Coordinator(db).create_campaign(campaign_id,idempotency_key=key,
                    description="Same idempotent campaign",created_by="concurrent-test",budget={"max_cost":2}))
            except Exception as exc: errors.append(exc)
            finally: db.close()
        threads=[threading.Thread(target=create,args=(ident("camp"),)) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertFalse(errors)
        self.assertEqual(len(results),2)
        self.assertEqual(len(set(results)),1)
        self.assertEqual(self.connection.execute("SELECT count(*) AS n FROM runtime.campaigns WHERE idempotency_key=%s",
            (key,)).fetchone()["n"],1)

    def test_runtime_event_records_are_immutable(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        event=self.connection.execute("SELECT event_id FROM runtime.events WHERE event_type='CAMPAIGN_CREATED' AND campaign_id=%s",
            (campaign,)).fetchone()["event_id"]
        with self.assertRaises(Exception):
            with self.connection.transaction():
                self.connection.execute("UPDATE runtime.events SET payload='{}'::jsonb WHERE event_id=%s",(event,))
        self.assertIsNotNone(self.connection.execute("SELECT 1 FROM runtime.events WHERE event_id=%s",
            (event,)).fetchone())

    def test_invalid_direct_state_transition_is_rejected_by_database(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),goal_id=goal)
        with self.assertRaises(Exception):
            with self.connection.transaction():
                self.connection.execute("UPDATE runtime.tasks SET status='ACCEPTED' WHERE task_id=%s",(task,))
        state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
        self.assertEqual(state,"QUEUED")

    def test_budget_violation_is_rejected_before_child_task_creation(self):
        coordinator=Coordinator(self.connection)
        campaign,goal,parent=ident("camp"),ident("goal"),ident("task")
        coordinator.create_campaign(campaign,idempotency_key=ident("campaign-key"),description="Bounded campaign",
                                    created_by="integration-test",budget={"max_cost":1})
        coordinator.create_goal(goal,campaign,description="A bounded neutral task",mission_ref="mission:test",
                                created_by="integration-test")
        coordinator.create_task(parent,campaign,task_type="coordinate",idempotency_key=ident("task-key"),
            goal_id=goal,budget={"max_cost":10},max_children=2,depth_remaining=1,
            required_capabilities=["coordinate"])
        worker=ident("worker"); coordinator.register_worker(worker,["coordinate"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        coordinator.transition(parent,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        with self.assertRaisesRegex(ValueError,"campaign budget exhausted"):
            coordinator.authorize_child(request_id=ident("request"),parent_task_id=parent,
                parent_attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],
                child_task_id=ident("child"),idempotency_key=ident("child-key"),task_type="inspect",
                goal="Check a small input",capabilities=["inspect"],input_refs=[],
                requested_budget={"max_cost":2})
        count=self.connection.execute("SELECT count(*) AS n FROM runtime.tasks WHERE parent_task_id=%s",
                                     (parent,)).fetchone()["n"]
        self.assertEqual(count,0)

    def test_child_authorization_reserves_budget_and_dispatch_intent_atomically(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        parent=ident("task")
        coordinator.create_task(parent,campaign,task_type="coordinate",idempotency_key=ident("parent-key"),
            goal_id=goal,budget={"max_cost":10},max_children=2,depth_remaining=1,
            required_capabilities=["coordinate"])
        worker=ident("worker"); coordinator.register_worker(worker,["coordinate"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        coordinator.transition(parent,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        child=ident("child")
        response=coordinator.authorize_child(request_id=ident("request"),parent_task_id=parent,
            parent_attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],child_task_id=child,
            idempotency_key=ident("child-key"),task_type="inspect",goal="Inspect a neutral input",
            capabilities=["inspect"],input_refs=[],requested_budget={"max_cost":3})
        self.assertTrue(response["approved"])
        self.assertEqual(self.connection.execute("SELECT reserved_budget->>'max_cost' AS value FROM runtime.tasks WHERE task_id=%s",
            (parent,)).fetchone()["value"],"3.0")
        self.assertEqual(self.connection.execute("SELECT count(*) AS n FROM runtime.outbox WHERE idempotency_key=%s",
            (f"dispatch:{child}",)).fetchone()["n"],1)

    def test_routing_uses_capabilities_health_and_persists_alternatives(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="transform",idempotency_key=ident("task-key"),
            goal_id=goal,required_capabilities=["structured_output"])
        worker=ident("worker"); coordinator.register_worker(worker,["structured_output"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        for executor,capabilities,health in (("exec-a",["structured_output"],"HEALTHY"),
                ("exec-b",["structured_output"],"HEALTHY"),
                ("exec-c",["code_review"],"HEALTHY"),
                ("exec-unhealthy",["structured_output"],"UNHEALTHY")):
            self.connection.execute("""INSERT INTO runtime.executors
                (executor_id,adapter_type,backend,version,configuration_ref,capabilities,location,metadata)
                VALUES (%s,'test-adapter','generic','1','config://test',%s,'LOCAL',%s)""",
                (executor,Jsonb(capabilities),Jsonb({"health":health})))
        selected=coordinator.choose_executor(task_id=task,attempt_id=lease["attempt_id"],
            required_capabilities=["structured_output"],policy_ref="capability-policy-v1",
            preferred_executor="exec-b")
        self.assertEqual(selected,"exec-b")
        decision=self.connection.execute("SELECT eligible_executor_refs,selected_executor_ref FROM runtime.routing_decisions WHERE task_id=%s",
            (task,)).fetchone()
        self.assertEqual(set(decision["eligible_executor_refs"]),{"exec-a","exec-b"})
        self.assertEqual(decision["selected_executor_ref"],"exec-b")

    def test_reconciliation_reports_orphaned_attempt_and_cleanup_work(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),
                                goal_id=goal,required_capabilities=["inspect"])
        worker=ident("worker"); coordinator.register_worker(worker,["inspect"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        self.connection.execute("UPDATE runtime.leases SET status='RELEASED' WHERE task_id=%s",(task,))
        self.connection.execute("""INSERT INTO runtime.sandboxes
            (sandbox_id,task_id,attempt_id,implementation,workspace_ref,status,cleanup_status,policy_ref)
            VALUES (%s,%s,%s,'test','temporary://workspace','CLEANUP_PENDING','PENDING','test-policy')""",
            (ident("sandbox"),task,lease["attempt_id"]))
        report=coordinator.reconcile_runtime(scan_id=ident("scan"))
        # Startup reconciliation now fences and moves orphaned in-flight work
        # to RETRY_PENDING before it reports the remaining invariant counts.
        self.assertEqual(report["orphan_attempts_reconciled"],[task])
        self.assertEqual(report["tasks_without_current_lease"],0)
        self.assertEqual(report["orphaned_attempts"],0)
        self.assertEqual(report["incomplete_sandbox_cleanup"],1)
        self.assertGreater(report["pending_outbox"],0)
        event=self.connection.execute("SELECT payload FROM runtime.events WHERE event_type='RUNTIME_RECONCILIATION_COMPLETED' AND correlation_id=%s",
            (report["scan_id"],)).fetchone()
        self.assertEqual(event["payload"]["orphaned_attempts"],0)
        self.assertEqual(event["payload"]["orphan_attempts_reconciled"],[task])

    def test_orphaned_artifact_does_not_accept_task(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),goal_id=goal)
        worker=ident("worker"); coordinator.register_worker(worker,["inspect"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        leased_state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
        self.assertEqual(leased_state,"LEASED")
        with tempfile.TemporaryDirectory(prefix="m3-orphan-artifact-") as tmp:
            store=ArtifactStore(Path(tmp)/"artifacts")
            manifest=store.put(b'{"orphan":true}',kind="executor-output",producer_execution_id=ident("exec"),
                               producer_attempt_id=lease["attempt_id"])
            with self.assertRaises(ValueError):
                coordinator.accept_verified_result(task_id=task,attempt_id=lease["attempt_id"],
                    lease_epoch=lease["lease_epoch"],artifact_store=store)
            state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            accepted=self.connection.execute("SELECT count(*) AS n FROM runtime.artifacts WHERE artifact_id=%s",
                                             (manifest.artifact_id,)).fetchone()["n"]
            self.assertEqual(state,"LEASED")
            self.assertEqual(accepted,0)

    def test_corrupt_registered_artifact_is_quarantined(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),goal_id=goal)
        worker=ident("worker"); coordinator.register_worker(worker,["inspect"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        with tempfile.TemporaryDirectory(prefix="m3-corrupt-artifact-") as tmp:
            store=ArtifactStore(Path(tmp)/"artifacts")
            manifest=store.put(b"verified bytes",kind="executor-output",producer_execution_id=ident("exec"),
                               producer_attempt_id=lease["attempt_id"])
            coordinator.register_artifact(task_id=task,attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],manifest=manifest,artifact_store=store,
                input_manifest_hash=hashlib.sha256(b"input").hexdigest())
            coordinator.commit_result(task_id=task,attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],artifact_refs=[manifest.artifact_id],
                result_hash=manifest.content_hash)
            (store.path_for(manifest.artifact_id)/"content").write_bytes(b"corrupted bytes")
            result=coordinator.accept_verified_result(task_id=task,attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],artifact_store=store)
            state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            artifact_status=self.connection.execute("SELECT verification_status FROM runtime.artifacts WHERE artifact_id=%s",
                                                      (manifest.artifact_id,)).fetchone()["verification_status"]
            self.assertEqual((result,state,artifact_status),("QUARANTINED","QUARANTINED","QUARANTINED"))

    def test_outbox_survives_dispatch_pause_and_duplicate_has_one_logical_effect(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task=ident("task")
        env=os.environ.copy(); env.update({"M3_TASK_ID":task,"M3_CAMPAIGN_ID":campaign,
            "M3_GOAL_ID":goal,"M3_IDEMPOTENCY_KEY":ident("task-key")})
        code="""import os
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.coordinator.service import Coordinator
db=connect(os.environ['M3_TEST_DATABASE_URL']); db.autocommit=True
db.execute('SET ROLE ' + os.environ['M3_TEST_DATABASE_ROLE'])
Coordinator(db).create_task(os.environ['M3_TASK_ID'],os.environ['M3_CAMPAIGN_ID'],
    task_type='inspect',idempotency_key=os.environ['M3_IDEMPOTENCY_KEY'],
    goal_id=os.environ['M3_GOAL_ID'])
os._exit(23)
"""
        crashed=subprocess.run([sys.executable,"-c",code],env=env,check=False)
        self.assertEqual(crashed.returncode,23)  # Process died after the DB transaction committed.
        item=self.connection.execute("SELECT outbox_id FROM runtime.outbox WHERE idempotency_key=%s",
                                     (f"dispatch:{task}",)).fetchone()
        self.assertIsNotNone(item)  # State/event/outbox survived the process death.
        effects={}; calls=[]
        def consumer(idempotency_key,payload):
            calls.append(idempotency_key)
            return effects.setdefault(idempotency_key,f"effect:{payload['task_id']}")
        dispatcher=OutboxDispatcher(self.connection,consumer)
        dispatch_key=f"dispatch:{task}"
        self.assertTrue(dispatcher.dispatch_one(ident("dispatcher"),idempotency_key=dispatch_key))
        # Simulate acknowledgement loss after the consumer accepted the idempotency key.
        self.connection.execute("UPDATE runtime.outbox SET delivered_at=NULL WHERE outbox_id=%s",(item["outbox_id"],))
        self.assertTrue(dispatcher.dispatch_one(ident("dispatcher"),idempotency_key=dispatch_key))
        receipt_count=self.connection.execute("SELECT count(*) AS n FROM runtime.dispatch_receipts WHERE outbox_id=%s",
                                              (item["outbox_id"],)).fetchone()["n"]
        self.assertEqual(len(calls),2)
        self.assertEqual(len(effects),1)
        self.assertEqual(receipt_count,1)

    def test_fake_executor_artifact_verification_acceptance_vertical_slice(self):
        coordinator,campaign,goal=self.campaign_and_goal()
        task,plan_id=ident("task"),ident("plan")
        proposal=PlanProposal(plan_id=plan_id,goal_id=goal,
            proposed_tasks=(ProposedTask("transform","structured_transform",
                required_capabilities=("structured_output",),
                output_contract={"format":"json","schema":{"type":"object",
                    "properties":{"ok":{"type":"boolean"}},"required":["ok"],
                    "additionalProperties":False}},budget={"max_cost":1}),),
            rationale_ref="artifact://vertical-slice-rationale",estimated_budget={"max_cost":1},
            created_by="deterministic-test-planner")
        coordinator.materialize_plan(proposal,campaign_id=campaign,
            task_id_for={"transform":task},idempotency_prefix=ident("task-key"))
        worker=ident("worker"); coordinator.register_worker(worker,["structured_output"])
        lease=coordinator.claim(worker); self.assertIsNotNone(lease)
        with tempfile.TemporaryDirectory(prefix="m3-slice-") as tmp:
            root=Path(tmp); repo=root/"repo"; repo.mkdir()
            subprocess.run(["git","init",str(repo)],check=True,stdout=subprocess.DEVNULL)
            subprocess.run(["git","-C",str(repo),"config","user.name","M3 Test"],check=True)
            subprocess.run(["git","-C",str(repo),"config","user.email","m3-test@example.invalid"],check=True)
            (repo/"input.txt").write_text("stable input\n",encoding="utf-8")
            subprocess.run(["git","-C",str(repo),"add","input.txt"],check=True)
            subprocess.run(["git","-C",str(repo),"commit","-m","test base"],check=True,
                           stdout=subprocess.DEVNULL)
            revision=subprocess.check_output(["git","-C",str(repo),"rev-parse","HEAD"],text=True).strip()
            sandbox_adapter=GitWorktreeSandboxAdapter(repo,root/"worktrees")
            sandbox=sandbox_adapter.prepare(sandbox_adapter.create(task,lease["attempt_id"],revision),{})
            sandbox_id=sandbox.sandbox_id
            with self.connection.transaction():
                self.connection.execute("""INSERT INTO runtime.sandboxes
                    (sandbox_id,task_id,attempt_id,implementation,base_revision,workspace_ref,status,cleanup_status,policy_ref)
                    VALUES (%s,%s,%s,'git-worktree-v1',%s,%s,'READY','NOT_STARTED','test-policy-v1')""",
                    (sandbox_id,task,lease["attempt_id"],revision,str(sandbox.workspace_path)))
                self.connection.execute("UPDATE runtime.attempts SET sandbox_id=%s WHERE attempt_id=%s",
                                        (sandbox_id,lease["attempt_id"]))
            coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,
                                    actor_id=worker)
            store=ArtifactStore(root/"artifacts")
            fake=FakeHarnessAdapter("fake-validation",("structured_output",),store)
            request=ExecutionRequest(ident("exec"),task,lease["attempt_id"],"structured_output",
                ExecutorClass.NATIVE_AGENT_PROCESS,"fake-validation",None,str(sandbox.workspace_path),
                output_contract="json",idempotency_key=ident("exec-key"),metadata={"fake_output":"{\"ok\":true}"})
            result=fake.execute(request)
            input_hash=hashlib.sha256(b"stable input\n").hexdigest()
            for artifact_ref in result.artifact_refs:
                manifest=store.verify(artifact_ref)
                coordinator.register_artifact(task_id=task,attempt_id=lease["attempt_id"],
                    lease_epoch=lease["lease_epoch"],manifest=manifest,artifact_store=store,
                    input_manifest_hash=input_hash)
            coordinator.commit_result(task_id=task,attempt_id=lease["attempt_id"],
                lease_epoch=lease["lease_epoch"],artifact_refs=list(result.artifact_refs),
                result_hash=result.output_hash or "")
            knowledge_id=coordinator.accept_verified_result(task_id=task,
                attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],artifact_store=store)
            state=self.connection.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
            self.assertEqual(state,"ACCEPTED")
            self.assertEqual(self.connection.execute("SELECT plan_id FROM runtime.tasks WHERE task_id=%s",
                (task,)).fetchone()["plan_id"],plan_id)
            self.assertEqual(self.connection.execute("SELECT status FROM runtime.plan_proposals WHERE plan_id=%s",
                (plan_id,)).fetchone()["status"],"MATERIALIZED")
            self.assertIsNotNone(self.connection.execute("SELECT 1 FROM runtime.knowledge_objects WHERE knowledge_id=%s",
                                                          (knowledge_id,)).fetchone())
            sandbox_adapter.destroy(sandbox)

    def test_shadow_evolution_records_result_and_keeps_champion(self):
        evolution=EvolutionController(self.connection)
        scope=ident("scope"); champion=ident("genome"); suite=ident("suite")
        suite_definition=json.loads((ROOT/"tests/fixtures/routing-policy-eval-v1.json").read_text(encoding="utf-8"))
        suite_hash=hashlib.sha256(canonical_json(suite_definition)).hexdigest()
        champion_config={"routes":{"structured_output":"executor-general",
            "code_review":"executor-general","data_transform":"executor-general"}}
        challenger_config={"routes":{"structured_output":"executor-specialized",
            "code_review":"executor-specialized","data_transform":"executor-general"}}
        evolution.register_scope_champion(scope_id=scope,description="Routing policy evaluation",
            genome_id=champion,version="1",config_ref="artifact://champion",config=champion_config)
        with self.assertRaises(Exception):
            with self.connection.transaction():
                self.connection.execute("""INSERT INTO evolution.system_genomes
                    (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
                     mutation_rationale,created_by) VALUES (%s,%s,'duplicate','CHAMPION',%s,%s,'x','x','test')""",
                    (ident("genome"),scope,"artifact://duplicate","b"*64))
        evolution.register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref="tests/fixtures/routing-policy-eval-v1.json",integrity_hash=suite_hash,
            created_by="governance",metric_directions=suite_definition["metric_directions"])
        champion_eval=evaluate_routing_genome(champion_config,suite_definition,expected_suite_hash=suite_hash)
        challenger_eval=evaluate_routing_genome(challenger_config,suite_definition,expected_suite_hash=suite_hash)
        self.assertEqual(champion_eval.case_count,10)
        self.assertEqual(challenger_eval.metrics["verified_quality"],champion_eval.metrics["verified_quality"])
        self.assertLess(challenger_eval.metrics["cost"],champion_eval.metrics["cost"])
        result=evolution.run_shadow(scope_id=scope,observation_id=ident("obs"),
            observation_refs=["event://plateau"],observation="Repeated equivalent outcomes",
            mutation_id=ident("mutation"),candidate_genome_id=ident("genome"),candidate_version="2",
            candidate_config_ref="artifact://challenger",candidate_config=challenger_config,
            hypothesis="Alternate routing improves verified quality without cost increase",
            expected_effect={"verified_quality":"increase"},evaluator_version="deterministic-v1",
            conditions_ref="artifact://conditions",evaluation_result_refs=[champion_eval.result_ref,challenger_eval.result_ref],
            champion_metrics=champion_eval.metrics,
            candidate_metrics=challenger_eval.metrics,rationale_ref="artifact://rationale",
            evaluator_id="independent-evaluator",proposer_id="challenger-proposer")
        active=self.connection.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                       (scope,)).fetchone()["genome_id"]
        self.assertEqual(active,champion)
        self.assertEqual(result["mode"],"SHADOW")
        record=self.connection.execute("SELECT eval_suite_id,eval_suite_version,status FROM evolution.evaluation_records WHERE evaluation_id=%s",
                                       (result["evaluation_id"],)).fetchone()
        self.assertEqual((record["eval_suite_id"],record["eval_suite_version"],record["status"]),(suite,"1","COMPLETED"))
        with self.assertRaisesRegex(ValueError,"sole evaluator"):
            evolution.run_shadow(scope_id=scope,observation_id=ident("obs"),
                observation_refs=["event://same-authority"],observation="Check authority separation",
                mutation_id=ident("mutation"),candidate_genome_id=ident("genome"),candidate_version="bad",
                candidate_config_ref="artifact://bad",candidate_config={"route":"bad"},
                hypothesis="Test authority separation",expected_effect={},evaluator_version="test",
                conditions_ref="artifact://conditions",evaluation_result_refs=["artifact://result"],
                champion_metrics={"verified_quality":0.8,"cost":3},
                candidate_metrics={"verified_quality":0.9,"cost":2},rationale_ref="artifact://why",
                evaluator_id="same-person",proposer_id="same-person")
        with self.assertRaises(Exception):
            with self.connection.transaction():
                self.connection.execute("UPDATE evolution.evaluation_records SET evaluator_id='challenger-proposer' WHERE evaluation_id=%s",
                                        (result["evaluation_id"],))
        def challenger(*, quality: float, retain: bool):
            return evolution.run_shadow(scope_id=scope,observation_id=ident("obs"),
                observation_refs=["event://repeat"],observation="A separate deterministic observation",
                mutation_id=ident("mutation"),candidate_genome_id=ident("genome"),candidate_version=ident("version"),
                candidate_config_ref="artifact://variant",candidate_config={"variant":ident("variant")},
                hypothesis="Measure an alternate policy",expected_effect={"verified_quality":"change"},
                evaluator_version="deterministic-v1",conditions_ref="artifact://same-conditions",
                evaluation_result_refs=["artifact://result"],
                champion_metrics={"verified_quality":0.8,"cost":3},
                candidate_metrics={"verified_quality":quality,"cost":3},rationale_ref="artifact://why",
                evaluator_id="independent-evaluator",proposer_id="challenger-proposer",
                retain_for_diversity=retain)
        rejected=challenger(quality=0.7,retain=False)
        retained=challenger(quality=0.75,retain=True)
        self.assertEqual(rejected["decision"],"REJECT")
        self.assertEqual(retained["decision"],"RETAIN_FOR_DIVERSITY")
        retained_status=self.connection.execute("SELECT status FROM evolution.system_genomes WHERE genome_id=%s",
                                                 (retained["candidate_genome_id"],)).fetchone()["status"]
        rejected_evidence=self.connection.execute("SELECT count(*) AS n FROM evolution.evaluation_records WHERE candidate_genome_id=%s",
                                                  (rejected["candidate_genome_id"],)).fetchone()["n"]
        self.assertEqual(retained_status,"RETAINED_FOR_DIVERSITY")
        self.assertEqual(rejected_evidence,1)
        with self.assertRaises(Exception):
            with self.connection.transaction():
                self.connection.execute("UPDATE evolution.eval_suite_versions SET definition_ref='altered' WHERE eval_suite_id=%s AND version='1'",
                                        (suite,))
        active=self.connection.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                       (scope,)).fetchone()["genome_id"]
        self.assertEqual(active,champion)


if __name__ == "__main__":
    unittest.main()
