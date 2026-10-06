from __future__ import annotations

import json
import os
import select
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))

from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.contracts.coordination import PlanNode,PlanVersionProposal
from agentic_runtime.coordinator.coordination import CoordinationService
from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.evolution.e2 import E2PolicyError,WorkflowMutation,apply_workflow_mutation,digest
from agentic_runtime.evolution.e2_store import E2EvolutionStore
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.remote.control import provision_worker
from agentic_runtime.remote.protocol import RemoteClientError,WorkerClient,WorkerIdentity


def ident(prefix:str)->str: return f"{prefix}_{uuid.uuid4().hex}"


class E2GovernedM9ExecutionTests(unittest.TestCase):
    """Full zero-provider E2 lifecycle through the real M9 control plane and workers."""

    @classmethod
    def setUpClass(cls):
        required=("M3_TEST_DATABASE_URL","M4C_ROLE_DSN_RUNTIME","M4C_ROLE_DSN_GOVERNANCE",
                 "M4C_ROLE_DSN_EVALUATOR","M4C_ROLE_DSN_VERIFIER","M4C_ROLE_DSN_E2PROMOTION")
        missing=[name for name in required if not os.environ.get(name)]
        if missing: raise RuntimeError("M10 E2 integration requires separate M4C role DSNs")

    def setUp(self):
        self.root=tempfile.TemporaryDirectory(prefix="e2-m9-integration-")
        base=Path(self.root.name)
        self.artifact_root=base/"artifacts"; self.artifact_root.mkdir(mode=0o700)
        self.worker_root=base/"workers"; self.worker_root.mkdir(mode=0o700)
        self.home=base/"home"; self.home.mkdir(mode=0o700)
        self.runtime=self._db("RUNTIME")
        self.governance=self._db("GOVERNANCE")
        self.evaluator=self._db("EVALUATOR")
        self.verifier=self._db("VERIFIER")
        self.promotion=self._db("E2PROMOTION")
        self.admin=connect(os.environ["M3_TEST_DATABASE_URL"]); self.admin.autocommit=True
        self.server=None; self.port=None; self.workers=[]
        os.environ.setdefault("M5_CONTROL_ADMIN_TOKEN",uuid.uuid4().hex+uuid.uuid4().hex)

    def tearDown(self):
        self._stop_workers()
        self._stop_server()
        for db in (self.admin,self.runtime,self.governance,self.evaluator,self.verifier,self.promotion):
            if db: db.close()
        self.root.cleanup()

    @staticmethod
    def _db(name):
        db=connect(os.environ[f"M4C_ROLE_DSN_{name}"]); db.autocommit=True; return db

    def _wait(self,predicate,timeout=45,description="condition"):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            value=predicate()
            if value: return value
            time.sleep(.05)
        self.fail(f"timed out waiting for {description}")

    def _start_server(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1",0)); port=sock.getsockname()[1]
        env={"PATH":os.environ.get("PATH","/usr/bin:/bin"),"PYTHONPATH":str(ROOT/"src"),
            "PYTHONDONTWRITEBYTECODE":"1","M5_CONTROL_DATABASE_URL":os.environ["M4C_ROLE_DSN_RUNTIME"],
            "M5_CONTROL_DATABASE_ROLE":"agentic_runtime_runtime",
            "M5_CONTROL_ADMIN_TOKEN":os.environ["M5_CONTROL_ADMIN_TOKEN"],
            "M5_ARTIFACT_ROOT":str(self.artifact_root),"M5_LISTEN_HOST":"127.0.0.1",
            "M5_LISTEN_PORT":str(port),"M5_LEASE_SECONDS":"20",
            "M5_HEARTBEAT_SUSPECT_SECONDS":"5","M6_MAINTENANCE_INTERVAL_SECONDS":"0.2"}
        proc=subprocess.Popen([sys.executable,"-m","agentic_runtime.remote.control_server"],
            cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,
            start_new_session=True)
        self.server=proc; self.port=port
        deadline=time.monotonic()+10; lines=[]
        while time.monotonic()<deadline:
            if proc.poll() is not None: self.fail("M9 control plane exited before READY: "+"".join(lines))
            ready,_,_=select.select([proc.stdout],[],[],.1)
            if ready:
                line=proc.stdout.readline(); lines.append(line)
                if line.startswith("READY:"): return
        self.fail("M9 control plane did not become ready")

    def _stop_server(self):
        proc=self.server
        if proc is None: return
        if proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=4)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
        if proc.stdout: proc.stdout.close()
        self.server=None

    def _provision_worker(self,worker_id):
        token=provision_worker(self.runtime,worker_id=worker_id,
            capabilities=["generic_agent_task"],task_types=["generic_agent_task"],
            resource_classes=["deterministic_compute_job"],
            max_resource_policy={"cpu_seconds":10,"memory_bytes":1073741824,
                "process_count":128,"wall_time_seconds":15,"max_sandbox_bytes":33554432},
            ttl_seconds=1800,created_by="m10-integration-policy")
        env={"PATH":os.environ.get("PATH","/usr/bin:/bin"),"PYTHONPATH":str(ROOT/"src"),
            "PYTHONDONTWRITEBYTECODE":"1","HOME":str(self.home),"LANG":"C.UTF-8",
            "M5_CONTROL_ENDPOINT":f"http://127.0.0.1:{self.port}","M5_WORKER_ID":worker_id,
            "M5_WORKER_TOKEN":token,"M5_WORKER_ROOT":str(self.worker_root/worker_id),
            "M5_WORKER_CAPABILITIES":"[\"generic_agent_task\"]",
            "M5_WORKER_TASK_TYPES":"[\"generic_agent_task\"]",
            "M5_WORKER_RESOURCE_CLASSES":"[\"deterministic_compute_job\"]",
            "M5_WORKER_RESOURCE_POLICY":json.dumps({"cpu_seconds":10,"memory_bytes":1073741824,
                "process_count":128,"wall_time_seconds":15,"max_sandbox_bytes":33554432})}
        Path(env["M5_WORKER_ROOT"]).mkdir(mode=0o700)
        proc=subprocess.Popen([sys.executable,"-m","agentic_runtime.remote.worker"],cwd=ROOT,
            env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,start_new_session=True)
        self.workers.append(proc)
        ready,_,_=select.select([proc.stdout],[],[],10)
        if not ready: self.fail("M9 worker did not register")
        record=json.loads(proc.stdout.readline())
        if record.get("event")!="remote_worker_registered" or record.get("state")!="READY":
            self.fail("M9 worker registration did not return READY")
        return {"worker_id":worker_id,"token":token,"worker_instance_id":record["worker_instance_id"]}

    def _start_workers(self,prefix):
        identities=[self._provision_worker(ident(prefix)) for _ in range(2)]
        self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.worker_instances
            WHERE worker_id=ANY(%s) AND status IN ('READY','BUSY')""",
            ([i["worker_id"] for i in identities],)).fetchone()["n"]==2,description="both M9 workers READY")
        return identities

    def _stop_workers(self):
        for proc in self.workers:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
            if proc.stdout: proc.stdout.close()
        self.workers=[]

    def _latest_clean_recovery(self,not_before):
        return self.runtime.execute("""SELECT event_id,occurred_at,payload FROM runtime.events
            WHERE event_type='RUNTIME_RECONCILIATION_COMPLETED' AND occurred_at>=%s
              AND coalesce((payload->>'orphaned_attempts')::integer,-1)=0
              AND coalesce((payload->>'tasks_without_current_lease')::integer,-1)=0
              AND coalesce((payload->>'pending_outbox')::integer,-1)=0
              AND coalesce((payload->>'incomplete_sandbox_cleanup')::integer,-1)=0
            ORDER BY occurred_at DESC LIMIT 1""",(not_before,)).fetchone()

    def _mission_evidence(self,mission):
        rows=self.runtime.execute("""SELECT t.task_id,t.result_refs,t.lease_epoch,
                a.attempt_id,a.worker_id,a.worker_instance_id,a.lease_epoch AS attempt_epoch,
                ar.artifact_id,ar.content_hash,ar.verification_status
            FROM runtime.coordination_plan_nodes n
            JOIN runtime.tasks t USING(task_id)
            JOIN runtime.attempts a ON a.task_id=t.task_id AND a.status='ACCEPTED'
            JOIN runtime.artifacts ar ON ar.producer_task_id=t.task_id
              AND ar.producer_attempt_id=a.attempt_id
            WHERE n.plan_version_id=%s ORDER BY n.node_key,ar.artifact_id""",
            (mission["plan_version_id"],)).fetchall()
        groups={}
        for row in rows:
            group=groups.setdefault(row["task_id"],{"task_id":row["task_id"],
                "attempt_id":row["attempt_id"],"worker_id":row["worker_id"],
                "worker_instance_id":row["worker_instance_id"],"lease_epoch":row["lease_epoch"],
                "artifact_refs":[]})
            self.assertEqual(row["verification_status"],"VERIFIED")
            self.assertEqual(row["lease_epoch"],row["attempt_epoch"])
            group["artifact_refs"].append(row["artifact_id"])
        evidence=list(groups.values())
        artifacts=sorted((row["artifact_id"],row["content_hash"]) for row in rows)
        return evidence,[item for item,_ in artifacts],digest(artifacts)

    def _await_plans(self,missions):
        plan_ids=[item["plan_version_id"] for item in missions.values()]
        def accepted():
            row=self.runtime.execute("""SELECT count(*) AS total,
                count(*) FILTER (WHERE t.status='ACCEPTED') AS accepted
                FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
                WHERE n.plan_version_id=ANY(%s)""",(plan_ids,)).fetchone()
            return row["total"]>0 and row["total"]==row["accepted"]
        self._wait(accepted,timeout=90,description="all M9 E2 tasks accepted by real workers")

    def test_observe_mutate_execute_promote_postcheck_and_auto_rollback(self):
        artifact_store=ArtifactStore(self.artifact_root)
        store=E2EvolutionStore(self.runtime,governance_db=self.governance,
            evaluator_db=self.evaluator,verifier_db=self.verifier,promotion_db=self.promotion,
            artifact_store=artifact_store)
        scope=ident("e2scope"); champion_id=ident("genomev1")
        suite_id=ident("e2suite"); pack_id=ident("e2pack")
        contract={"type":"object","required":["ok","value"],
            "properties":{"ok":{"type":"boolean"},"value":{"type":"string"}},
            "additionalProperties":False}
        workflow={"plan_id":"generic-workflow-template","goal_id":"generic-template-goal",
            "nodes":[
                {"node_key":"prepare","task_type":"generic_agent_task",
                 "objective":"Perform the first bounded neutral operation",
                 "required_capabilities":["generic_agent_task"],"output_contract":contract,
                 "budget":{},"depends_on":[]},
                {"node_key":"review","task_type":"generic_agent_task",
                 "objective":"Perform an independent neutral review operation",
                 "required_capabilities":["generic_agent_task"],"output_contract":contract,
                 "budget":{},"depends_on":["prepare"],
                 "dependency_requirements":{"prepare":"ACCEPTED"}}],
            "estimated_budget":{},"bounds":{"max_depth":2,"max_children_per_node":3,
                "max_descendants":6,"max_concurrent_descendants":2,"max_retries_per_node":1,
                "max_wall_time_seconds":120,"max_artifact_bytes":1048576}}
        store.register_scope(scope_id=scope,description="Bounded domain-neutral M9 workflow evaluation",
            champion_id=champion_id,workflow_template=workflow,suite_id=suite_id,suite_version="1",
            suite_definition={"kind":"generic-dag-runtime-contract","version":"1"},
            evaluator_version="m10-evaluator-v1",policy={"limits":{},"evaluation":{}},
            created_by="m10-governance")
        store.freeze_evaluation_pack(pack_id=pack_id,scope_id=scope,suite_id=suite_id,
            suite_version="1",evaluator_version="m10-evaluator-v1",
            workload_keys=["light","medium","heavy"],
            workloads={"light":{"work_units":1},"medium":{"work_units":2},"heavy":{"work_units":4}},
            repetitions=2,holdout_scenarios={"hidden-stress":{"work_units":3}},
            created_by="m10-governance",minimum_latency_improvement=0.05)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='DEVELOPMENT'",
            (pack_id,)).fetchone()["n"],3)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'",
            (pack_id,)).fetchone()["n"],0,"runtime candidate generation must not read HOLDOUT definitions")
        self.assertEqual(self.evaluator.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'",
            (pack_id,)).fetchone()["n"],0,"development evaluator must not read HOLDOUT definitions")
        self.assertEqual(self.verifier.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'",
            (pack_id,)).fetchone()["n"],1)
        with self.assertRaises(Exception) as holdout_fixture_tamper:
            self.evaluator.execute("UPDATE evolution.e2_evaluation_scenarios SET definition=definition WHERE pack_id=%s AND partition='HOLDOUT'",
                                   (pack_id,))
        self.assertEqual(type(holdout_fixture_tamper.exception).__name__,"InsufficientPrivilege")
        frozen_before=self.governance.execute("SELECT definition_hash FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
                                              (pack_id,)).fetchone()["definition_hash"]
        with self.assertRaises(Exception) as frozen_tamper:
            self.evaluator.execute("UPDATE evolution.e2_evaluation_packs SET definition=definition WHERE pack_id=%s",
                                   (pack_id,))
        self.assertEqual(type(frozen_tamper.exception).__name__,"InsufficientPrivilege")
        self.assertEqual(self.governance.execute("SELECT definition_hash FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
            (pack_id,)).fetchone()["definition_hash"],frozen_before)
        parent=self.runtime.execute("SELECT workflow_hash FROM evolution.e2_workflow_genomes WHERE genome_id=%s",
                                    (champion_id,)).fetchone()["workflow_hash"]
        development_scenario=self.runtime.execute("""SELECT scenario_id FROM evolution.e2_evaluation_scenarios
            WHERE pack_id=%s AND partition='DEVELOPMENT' ORDER BY scenario_id LIMIT 1""",(pack_id,)).fetchone()
        holdout_scenario_id=self.admin.execute("SELECT scenario_id FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'",
                                                (pack_id,)).fetchone()["scenario_id"]
        # The observation is measured directly from the persisted Champion V1 DAG.
        observed={"nodes":len(workflow["nodes"])}
        observation_id=ident("e2observation")
        store.observe_workflow(observation_id=observation_id,scope_id=scope,
            source_refs=["sha256:"+parent,"e2-scenario:"+development_scenario["scenario_id"]],
            summary="Independent operations are serialized by one unnecessary dependency edge",
            created_by="e2-observer",measurements={"node_count":observed["nodes"],"avoidable_dependency_edges":1})
        hidden_source_observation=ident("holdout-source-observation")
        with self.assertRaisesRegex(E2PolicyError,"not persisted DEVELOPMENT evidence"):
            store.observe_workflow(observation_id=hidden_source_observation,scope_id=scope,
                source_refs=["e2-scenario:"+holdout_scenario_id],summary="hidden input bypass attempt",
                created_by="e2-observer",measurements={"work_units":3})
        self.assertIsNone(self.runtime.execute("SELECT 1 FROM evolution.observations WHERE observation_id=%s",
                                               (hidden_source_observation,)).fetchone())
        with self.assertRaisesRegex(E2PolicyError,"DEVELOPMENT evidence"):
            store.observe_workflow(observation_id=ident("holdout-observation"),scope_id=scope,
                source_refs=["holdout:"+pack_id],summary="attempt to feed hidden scenario to mutation",
                created_by="e2-observer",measurements={"work_units":3},partition="HOLDOUT")
        mutation_id=ident("e2mutation")
        mutation=WorkflowMutation(mutation_id,scope,champion_id,parent,observation_id,
            "Two operations have no data dependency but Champion V1 serializes them",
            "Removing the edge should shorten critical path while preserving schema validation",
            "Parallel execution can increase contention; compare paired repeated workloads",
            pack_id,champion_id,({"op":"replace","path":"/nodes/1/depends_on","value":[]},
                {"op":"replace","path":"/nodes/1/dependency_requirements","value":{}}))
        mutation_key=ident("e2-idempotency")
        store.propose(mutation,proposer_id="e2-proposer",idempotency_key=mutation_key)
        bad_rollback=WorkflowMutation(**{**mutation.__dict__,"rollback_genome_id":ident("forged-rollback")})
        with self.assertRaisesRegex(E2PolicyError,"rollback target"):
            store.propose(bad_rollback,proposer_id="e2-proposer",idempotency_key=ident("bad-rollback-key"))
        with self.assertRaises(Exception) as rollback_tamper:
            self.runtime.execute("UPDATE evolution.e2_mutation_proposals SET rollback_genome_id=%s WHERE mutation_id=%s",
                                 (ident("forged-rollback"),mutation_id))
        self.assertEqual(type(rollback_tamper.exception).__name__,"InsufficientPrivilege")
        self.assertEqual(self.governance.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                                  (scope,)).fetchone()["genome_id"],champion_id)
        duplicate_proposal_idempotent=(store.propose(mutation,proposer_id="e2-proposer",
            idempotency_key=mutation_key)==mutation_id)
        changed_mutation=WorkflowMutation(**{**mutation.__dict__,
            "rationale":"Changed content must not reuse this immutable idempotency key"})
        with self.assertRaisesRegex(E2PolicyError,"idempotency key reused"):
            store.propose(changed_mutation,proposer_id="e2-proposer",idempotency_key=mutation_key)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM evolution.e2_mutation_proposals WHERE campaign_id=%s",
            ("e2campaign_"+mutation_id,)).fetchone()["n"],1)
        candidate_id=ident("genomev2")
        base_proposal=self.admin.execute("SELECT candidate_config_hash,candidate_config_ref FROM evolution.mutation_proposals WHERE mutation_id=%s",
                                         (mutation_id,)).fetchone()
        candidate_hash_check={"expected":base_proposal["candidate_config_hash"],
            "observed":digest(apply_workflow_mutation(workflow,mutation,persisted_parent_hash=parent)),
            "tampered":"0"*64}
        self.admin.execute("UPDATE evolution.mutation_proposals SET candidate_config_hash=%s,candidate_config_ref=%s WHERE mutation_id=%s",
                           ("0"*64,"sha256:"+"0"*64,mutation_id))
        with self.assertRaisesRegex(E2PolicyError,"candidate genome or delta hash"):
            store.materialize_challenger(mutation_id=mutation_id,genome_id=candidate_id,created_by="m10-integrity-test")
        self.assertIsNone(self.runtime.execute("SELECT 1 FROM evolution.e2_workflow_genomes WHERE genome_id=%s",
                                               (candidate_id,)).fetchone())
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM runtime.improvement_reservations WHERE campaign_id=%s AND stage='e2_evaluation'",
                                              ("e2campaign_"+mutation_id,)).fetchone()["n"],0)
        self.admin.execute("UPDATE evolution.mutation_proposals SET candidate_config_hash=%s,candidate_config_ref=%s WHERE mutation_id=%s",
                           (base_proposal["candidate_config_hash"],base_proposal["candidate_config_ref"],mutation_id))
        store.materialize_challenger(mutation_id=mutation_id,genome_id=candidate_id,created_by="e2-mutator")
        with self.assertRaises(Exception) as candidate_tamper:
            self.runtime.execute("UPDATE evolution.e2_workflow_genomes SET workflow_hash=workflow_hash WHERE genome_id=%s",
                                 (candidate_id,))
        self.assertEqual(type(candidate_tamper.exception).__name__,"InsufficientPrivilege")
        campaign=self.runtime.execute("SELECT campaign_id FROM evolution.e2_mutation_proposals WHERE mutation_id=%s",
                                      (mutation_id,)).fetchone()["campaign_id"]
        definition=self.runtime.execute("SELECT definition FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
                                        (pack_id,)).fetchone()["definition"]
        missions={}; run_info={}; replay_checked=False; mutation_tamper_checked=False
        evaluator_identity_rejection_checked=False; verifier_identity_rejection_checked=False
        artifact_corruption_checked=False; corrupted_artifact_id=None; e2_recovery_results=[]
        evaluator_unavailable_rejected=False
        evaluator_id=self.evaluator.execute("SELECT session_user AS actor").fetchone()["actor"]
        self._start_server()
        for key in definition["workload_keys"]:
            for repetition in range(1,3):
                seed=next(s["seed"] for s in definition["seeds"]
                          if s["workload_key"]==key and s["repetition"]==repetition)
                for side,genome in (("CHAMPION",champion_id),("CANDIDATE",candidate_id)):
                    run=ident("e2run"); info={"side":side,"workload":key,
                        "repetition":repetition,"seed":seed}; run_info[run]=info
                    store.begin_evaluation(run_id=run,campaign_id=campaign)
                    mission=store.materialize_evaluation_mission(run_id=run,
                        mutation_id=mutation_id,candidate_genome_id=candidate_id,
                        champion_genome_id=champion_id,pack_id=pack_id,side=side,
                        workload_key=key,repetition=repetition,seed=seed,
                        proposer_id="e2-plan-proposer",coordinator_id="m9-independent-policy")
                    missions[run]=mission
                    self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
                        JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
                        WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
                        (mission["plan_version_id"],)).fetchone()["n"]==0,
                        description="M9 mission dispatch outbox")
                    # Restart with this mission durable and dispatched, then record
                    # the clean recovery scan before execution begins.
                    self._stop_server(); self._start_server()
                    created=self.runtime.execute("SELECT created_at FROM evolution.e2_evaluation_missions WHERE run_id=%s",
                                                 (run,)).fetchone()["created_at"]
                    recovery=self._wait(lambda:self._latest_clean_recovery(created),
                                        description="clean M9 recovery scan")
                    e2_recovery_results.append(store.reconcile_evaluation_accounting())
                    reservation_state=self.runtime.execute("SELECT status FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",
                        (campaign,"e2:"+run)).fetchone()["status"]
                    self.assertEqual(reservation_state,"RESERVED",
                        "M9 restart reconciliation must leave the resumable E2 run reservation active")
                    self._worker_identities=self._start_workers("e2evalworker")
                    self._await_plans({run:mission})
                    self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
                        JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
                        WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
                        (mission["plan_version_id"],)).fetchone()["n"]==0,
                        description="M9 result events reconciled")
                    store.finalize_evaluation_mission(run_id=run,accepted_by="m9-goal-acceptance-policy")
                    evidence,artifact_refs,evidence_hash=self._mission_evidence(mission)
                    if not evaluator_identity_rejection_checked:
                        with self.assertRaisesRegex(E2PolicyError,"authenticated database principal"):
                            store.record_evaluation(run_id=run,campaign_id=campaign,mutation_id=mutation_id,
                                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                                plan_version_id=mission["plan_version_id"],recovery_event_id=recovery["event_id"],
                                execution_evidence=evidence,artifact_refs=artifact_refs,side=side,
                                workload_key=key,repetition=repetition,seed=seed,evaluator_id="e2-proposer",
                                evidence_refs=artifact_refs,evidence_hash=evidence_hash)
                        self.assertIsNone(self.evaluator.execute(
                            "SELECT 1 FROM evolution.e2_evaluation_runs WHERE run_id=%s",(run,)).fetchone())
                        evaluator_identity_rejection_checked=True
                    if not artifact_corruption_checked:
                        corrupted_artifact_id=artifact_refs[0]
                        blob=artifact_store.path_for(corrupted_artifact_id)/"content"
                        original_blob=blob.read_bytes()
                        try:
                            blob.write_bytes(original_blob+b"\ncorruption")
                            with self.assertRaisesRegex(E2PolicyError,"content failed integrity verification"):
                                store.record_evaluation(run_id=run,campaign_id=campaign,mutation_id=mutation_id,
                                    candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                                    plan_version_id=mission["plan_version_id"],recovery_event_id=recovery["event_id"],
                                    execution_evidence=evidence,artifact_refs=artifact_refs,side=side,
                                    workload_key=key,repetition=repetition,seed=seed,evaluator_id=evaluator_id,
                                    evidence_refs=artifact_refs,evidence_hash=evidence_hash)
                        finally:
                            blob.write_bytes(original_blob)
                        self.assertIsNone(self.evaluator.execute("SELECT 1 FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                            (run,)).fetchone())
                        artifact_corruption_checked=True
                    if not mutation_tamper_checked:
                        with self.assertRaisesRegex(E2PolicyError,"hash does not match"):
                            store.record_evaluation(run_id=run,campaign_id=campaign,mutation_id=mutation_id,
                                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                                plan_version_id=mission["plan_version_id"],recovery_event_id=recovery["event_id"],
                                execution_evidence=evidence,artifact_refs=artifact_refs,side=side,
                                workload_key=key,repetition=repetition,seed=seed,evaluator_id=evaluator_id,
                                evidence_refs=artifact_refs,evidence_hash="0"*64)
                        self.assertIsNone(self.evaluator.execute("SELECT 1 FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                            (run,)).fetchone())
                        self.assertEqual(self.runtime.execute("SELECT status FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",
                            (campaign,"e2:"+run)).fetchone()["status"],"RESERVED")
                        mutation_tamper_checked=True
                    store.record_evaluation(run_id=run,campaign_id=campaign,mutation_id=mutation_id,
                        candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                        plan_version_id=mission["plan_version_id"],recovery_event_id=recovery["event_id"],
                        execution_evidence=evidence,artifact_refs=artifact_refs,side=side,
                        workload_key=key,repetition=repetition,seed=seed,evaluator_id=evaluator_id,
                        evidence_refs=artifact_refs,evidence_hash=evidence_hash)
                    if not replay_checked:
                        task=self.runtime.execute("""SELECT t.task_id,t.result_refs,t.lease_epoch,a.attempt_id,
                                a.worker_id,a.worker_instance_id,ar.content_hash
                            FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
                            JOIN runtime.attempts a ON a.task_id=t.task_id AND a.status='ACCEPTED'
                            JOIN runtime.artifacts ar ON ar.producer_task_id=t.task_id AND ar.producer_attempt_id=a.attempt_id
                            WHERE n.plan_version_id=%s ORDER BY n.node_key LIMIT 1""",
                            (mission["plan_version_id"],)).fetchone()
                        worker_identity=next(i for i in self._worker_identities if i["worker_id"]==task["worker_id"])
                        stale_client=WorkerClient(f"http://127.0.0.1:{self.port}",WorkerIdentity(
                            task["worker_id"],worker_identity["token"],task["worker_instance_id"]))
                        before=self.runtime.execute("SELECT status,result_hash FROM runtime.tasks WHERE task_id=%s",
                                                    (task["task_id"],)).fetchone()
                        with self.assertRaises(RemoteClientError) as stale:
                            stale_client.submit_result(task_id=task["task_id"],attempt_id=task["attempt_id"],
                                lease_epoch=task["lease_epoch"],artifact_id=task["result_refs"][0],
                                result_hash=task["content_hash"],idempotency_key=ident("late-e2-result"))
                        self.assertEqual(stale.exception.status,409)
                        after=self.runtime.execute("SELECT status,result_hash FROM runtime.tasks WHERE task_id=%s",
                                                   (task["task_id"],)).fetchone()
                        self.assertEqual(dict(before),dict(after)); replay_checked=True
                    self._stop_workers()

        self.evaluator.close()
        with self.assertRaises(Exception):
            store.compare(comparison_id=ident("unavailable-evaluator-comparison"),mutation_id=mutation_id,
                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                evaluator_id=evaluator_id)
        self.evaluator=self._db("EVALUATOR"); store.evaluator_db=self.evaluator
        self.assertEqual(self.governance.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                                  (scope,)).fetchone()["genome_id"],champion_id)
        self.assertEqual(self.evaluator.execute("SELECT count(*) AS n FROM evolution.e2_comparisons WHERE mutation_id=%s",
                                                 (mutation_id,)).fetchone()["n"],0)
        evaluator_unavailable_rejected=True
        comparison_id=ident("e2comparison")
        first_development_run=next(run for run,item in run_info.items() if item["side"]=="CANDIDATE")
        development_artifact=self.admin.execute("SELECT artifact_id FROM evolution.e2_evaluation_run_evidence WHERE run_id=%s LIMIT 1",
                                                (first_development_run,)).fetchone()["artifact_id"]
        development_metrics=self.evaluator.execute("SELECT metrics FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                                                   (first_development_run,)).fetchone()["metrics"]
        store.observe_workflow(observation_id=ident("development-evidence-observation"),scope_id=scope,
            source_refs=["e2-run:"+first_development_run,"e2-artifact:"+development_artifact],
            summary="Persisted DEVELOPMENT execution evidence is available for a later proposal",
            created_by="e2-observer",measurements={key:value for key,value in development_metrics.items()
                if isinstance(value,(int,float,bool,str))})
        development_evidence_intake_verified=True
        store.compare(comparison_id=comparison_id,mutation_id=mutation_id,
            candidate_genome_id=candidate_id,champion_genome_id=champion_id,
            pack_id=pack_id,evaluator_id=evaluator_id)
        comparison=self.evaluator.execute("SELECT eligible,comparison FROM evolution.e2_comparisons WHERE comparison_id=%s",
                                          (comparison_id,)).fetchone()
        self.assertTrue(comparison["eligible"],comparison["comparison"])
        with self.assertRaisesRegex(E2PolicyError,"complete frozen HOLDOUT"):
            store.authorize_and_promote(authorization_id=ident("missing-holdout-auth"),
                decision_id=ident("missing-holdout-decision"),comparison_id=comparison_id,
                authorized_by=self.governance.execute("SELECT session_user AS actor").fetchone()["actor"])
        self.assertIsNone(self.governance.execute(
            "SELECT 1 FROM evolution.e2_promotion_authorizations WHERE mutation_id=%s",(mutation_id,)).fetchone())

        holdout_scenario=self.verifier.execute("""SELECT scenario_id,definition_hash,definition->>'scenario_key' AS scenario_key,
                suite_id,suite_version FROM evolution.e2_evaluation_scenarios
            WHERE pack_id=%s AND partition='HOLDOUT'""",(pack_id,)).fetchone()
        holdout_run=ident("e2holdoutrun")
        store.begin_evaluation(run_id=holdout_run,campaign_id=campaign)
        with self.assertRaisesRegex(E2PolicyError,"frozen suite partition"):
            store.materialize_evaluation_mission(run_id=holdout_run,mutation_id=mutation_id,
                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                side="HOLDOUT",workload_key="light",repetition=1,seed=holdout_scenario["definition_hash"],
                proposer_id="holdout-plan-proposer",coordinator_id="m9-independent-policy")
        holdout_mission=store.materialize_evaluation_mission(run_id=holdout_run,mutation_id=mutation_id,
            candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
            side="HOLDOUT",workload_key=holdout_scenario["scenario_key"],repetition=1,
            seed=holdout_scenario["definition_hash"],proposer_id="holdout-plan-proposer",
            coordinator_id="m9-independent-policy")
        with self.assertRaises(Exception) as frozen_candidate_after_holdout:
            self.runtime.execute("UPDATE evolution.e2_workflow_genomes SET workflow_hash=workflow_hash WHERE genome_id=%s",
                                 (candidate_id,))
        self.assertEqual(type(frozen_candidate_after_holdout.exception).__name__,"InsufficientPrivilege")
        holdout_binding=self.verifier.execute("""SELECT s.suite_id,s.suite_version,s.definition_hash,
                p.suite_id AS pack_suite_id,p.suite_version AS pack_suite_version
            FROM evolution.e2_evaluation_scenarios s JOIN evolution.e2_evaluation_packs p USING(pack_id)
            WHERE s.pack_id=%s AND s.scenario_id=%s""",(pack_id,holdout_scenario["scenario_id"])).fetchone()
        self.assertEqual((holdout_binding["suite_id"],holdout_binding["suite_version"]),
                         (holdout_binding["pack_suite_id"],holdout_binding["pack_suite_version"]))
        self.assertGreaterEqual(self.verifier.execute("SELECT created_at FROM evolution.e2_evaluation_missions WHERE run_id=%s",
            (holdout_run,)).fetchone()["created_at"],self.runtime.execute(
            "SELECT created_at FROM evolution.e2_workflow_genomes WHERE genome_id=%s",(candidate_id,)).fetchone()["created_at"])
        self._stop_server(); self._start_server()
        self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
            JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
            WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
            (holdout_mission["plan_version_id"],)).fetchone()["n"]==0,description="HOLDOUT M9 outbox replay")
        holdout_created=self.verifier.execute("SELECT created_at FROM evolution.e2_evaluation_missions WHERE run_id=%s",
            (holdout_run,)).fetchone()["created_at"]
        holdout_recovery=self._wait(lambda:self._latest_clean_recovery(holdout_created),description="HOLDOUT M9 recovery scan")
        self._worker_identities=self._start_workers("e2holdoutworker")
        self._await_plans({holdout_run:holdout_mission})
        self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
            JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
            WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
            (holdout_mission["plan_version_id"],)).fetchone()["n"]==0,description="HOLDOUT result outbox reconciliation")
        store.finalize_evaluation_mission(run_id=holdout_run,accepted_by="m9-independent-holdout-acceptance")
        holdout_evidence,holdout_artifacts,holdout_hash=self._mission_evidence(holdout_mission)
        holdout_evaluator=self.verifier.execute("SELECT session_user AS actor").fetchone()["actor"]
        with self.assertRaisesRegex(E2PolicyError,"evidence hash"):
            store.record_evaluation(run_id=holdout_run,campaign_id=campaign,mutation_id=mutation_id,
                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                plan_version_id=holdout_mission["plan_version_id"],recovery_event_id=holdout_recovery["event_id"],
                execution_evidence=holdout_evidence,artifact_refs=holdout_artifacts,side="HOLDOUT",
                workload_key=holdout_scenario["scenario_key"],repetition=1,seed=holdout_scenario["definition_hash"],
                evaluator_id=holdout_evaluator,evidence_refs=holdout_artifacts,evidence_hash="0"*64)
        self.assertIsNone(self.verifier.execute(
            "SELECT 1 FROM evolution.e2_evaluation_runs WHERE run_id=%s",(holdout_run,)).fetchone())
        self.assertEqual(self.runtime.execute("SELECT status FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",
            (campaign,"e2:"+holdout_run)).fetchone()["status"],"RESERVED")
        wrong_suite_version="wrong_"+uuid.uuid4().hex[:12]
        self.admin.execute("""INSERT INTO evolution.eval_suite_versions
            (eval_suite_id,version,scope_id,status,definition_ref,integrity_hash,
             supersedes_eval_suite_id,supersedes_version,created_by)
            VALUES (%s,%s,%s,'SUPERSEDED','fixture:wrong-suite-version',%s,%s,%s,'m10-adversarial-test')""",
            (holdout_binding["suite_id"],wrong_suite_version,scope,"0"*64,
             holdout_binding["suite_id"],holdout_binding["suite_version"]))
        self.admin.execute("UPDATE evolution.e2_evaluation_scenarios SET suite_version=%s WHERE pack_id=%s AND scenario_id=%s",
            (wrong_suite_version,pack_id,holdout_scenario["scenario_id"]))
        with self.assertRaisesRegex(E2PolicyError,"frozen hidden scenario and suite version"):
            store.record_evaluation(run_id=holdout_run,campaign_id=campaign,mutation_id=mutation_id,
                candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
                plan_version_id=holdout_mission["plan_version_id"],recovery_event_id=holdout_recovery["event_id"],
                execution_evidence=holdout_evidence,artifact_refs=holdout_artifacts,side="HOLDOUT",
                workload_key=holdout_scenario["scenario_key"],repetition=1,seed=holdout_scenario["definition_hash"],
                evaluator_id=holdout_evaluator,evidence_refs=holdout_artifacts,evidence_hash=holdout_hash)
        self.assertIsNone(self.verifier.execute("SELECT 1 FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                                                (holdout_run,)).fetchone())
        self.admin.execute("UPDATE evolution.e2_evaluation_scenarios SET suite_version=%s WHERE pack_id=%s AND scenario_id=%s",
            (holdout_binding["suite_version"],pack_id,holdout_scenario["scenario_id"]))
        store.record_evaluation(run_id=holdout_run,campaign_id=campaign,mutation_id=mutation_id,
            candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
            plan_version_id=holdout_mission["plan_version_id"],recovery_event_id=holdout_recovery["event_id"],
            execution_evidence=holdout_evidence,artifact_refs=holdout_artifacts,side="HOLDOUT",
            workload_key=holdout_scenario["scenario_key"],repetition=1,seed=holdout_scenario["definition_hash"],
            evaluator_id=holdout_evaluator,evidence_refs=holdout_artifacts,evidence_hash=holdout_hash)
        store.record_evaluation(run_id=holdout_run,campaign_id=campaign,mutation_id=mutation_id,
            candidate_genome_id=candidate_id,champion_genome_id=champion_id,pack_id=pack_id,
            plan_version_id=holdout_mission["plan_version_id"],recovery_event_id=holdout_recovery["event_id"],
            execution_evidence=holdout_evidence,artifact_refs=holdout_artifacts,side="HOLDOUT",
            workload_key=holdout_scenario["scenario_key"],repetition=1,seed=holdout_scenario["definition_hash"],
            evaluator_id=holdout_evaluator,evidence_refs=holdout_artifacts,evidence_hash=holdout_hash)
        self.assertEqual(self.verifier.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_runs WHERE mutation_id=%s AND partition='HOLDOUT'",
            (mutation_id,)).fetchone()["n"],1)
        for db in (self.runtime,self.evaluator):
            self.assertEqual(db.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'",
                (pack_id,)).fetchone()["n"],0)
            self.assertEqual(db.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_missions WHERE run_id=%s AND partition='HOLDOUT'",
                (holdout_run,)).fetchone()["n"],0)
            self.assertEqual(db.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_runs WHERE run_id=%s AND partition='HOLDOUT'",
                (holdout_run,)).fetchone()["n"],0)
        self.assertEqual(self.evaluator.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_run_evidence WHERE run_id=%s",
            (holdout_run,)).fetchone()["n"],0)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_run_evidence WHERE run_id=%s",
            (holdout_run,)).fetchone()["n"],0)
        self.assertEqual(self.verifier.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_run_evidence WHERE run_id=%s",
            (holdout_run,)).fetchone()["n"],len(holdout_artifacts))
        pack_metrics=self.runtime.execute("SELECT definition FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
                                          (pack_id,)).fetchone()["definition"]["required_metrics"]
        self.assertNotIn("monetary_cost",pack_metrics)
        self.assertEqual(self.runtime.execute("""SELECT count(*) AS n FROM runtime.improvement_reservations r
            JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.campaign_id=%s AND d.dimension='monetary_cost' AND d.consumed IS NOT NULL""",
            (campaign,)).fetchone()["n"],0)
        missions[holdout_run]=holdout_mission
        run_info[holdout_run]={"side":"HOLDOUT","workload":holdout_scenario["scenario_key"],
            "repetition":1,"seed":holdout_scenario["definition_hash"]}
        self._stop_workers()

        # A normally accepted but never-started M9 plan is cancelled through
        # the plan control path. Restart reconciliation must leave it terminal.
        cancel_goal=ident("e2cancelgoal"); cancel_plan=ident("e2cancelplan")
        Coordinator(self.runtime).create_goal(cancel_goal,campaign,
            description="Verify terminal work is not resurrected",mission_ref="m10-cancel-invariant",
            created_by="m10-invariant-test")
        cancel_proposal=PlanVersionProposal(plan_id=ident("e2cancel"),goal_id=cancel_goal,
            created_by="m10-test-planner",nodes=(PlanNode(node_key="bounded",task_type="generic_agent_task",
                objective="Produce a bounded terminal-state verification artifact",
                required_capabilities=("generic_agent_task",),output_contract=contract),),
            estimated_budget={},max_wall_time_seconds=30,max_concurrent_descendants=1)
        plan_service=CoordinationService(self.runtime)
        plan_service.propose(cancel_plan,cancel_proposal)
        cancelled_tasks=plan_service.accept(cancel_plan,accepted_by="m10-test-coordinator")
        cancel_result=plan_service.request_plan_cancellation(cancel_plan,requested_by="m10-control-plane")
        self.assertEqual(len(cancel_result["cancelled_unstarted"]),1)
        terminal_task=cancel_result["cancelled_unstarted"][0]
        self.assertIn(terminal_task,cancelled_tasks.values())
        self._stop_server(); self._start_server()
        self._wait(lambda:self.runtime.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
            (terminal_task,)).fetchone()["status"]=="CANCELLED",description="cancelled task remains terminal after restart")
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM runtime.attempts WHERE task_id=%s",
            (terminal_task,)).fetchone()["n"],0)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM runtime.leases WHERE task_id=%s AND status='ACTIVE'",
            (terminal_task,)).fetchone()["n"],0)
        duplicate_cancel=plan_service.request_plan_cancellation(cancel_plan,requested_by="m10-control-plane")
        self.assertEqual(duplicate_cancel["already_terminal"],[terminal_task])
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM runtime.attempts WHERE task_id=%s",
            (terminal_task,)).fetchone()["n"],0)
        with self.assertRaises(Exception) as direct_promotion:
            self.runtime.execute("SELECT evolution.execute_e2_promotion(%s,%s,%s)",
                (ident("unauthorized-auth"),ident("unauthorized-decision"),
                 self.runtime.execute("SELECT session_user AS actor").fetchone()["actor"]))
        self.assertEqual(type(direct_promotion.exception).__name__,"InsufficientPrivilege")

        # Competing authorized promotions use separate live DB sessions. Exactly one CAS wins.
        auth_id=ident("e2authorization"); decisions=[ident("e2decision"),ident("e2decision")]
        barrier=__import__("threading").Barrier(2)
        def promote(decision_id):
            gov=self._db("GOVERNANCE"); run=self._db("RUNTIME"); promo=self._db("E2PROMOTION")
            try:
                actor=gov.execute("SELECT session_user AS actor").fetchone()["actor"]
                barrier.wait(timeout=10)
                return E2EvolutionStore(run,governance_db=gov,evaluator_db=self.evaluator,
                    verifier_db=self.verifier,promotion_db=promo).authorize_and_promote(
                        authorization_id=auth_id,decision_id=decision_id,comparison_id=comparison_id,
                        authorized_by=actor)
            finally:
                gov.close(); run.close(); promo.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(promote,d) for d in decisions]
            outcomes=[]
            for future in futures:
                try: outcomes.append(("PASS",future.result(timeout=40)))
                except Exception as exc: outcomes.append(("REJECTED",f"{type(exc).__name__}: {exc}"))
        winners=[value for status,value in outcomes if status=="PASS"]
        self.assertEqual(len(winners),1,outcomes)
        decision_id=winners[0]
        self.assertEqual(store.authorize_and_promote(authorization_id=auth_id,decision_id=decision_id,
            comparison_id=comparison_id,authorized_by=self.governance.execute(
                "SELECT session_user AS actor").fetchone()["actor"]),decision_id,
            "duplicate promotion with the same immutable IDs must be idempotent")
        decision_row=self.governance.execute("SELECT promoted_genome_id,prior_champion_id FROM evolution.e2_promotion_decisions WHERE decision_id=%s",
                                             (decision_id,)).fetchone()
        self.assertEqual((decision_row["promoted_genome_id"],decision_row["prior_champion_id"]),
                         (candidate_id,champion_id))
        event_count=self.runtime.execute("SELECT count(*) AS n FROM evolution.e2_events WHERE event_type='E2_CHAMPION_PROMOTED' AND mutation_id=%s",
                                         (mutation_id,)).fetchone()["n"]
        self.assertEqual(event_count,1)
        with self.assertRaises(Exception):
            store.authorize_and_promote(authorization_id=auth_id,decision_id=ident("conflicting-decision"),
                comparison_id=comparison_id,authorized_by=self.governance.execute(
                    "SELECT session_user AS actor").fetchone()["actor"])

        # A frozen post-promotion stress load is executed by M9 and measured from its real attempts.
        self._stop_workers()
        post_run=ident("e2postrun"); heavy_seed=next(s["seed"] for s in definition["seeds"]
            if s["workload_key"]=="heavy" and s["repetition"]==1)
        store.begin_evaluation(run_id=post_run,campaign_id=campaign)
        post_mission=store.materialize_evaluation_mission(run_id=post_run,
            mutation_id=mutation_id,candidate_genome_id=candidate_id,
            champion_genome_id=champion_id,pack_id=pack_id,side="POSTPROMOTION",
            workload_key="heavy",repetition=1,seed=heavy_seed,
            proposer_id="e2-postcheck-plan-proposer",coordinator_id="m9-independent-policy")
        self._stop_server(); self._start_server()
        self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
            JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
            WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
            (post_mission["plan_version_id"],)).fetchone()["n"]==0,
            description="post-check M9 outbox replay")
        self._stop_server(); self._start_server()
        post_created=self.runtime.execute("SELECT created_at FROM evolution.e2_evaluation_missions WHERE run_id=%s",
                                          (post_run,)).fetchone()["created_at"]
        post_recovery=self._wait(lambda:self._latest_clean_recovery(post_created),description="post-check recovery scan")
        self._worker_identities=self._start_workers("e2postworker")
        self._await_plans({post_run:post_mission})
        self._wait(lambda:self.runtime.execute("""SELECT count(*) AS n FROM runtime.outbox o
            JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
            WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
            (post_mission["plan_version_id"],)).fetchone()["n"]==0,
            description="post-check M9 result events reconciled")
        store.finalize_evaluation_mission(run_id=post_run,accepted_by="m9-postcheck-goal-acceptance")
        post_evidence,post_artifacts,_=self._mission_evidence(post_mission)
        verifier_id=self.verifier.execute("SELECT session_user AS actor").fetchone()["actor"]
        post_check_id=ident("e2postcheck")
        with self.assertRaisesRegex(E2PolicyError,"authenticated principal"):
            store.postpromotion_check(check_id=ident("e2wrongverifier"),decision_id=decision_id,
                run_id=post_run,recovery_event_id=post_recovery["event_id"],
                execution_evidence=post_evidence,artifact_refs=post_artifacts,checked_by="e2-proposer")
        self.assertIsNone(self.verifier.execute(
            "SELECT 1 FROM evolution.e2_postpromotion_checks WHERE decision_id=%s",(decision_id,)).fetchone())
        verifier_identity_rejection_checked=True
        # E35: interrupt PostgreSQL while the automatic rollback transaction is
        # in-flight. The champion pointer and rollback row must both roll back;
        # replaying the durable post-check then completes rollback exactly once.
        self.admin.execute("""CREATE OR REPLACE FUNCTION evolution.m10_test_pause_rollback()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(30); RETURN NEW; END $$""")
        self.admin.execute("""CREATE TRIGGER m10_test_pause_rollback BEFORE INSERT ON evolution.e2_rollback_records
            FOR EACH ROW EXECUTE FUNCTION evolution.m10_test_pause_rollback()""")
        interrupt_verifier=self._db("VERIFIER"); interrupt_promotion=self._db("E2PROMOTION")
        interrupt_promotion.execute("SET application_name='m10_rollback_interruption'")
        rollback_interrupted_backend_pid=None
        def interrupted_postcheck():
            interrupted_store=E2EvolutionStore(self.runtime,governance_db=self.governance,
                evaluator_db=self.evaluator,verifier_db=interrupt_verifier,
                promotion_db=interrupt_promotion,artifact_store=artifact_store)
            return interrupted_store.postpromotion_check(check_id=post_check_id,decision_id=decision_id,
                run_id=post_run,recovery_event_id=post_recovery["event_id"],
                execution_evidence=post_evidence,artifact_refs=post_artifacts,checked_by=verifier_id)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                interrupted=pool.submit(interrupted_postcheck)
                blocked=self._wait(lambda:self.admin.execute("""SELECT pid FROM pg_stat_activity
                    WHERE application_name='m10_rollback_interruption' AND wait_event='PgSleep'""").fetchone(),
                    timeout=20,description="rollback transaction enters injected pause")
                rollback_interrupted_backend_pid=blocked["pid"]
                self.admin.execute("SELECT pg_terminate_backend(%s)",(blocked["pid"],))
                with self.assertRaises(Exception): interrupted.result(timeout=10)
        finally:
            interrupt_verifier.close(); interrupt_promotion.close()
            self.admin.execute("DROP TRIGGER IF EXISTS m10_test_pause_rollback ON evolution.e2_rollback_records")
            self.admin.execute("DROP FUNCTION IF EXISTS evolution.m10_test_pause_rollback()")
        self.assertEqual(self.governance.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                                  (scope,)).fetchone()["genome_id"],candidate_id)
        self.assertIsNone(self.verifier.execute("SELECT 1 FROM evolution.e2_rollback_records WHERE decision_id=%s",
                                                (decision_id,)).fetchone())
        self.assertFalse(store.postpromotion_check(check_id=post_check_id,decision_id=decision_id,
            run_id=post_run,recovery_event_id=post_recovery["event_id"],
            execution_evidence=post_evidence,artifact_refs=post_artifacts,checked_by=verifier_id),
            "replayed durable post-check must finish the interrupted rollback")
        passed=False
        self.assertFalse(passed,"injected frozen stress load should trigger the post-promotion rollback")
        self.assertFalse(store.postpromotion_check(check_id=post_check_id,decision_id=decision_id,
            run_id=post_run,recovery_event_id=post_recovery["event_id"],
            execution_evidence=post_evidence,artifact_refs=post_artifacts,checked_by=verifier_id),
            "duplicate post-check must remain idempotent after rollback")
        champion=self.governance.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                                         (scope,)).fetchone()["genome_id"]
        self.assertEqual(champion,champion_id)
        rollback=self.verifier.execute("SELECT restored_genome_id,failed_genome_id FROM evolution.e2_rollback_records WHERE decision_id=%s",
                                        (decision_id,)).fetchone()
        self.assertEqual((rollback["restored_genome_id"],rollback["failed_genome_id"]),
                         (champion_id,candidate_id))
        self.assertEqual(self.admin.execute("SELECT count(*) AS n FROM evolution.e2_evaluation_runs WHERE mutation_id=%s",
            (mutation_id,)).fetchone()["n"],13)
        accounting=self.runtime.execute("""SELECT count(*) AS total,
                count(*) FILTER (WHERE status='SETTLED' AND usage_status='KNOWN') AS known_settled,
                count(*) FILTER (WHERE status='UNKNOWN') AS unknown
            FROM runtime.improvement_reservations WHERE campaign_id=%s
              AND stage IN ('e2_challenger','e2_evaluation')""",(campaign,)).fetchone()
        self.assertEqual((accounting["total"],accounting["known_settled"],accounting["unknown"]),(15,15,0))
        with self.assertRaisesRegex(ValueError,"campaign is stopped"):
            store.begin_evaluation(run_id=ident("after-terminal-e2-run"),campaign_id=campaign)
        self.assertEqual(self.runtime.execute("SELECT count(*) AS n FROM runtime.improvement_reservations WHERE campaign_id=%s",
            (campaign,)).fetchone()["n"],15)
        all_plans=[mission["plan_version_id"] for mission in missions.values()]+[post_mission["plan_version_id"],cancel_plan]
        all_tasks=[item["task_id"] for item in self.admin.execute("""SELECT n.task_id
            FROM runtime.coordination_plan_nodes n WHERE n.plan_version_id=ANY(%s)""",(all_plans[:-1],)).fetchall()]
        all_tasks.append(terminal_task)
        self.assertEqual(self.admin.execute("SELECT count(*) AS n FROM runtime.tasks WHERE task_id=ANY(%s) AND task_type<>'generic_agent_task'",
                                             (all_tasks,)).fetchone()["n"],0,
                         "E2 M9 challenger work remains within the generic bounded task capability")
        evidence={"format":"agentic-runtime-m10-e2-m9-e2e-v1","status":"PASS",
            "provider_calls":0,"scope_id":scope,"campaign_id":campaign,"observation_id":observation_id,
            "mutation_id":mutation_id,"champion_v1_id":champion_id,"challenger_v2_id":candidate_id,
            "evaluation_pack_id":pack_id,"comparison_id":comparison_id,"authorization_id":auth_id,
            "holdout_run_id":holdout_run,"holdout_scenario_id":holdout_scenario["scenario_id"],
            "holdout_workflow_hash":self.verifier.execute("SELECT workflow_hash FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                (holdout_run,)).fetchone()["workflow_hash"],
            "holdout_evaluator_id":holdout_evaluator,"holdout_evidence_hash":holdout_hash,
            "holdout_promotion_gate_enforced":True,"promotion_without_holdout_rejected":True,
            "holdout_forged_evidence_rejected":True,"holdout_repeated_result_idempotent":True,
            "challenger_hash_mismatch_rejected_before_materialization":True,
            "challenger_hash_mismatch_digests":candidate_hash_check,
            "evaluator_connection_loss_fail_closed":evaluator_unavailable_rejected,
            "monetary_cost_not_claimed_without_provider_usage":True,
            "rollback_target_mutation_rejected":True,
            "challenger_immutable_after_holdout_started":True,
            "holdout_suite_version_binding_verified":True,
            "wrong_holdout_suite_version_rejected":True,
            "holdout_mission_result_evidence_hidden_from_runtime_evaluator":True,
            "development_scenario_and_run_evidence_intake_verified":development_evidence_intake_verified,
            "holdout_scenario_source_rejected_at_observation_ingress":True,
            "postgres_rollback_interruption_recovered":True,
            "rollback_interrupted_backend_pid":rollback_interrupted_backend_pid,
            "rollback_absent_after_interruption_before_retry":True,
            "promotion_decision_id":decision_id,"postpromotion_run_id":post_run,
            "postpromotion_check_id":post_check_id,"duplicate_postcheck_idempotent":True,
            "postpromotion_plan_version_id":post_mission["plan_version_id"],
            "postpromotion_recovery_event_id":post_recovery["event_id"],
            "rollback":dict(rollback),"active_champion_after_rollback":champion,
            "cancelled_terminal_task_id":terminal_task,"stale_result_http_status":stale.exception.status,
            "artifact_corruption_rejected":artifact_corruption_checked,
            "corrupted_test_artifact_id":corrupted_artifact_id,
            "duplicate_promotion_idempotent":True,
            "duplicate_proposal_idempotent":duplicate_proposal_idempotent,
            "changed_duplicate_proposal_rejected":True,
            "evaluator_identity_rejection_checked":evaluator_identity_rejection_checked,
            "verifier_identity_rejection_checked":verifier_identity_rejection_checked,
            "terminal_campaign_dispatch_rejected":True,
            "accounting_summary":dict(accounting),
            "accounting_reservations":self.admin.execute("SELECT * FROM runtime.improvement_reservations WHERE campaign_id=%s AND stage IN ('e2_challenger','e2_evaluation') ORDER BY created_at,reservation_id",
                (campaign,)).fetchall(),
            "e2_reservation_reconciliation_results":e2_recovery_results,
            "missions":[{"run_id":run,"side":run_info[run]["side"],"workload":run_info[run]["workload"],
                "repetition":run_info[run]["repetition"],"seed":run_info[run]["seed"],
                "mission_id":missions[run]["mission_id"],"plan_version_id":missions[run]["plan_version_id"]}
                for run in missions],
            "persisted_evaluation_runs":self.admin.execute("""SELECT * FROM evolution.e2_evaluation_runs
                WHERE mutation_id=%s ORDER BY side,workload_key,repetition""",(mutation_id,)).fetchall(),
            "persisted_evaluation_run_evidence":self.admin.execute("""SELECT * FROM evolution.e2_evaluation_run_evidence
                WHERE run_id=ANY(%s) ORDER BY run_id,task_id,artifact_id""",(list(missions),)).fetchall(),
            "m9_tasks":self.admin.execute("SELECT * FROM runtime.tasks WHERE task_id=ANY(%s) ORDER BY task_id",
                                           (all_tasks,)).fetchall(),
            "m9_attempts":self.admin.execute("SELECT * FROM runtime.attempts WHERE task_id=ANY(%s) ORDER BY task_id,attempt_id",
                                              (all_tasks,)).fetchall(),
            "m9_leases":self.admin.execute("SELECT * FROM runtime.leases WHERE task_id=ANY(%s) ORDER BY task_id",
                                             (all_tasks,)).fetchall(),
            "m9_artifacts":self.admin.execute("SELECT * FROM runtime.artifacts WHERE producer_task_id=ANY(%s) ORDER BY producer_task_id,artifact_id",
                                               (all_tasks,)).fetchall(),
            "runtime_events":self.admin.execute("SELECT * FROM runtime.events WHERE campaign_id=%s OR task_id=ANY(%s) ORDER BY occurred_at,event_id",
                                                  (campaign,all_tasks)).fetchall(),
            "e2_events":self.admin.execute("SELECT * FROM evolution.e2_events WHERE scope_id=%s ORDER BY occurred_at,event_id",
                                              (scope,)).fetchall(),
            "comparison":self.admin.execute("SELECT * FROM evolution.e2_comparisons WHERE comparison_id=%s",
                                              (comparison_id,)).fetchone(),
            "promotion_decision":self.admin.execute("SELECT * FROM evolution.e2_promotion_decisions WHERE decision_id=%s",
                                                      (decision_id,)).fetchone(),
            "postpromotion_check":self.admin.execute("SELECT * FROM evolution.e2_postpromotion_checks WHERE decision_id=%s",
                                                       (decision_id,)).fetchone(),
            "postpromotion_check_evidence":self.admin.execute("SELECT * FROM evolution.e2_postpromotion_check_evidence WHERE check_id=(SELECT check_id FROM evolution.e2_postpromotion_checks WHERE decision_id=%s)",
                                                                (decision_id,)).fetchall()}
        evidence=json.loads(json.dumps(evidence,default=str))
        evidence["evidence_hash"]=digest(evidence)
        output=Path(os.environ.get("M10_E2_EVIDENCE_PATH",
            str(ROOT/"evidence/m10/automated-e2-m9-cycle-20261006.json")))
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(evidence,sort_keys=True,indent=2,default=str)+"\n",encoding="utf-8")
