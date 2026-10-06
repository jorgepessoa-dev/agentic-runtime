from __future__ import annotations

import os
import hashlib
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from datetime import datetime, timezone
from pathlib import Path
from psycopg.types.json import Jsonb

from agentic_runtime.evolution.e1 import E1EvolutionService, postgres_json_hash, sha256
from agentic_runtime.persistence.postgres import connect, apply_migrations
from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.coordinator.state import TaskState
from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.adapters.fakes import FakeHarnessAdapter
from agentic_runtime.contracts.execution import ExecutionRequest, ExecutorClass


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


@unittest.skipUnless(os.environ.get("M3_TEST_DATABASE_URL"), "M7 needs disposable PostgreSQL")
class M7E1PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_db = connect(os.environ["M3_TEST_DATABASE_URL"])
        apply_migrations(cls.admin_db)

    @classmethod
    def tearDownClass(cls):
        cls.admin_db.close()

    def setUp(self):
        with self.admin_db.transaction():
            self.admin_db.execute("TRUNCATE runtime.campaigns,runtime.workers,runtime.executors CASCADE")
            self.admin_db.execute("TRUNCATE evolution.scopes CASCADE")
        self.db=connect(os.environ["M4C_ROLE_DSN_RUNTIME"])
        self.governance_db=connect(os.environ["M4C_ROLE_DSN_GOVERNANCE"])
        self.evaluator_db=connect(os.environ["M4C_ROLE_DSN_EVALUATOR"])
        promoter_dsn=os.environ.get("M4C_ROLE_DSN_E1PROMOTION")
        self.promoter_db=connect(promoter_dsn) if promoter_dsn else self.db
        self.service = E1EvolutionService(self.db,governance_db=self.governance_db,
            evaluator_db=self.evaluator_db,promotion_db=self.promoter_db)
        self.artifact_tmp = tempfile.TemporaryDirectory(prefix="m7-runtime-evidence-")
        self.artifacts = ArtifactStore(Path(self.artifact_tmp.name) / "artifacts")
        self.coordinator = Coordinator(self.db)
        self.scope = ident("e1scope")
        self.champion = ident("champion")
        self.campaign = ident("m7campaign")
        self.goal = ident("m7goal")
        self.task_class = "generic-structured-task"
        self.service.register_scope(scope_id=self.scope,description="generic routing E1 test scope",
            champion_id=self.champion,champion_config={"routing":{"preference":{"default":"route-baseline"}}},
            suite_id=ident("suite"),suite_version="1",suite_definition={"cases":["case-a","case-b"],"version":1},
            promotion_policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
                "required_improvement":0.50,"protected_quality_metric":"quality","maximum_cost_units":1e12},
            mode="ACTIVE_E1")
        self.service.create_campaign(campaign_id=self.campaign,goal_id=self.goal,scope_id=self.scope)
        data={"route-baseline": [1.0,1.0], "route-fast": [0.10,0.10],
              "route-modest": [0.20,0.20], "route-slower": [1.2,1.2]}
        self.create_accepted_measurements(data)
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.close()
        self.governance_db.close()
        self.evaluator_db.close()
        if self.promoter_db is not self.db:
            self.promoter_db.close()
        self.artifact_tmp.cleanup()

    def create_accepted_measurements(self, routes):
        for executor, samples in routes.items():
            self.db.execute("""INSERT INTO runtime.executors
                (executor_id,adapter_type,backend,version,configuration_ref,capabilities,location,enabled,metadata)
                VALUES (%s,'FAKE','deterministic-test','1','test://adapter',%s,'LOCAL',true,'{}'::jsonb)
                ON CONFLICT(executor_id) DO NOTHING""",(executor,'["structured_output"]'))
            for latency in samples:
                task, worker = ident("e1task"), ident("e1worker")
                self.coordinator.create_task(task,self.campaign,task_type=self.task_class,
                    idempotency_key=ident("e1task-key"),goal_id=self.goal,
                    required_capabilities=["structured_output"],output_contract={},
                    budget={"max_wall_time_seconds":15})
                self.db.commit()
                self.coordinator.register_worker(worker,["structured_output"])
                self.db.commit()
                lease=self.coordinator.claim(worker)
                self.db.commit()
                self.coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],
                    TaskState.CONTEXT_VALIDATED,actor_id=worker)
                self.db.commit()
                self.coordinator.transition(task,lease["attempt_id"],lease["lease_epoch"],
                    TaskState.RUNNING,actor_id=worker)
                self.db.commit()
                # E1 reads the coordinator's attempt clock, never worker text.
                time.sleep(latency)
                fake=FakeHarnessAdapter(executor, ("structured_output",), self.artifacts)
                output=(('{"ok":true,"task":"'+task+'"}').encode())
                request=ExecutionRequest(ident("exec"),task,lease["attempt_id"],"structured_output",
                    ExecutorClass.DETERMINISTIC_COMPUTE_JOB,executor,None,"/tmp/m7-test-workspace",
                    output_contract="json",idempotency_key=ident("exec-key"),metadata={"fake_output":output.decode()})
                result=fake.execute(request)
                manifest=self.artifacts.verify(result.artifact_refs[0])
                self.coordinator.register_artifact(task_id=task,attempt_id=lease["attempt_id"],
                    lease_epoch=lease["lease_epoch"],manifest=manifest,artifact_store=self.artifacts,
                    input_manifest_hash=hashlib.sha256(b"m7-fixed-neutral-input").hexdigest())
                self.coordinator.commit_result(task_id=task,attempt_id=lease["attempt_id"],
                    lease_epoch=lease["lease_epoch"],artifact_refs=list(result.artifact_refs),
                    result_hash=result.output_hash)
                self.coordinator.accept_verified_result(task_id=task,attempt_id=lease["attempt_id"],
                    lease_epoch=lease["lease_epoch"],artifact_store=self.artifacts)
                self.db.commit()
                model_run=ident("modelrun")
                now=datetime.now(timezone.utc)
                self.db.execute("""INSERT INTO runtime.model_runs
                    (model_run_id,task_id,attempt_id,executor_id,adapter_type,requested_model,resolved_model,
                     role,input_tokens,output_tokens,estimated_cost,latency_ms,status,schema_valid,
                     started_at,completed_at,telemetry)
                    VALUES (%s,%s,%s,%s,'DETERMINISTIC',NULL,NULL,'e1-evaluation-fixture',0,0,0,%s,'SUCCEEDED',true,%s,%s,%s)""",
                    (model_run,task,lease["attempt_id"],executor,None,now,now,Jsonb({"source":"fake deterministic execution"})))
                self.db.execute("UPDATE runtime.attempts SET model_run_id=%s WHERE attempt_id=%s",
                    (model_run,lease["attempt_id"]))
                self.service.record_measurement(measurement_id=ident("measurement"),scope_id=self.scope,
                    campaign_id=self.campaign,executor_id=executor,capability="structured_output",
                    task_id=task,attempt_id=lease["attempt_id"],model_run_id=model_run,
                    artifact_ref=result.artifact_refs[0],metadata={"source":"accepted deterministic runtime task"})

    def run_active_campaign(self):
        return self.service.run(campaign_id=self.campaign,scope_id=self.scope,task_class=self.task_class,
                                code_revision="test-revision")

    def test_autonomous_campaign_promotes_data_selected_e1_winner_and_retains_lineage(self):
        self.db.execute("UPDATE runtime.improvement_campaigns SET budget=budget || '{\"max_wall_time\":7200}'::jsonb WHERE campaign_id=%s",
                        (self.campaign,))
        result=self.run_active_campaign()
        self.assertEqual(result["champion_before"],self.champion)
        self.assertNotEqual(result["champion_after"],self.champion)
        self.assertEqual(len(result["challengers"]),3)
        self.assertEqual(result["decision"]["winner"],result["champion_after"])
        self.assertEqual(result["decision"]["mode"],"ACTIVE_E1")
        statuses=self.db.execute("SELECT genome_id,status FROM evolution.system_genomes WHERE genome_id=ANY(%s)",
                                 (result["challengers"],)).fetchall()
        self.assertCountEqual([r["status"] for r in statuses],["CHAMPION","RETAINED_FOR_DIVERSITY","REJECTED"],
            msg=f"persisted measurements: {result['measurement_summary']}; challenger rows: {statuses}")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM evolution.e1_promotion_decisions WHERE mode='ACTIVE_E1' AND decision='PROMOTED'").fetchone()["n"],1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM evolution.e1_evaluation_runs WHERE pack_id=%s AND reproducible",(result["pack_id"],)).fetchone()["n"],4)
        invalid_event_hashes=self.db.execute("""SELECT count(*) AS n FROM evolution.e1_events
            WHERE campaign_id=%s AND payload_hash<>encode(sha256(convert_to(payload::text,'UTF8')),'hex')""",
            (self.campaign,)).fetchone()["n"]
        self.assertEqual(invalid_event_hashes,0)
        self.assertEqual(self.db.execute("SELECT parent_genome_id FROM evolution.e1_genome_versions WHERE genome_id=%s",(result["champion_after"],)).fetchone()["parent_genome_id"],self.champion)
        reservation=self.db.execute("SELECT count(*) AS n FROM runtime.improvement_reservations WHERE campaign_id=%s AND status='RESERVED'",(self.campaign,)).fetchone()["n"]
        self.assertEqual(reservation,0)
        usage=self.db.execute("""SELECT sum(d.consumed) AS consumed,sum(d.released) AS released
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.campaign_id=%s AND d.dimension='experiment_units'""",(self.campaign,)).fetchone()
        self.assertEqual(float(usage["consumed"]),17.0)
        self.assertEqual(float(usage["released"]),0.0)
        wall=self.db.execute("""SELECT count(*) AS stages,sum(d.reserved) AS reserved,
            sum(d.consumed) AS consumed,sum(d.released) AS released,
            bool_and(r.status='SETTLED') AS settled
            FROM runtime.improvement_reservations r
            JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.campaign_id=%s AND d.dimension='wall_time' AND r.stage<>'task_attempt'""",(self.campaign,)).fetchone()
        self.assertGreater(wall["stages"],0)
        self.assertTrue(wall["settled"])
        self.assertLessEqual(float(wall["consumed"]),float(wall["reserved"]))
        self.assertGreater(float(wall["released"]),0)
        comparison_id=result["comparisons"][result["decision"]["winner"]]
        barrier=Barrier(2)
        def concurrent_duplicate_promotion():
            connection=connect(os.environ["M4C_ROLE_DSN_E1PROMOTION"])
            try:
                barrier.wait(timeout=5)
                with connection.transaction():
                    actor=connection.execute("SELECT session_user AS actor").fetchone()["actor"]
                    return connection.execute("SELECT evolution.authorize_and_execute_e1(%s,%s,%s) AS result",
                        (result["decision"]["authorization_id"],comparison_id,actor)).fetchone()["result"]
            finally:
                connection.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            concurrent_results=list(pool.map(lambda _: concurrent_duplicate_promotion(),range(2)))
        self.assertEqual(concurrent_results,[result["decision"]["decision_id"]]*2)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM evolution.system_genomes WHERE scope_id=%s AND status='CHAMPION'",
            (self.scope,)).fetchone()["n"],1)

        # A fresh independently evaluated clone of the same eligible mutation
        # still references the original champion. After the first promotion,
        # its authorization must become durably STALE without moving the pointer.
        source=self.db.execute("""SELECT g.*,v.canonical_config,v.parent_genome_id,v.changed_paths,v.provenance,
            c.champion_run_id,c.candidate_run_id,cr.pack_id,cr.evaluator_id,cr.evaluator_version,
            cr.metrics,cr.result_refs,cr.campaign_id,br.genome_id AS baseline_genome_id,
            br.started_at AS baseline_started,br.completed_at AS baseline_completed
            FROM evolution.e1_comparisons c
            JOIN evolution.system_genomes g ON g.genome_id=c.candidate_genome_id
            JOIN evolution.e1_genome_versions v ON v.genome_id=g.genome_id
            JOIN evolution.e1_evaluation_runs cr ON cr.run_id=c.candidate_run_id
            JOIN evolution.e1_evaluation_runs br ON br.run_id=c.champion_run_id
            WHERE c.comparison_id=%s""",(comparison_id,)).fetchone()
        stale_candidate=ident("stale-challenger")
        stale_run=ident("stale-eval")
        stale_comparison=ident("stale-comparison")
        now=datetime.now(timezone.utc)
        self.db.execute("""INSERT INTO evolution.system_genomes
            (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
             mutation_rationale,created_by,metadata)
            VALUES (%s,%s,%s,'CHALLENGER',%s,%s,'stale-baseline mutation clone',
             'exercise compare-and-swap freshness',%s,%s)""",
            (stale_candidate,self.scope,"stale-test-"+uuid.uuid4().hex,source["config_ref"],source["config_hash"],
             source["created_by"],Jsonb(dict(source["metadata"]))))
        self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",
            (stale_candidate,source["parent_genome_id"]))
        self.db.execute("""INSERT INTO evolution.e1_genome_versions
            (genome_id,scope_id,tier,parent_genome_id,canonical_config,config_hash,changed_paths,provenance)
            VALUES (%s,%s,'E1',%s,%s,%s,%s,%s)""",
            (stale_candidate,self.scope,source["parent_genome_id"],Jsonb(dict(source["canonical_config"])),
             source["config_hash"],Jsonb(list(source["changed_paths"])),Jsonb(dict(source["provenance"]))))
        # The independent evaluator connection must be able to resolve the
        # candidate FK before it records its deterministic replay result.
        self.db.commit()
        with self.evaluator_db.transaction():
            self.evaluator_db.execute("""INSERT INTO evolution.e1_evaluation_runs
                (run_id,scope_id,genome_id,baseline_genome_id,pack_id,campaign_id,evaluator_id,evaluator_version,
                 status,metrics,result_refs,reproducible,started_at,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETED',%s,%s,true,%s,%s)""",
                (stale_run,self.scope,stale_candidate,source["baseline_genome_id"],source["pack_id"],
                 source["campaign_id"],source["evaluator_id"],source["evaluator_version"],
                 Jsonb(dict(source["metrics"])),Jsonb(list(source["result_refs"])),now,now))
        self.db.execute("""INSERT INTO evolution.e1_comparisons
            (comparison_id,scope_id,campaign_id,candidate_genome_id,champion_genome_id,candidate_run_id,
             champion_run_id,eligible,disposition,rationale)
            VALUES (%s,%s,%s,%s,%s,%s,%s,true,'ELIGIBLE',%s)""",
            (stale_comparison,self.scope,source["campaign_id"],stale_candidate,source["parent_genome_id"],
             stale_run,source["champion_run_id"],Jsonb({"test":"stale champion baseline CAS"})))
        self.db.commit()
        stale_auth=ident("stale-auth")
        with self.promoter_db.transaction():
            actor=self.promoter_db.execute("SELECT session_user AS actor").fetchone()["actor"]
            stale=self.promoter_db.execute("SELECT evolution.authorize_and_execute_e1(%s,%s,%s) AS result",
                (stale_auth,stale_comparison,actor)).fetchone()["result"]
        self.assertEqual(stale,"STALE")
        self.assertEqual(self.db.execute("SELECT status FROM evolution.e1_promotion_authorizations WHERE authorization_id=%s",
            (stale_auth,)).fetchone()["status"],"STALE")
        self.assertEqual(self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
            (self.scope,)).fetchone()["genome_id"],result["champion_after"])

    def test_deterministic_postpromotion_failure_rolls_back_and_duplicate_is_idempotent(self):
        result=self.run_active_campaign()
        decision_id=result["decision"]["decision_id"]
        check_id=ident("postcheck")
        restored=self.service.postpromotion_check(check_id=check_id,decision_id=decision_id,
            scope_id=self.scope,passed=False,failure_class="PROTECTED_QUALITY_REGRESSION",
            metrics={"quality":0.80},evidence_hash=sha256({"quality":0.80}))
        self.assertEqual(restored,self.champion)
        self.assertEqual(self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(self.scope,)).fetchone()["genome_id"],self.champion)
        again=self.service.postpromotion_check(check_id=ident("postcheck"),decision_id=decision_id,
            scope_id=self.scope,passed=False,failure_class="PROTECTED_QUALITY_REGRESSION",
            metrics={"quality":0.80},evidence_hash=sha256({"quality":0.80}))
        self.assertEqual(again,self.champion)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM evolution.e1_rollback_records WHERE decision_id=%s",(decision_id,)).fetchone()["n"],1)
        with self.assertRaises(Exception):
            self.promoter_db.execute("SELECT evolution.rollback_e1_on_failed_check(%s,%s)",(check_id,"forged-actor"))
        self.promoter_db.rollback()
        invalid_event_hashes=self.db.execute("""SELECT count(*) AS n FROM evolution.e1_events
            WHERE scope_id=%s AND payload_hash<>encode(sha256(convert_to(payload::text,'UTF8')),'hex')""",
            (self.scope,)).fetchone()["n"]
        self.assertEqual(invalid_event_hashes,0)

    def test_shadow_mode_cannot_move_champion_and_policy_cannot_be_overridden_by_campaign(self):
        shadow_scope=ident("shadow-scope")
        shadow_champion=ident("shadow-champion")
        shadow_campaign=ident("shadow-campaign")
        shadow_goal=ident("shadow-goal")
        shadow_service=E1EvolutionService(self.db,governance_db=self.governance_db,
            evaluator_db=self.evaluator_db,promotion_db=self.promoter_db)
        shadow_service.register_scope(scope_id=shadow_scope,description="generic shadow-only E1 scope",
            champion_id=shadow_champion,champion_config={"routing":{"preference":{"default":"route-baseline"}}},
            suite_id=ident("shadow-suite"),suite_version="1",suite_definition={"cases":["case-a","case-b"]},
            promotion_policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
                "required_improvement":0.50,"protected_quality_metric":"quality","maximum_cost_units":1e12},mode="SHADOW")
        with self.assertRaises(ValueError):
            self.service.create_campaign(campaign_id=ident("badcampaign"),goal_id=ident("badgoal"),scope_id=shadow_scope,mode="ACTIVE_E1")
        self.service.create_campaign(campaign_id=shadow_campaign,goal_id=shadow_goal,scope_id=shadow_scope)
        # Recreate accepted, artifact-backed runtime evidence for the shadow scope.
        old_scope,old_campaign,old_goal=self.scope,self.campaign,self.goal
        self.scope,self.campaign,self.goal=shadow_scope,shadow_campaign,shadow_goal
        self.create_accepted_measurements({"route-baseline":[1.0,1.0],"route-fast":[0.10,0.10],
            "route-modest":[0.20,0.20],"route-slower":[1.2,1.2]})
        self.scope,self.campaign,self.goal=old_scope,old_campaign,old_goal
        self.db.commit()
        result=self.service.run(campaign_id=shadow_campaign,scope_id=shadow_scope,task_class=self.task_class,code_revision="shadow-test")
        self.assertEqual(result["decision"]["decision_id"],"WOULD_PROMOTE")
        self.assertEqual(result["champion_after"],shadow_champion)
        self.assertEqual(self.db.execute("SELECT status FROM evolution.system_genomes WHERE genome_id=%s",(shadow_champion,)).fetchone()["status"],"CHAMPION")

    def test_policy_and_frozen_evaluation_inputs_are_immutable(self):
        with self.assertRaises(Exception):
            self.db.execute("UPDATE evolution.e1_scope_policies SET mode='SHADOW' WHERE scope_id=%s",(self.scope,))
        self.db.rollback()
        result=self.run_active_campaign()
        with self.assertRaises(Exception):
            self.db.execute("UPDATE evolution.e1_evaluation_pack_versions SET definition='{}'::jsonb WHERE pack_id=%s",(result["pack_id"],))
        self.db.rollback()
        pack=self.db.execute("SELECT definition_hash FROM evolution.e1_evaluation_pack_versions WHERE pack_id=%s",(result["pack_id"],)).fetchone()
        self.assertEqual(len(pack["definition_hash"]),64)

    def test_interrupted_campaign_restart_reconciliation_stops_without_inventing_decision(self):
        task=ident("interrupted-e1-task")
        self.coordinator.create_task(task,self.campaign,task_type=self.task_class,
            idempotency_key=ident("interrupted-e1-key"),goal_id=self.goal,
            required_capabilities=["structured_output"],output_contract={},budget={})
        self.db.execute("UPDATE runtime.improvement_campaigns SET status='EVALUATING' WHERE campaign_id=%s",
                        (self.campaign,))
        self.db.commit()
        recovered=self.service.reconcile_incomplete_campaigns()
        self.assertEqual(recovered["campaigns_stopped"],1)
        row=self.admin_db.execute("SELECT status,stop_reason FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                            (self.campaign,)).fetchone()
        self.assertEqual(row["status"],"STOPPED")
        self.assertEqual(row["stop_reason"],"PROCESS_RESTART_INCOMPLETE_CAMPAIGN")
        runtime_campaign=self.admin_db.execute("SELECT status FROM runtime.campaigns WHERE campaign_id=%s",
                            (self.campaign,)).fetchone()
        self.assertEqual(runtime_campaign["status"],"CANCELLED")
        self.assertEqual(self.admin_db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",
                            (task,)).fetchone()["status"],"CANCELLED")
        self.assertEqual(self.admin_db.execute("""SELECT count(*) AS n FROM runtime.events
            WHERE campaign_id=%s AND task_id=%s AND event_type='TASK_CANCELLED'""",
                            (self.campaign,task)).fetchone()["n"],1)
        self.assertEqual(self.admin_db.execute("SELECT count(*) AS n FROM evolution.e1_promotion_decisions WHERE campaign_id=%s",
                            (self.campaign,)).fetchone()["n"],0)
        # This is a separate connection to prove recovery was committed, not
        # merely visible in the worker connection's transaction. Close the
        # read transaction so the shared fixture connection cannot hold locks
        # across later test setup/truncation.
        self.admin_db.commit()
        self.db.execute("UPDATE runtime.campaigns SET status='ACTIVE' WHERE campaign_id=%s",(self.campaign,))
        self.db.commit()
        repaired=self.service.reconcile_incomplete_campaigns()
        self.assertEqual(repaired["campaigns_stopped"],0)
        self.assertEqual(repaired["campaign_dispatch_states_repaired"],1)
        self.assertEqual(repaired["dispatchable_tasks_cancelled"],0)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.campaigns WHERE campaign_id=%s",
                            (self.campaign,)).fetchone()["status"],"CANCELLED")

    def test_completed_improvement_campaign_cannot_dispatch_more_tasks(self):
        task=ident("stopped-e1-task")
        self.coordinator.create_task(task,self.campaign,task_type=self.task_class,
            idempotency_key=ident("stopped-e1-key"),goal_id=self.goal,
            required_capabilities=["structured_output"],output_contract={},budget={})
        worker=ident("stopped-e1-worker")
        self.coordinator.register_worker(worker,["structured_output"])
        self.db.execute("UPDATE runtime.campaigns SET status='CANCELLED' WHERE campaign_id=%s",(self.campaign,))
        self.db.commit()
        self.assertIsNone(self.coordinator.claim(worker))
        status=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["status"]
        self.assertEqual(status,"QUEUED")

    def test_non_e1_tier_and_out_of_scope_mutation_registration_are_rejected(self):
        with self.assertRaises(Exception):
            self.db.execute("""INSERT INTO evolution.e1_genome_versions
                (genome_id,scope_id,tier,parent_genome_id,canonical_config,config_hash,changed_paths,provenance)
                VALUES (%s,%s,'E3',%s,'{}'::jsonb,%s,'[]'::jsonb,'{}'::jsonb)""",
                (ident("forbidden-tier"),self.scope,self.champion,"0"*64))
        self.db.rollback()
        with self.assertRaises(ValueError):
            self.service.register_scope(scope_id=ident("forbidden-scope"),description="invalid tier scope",
                champion_id=ident("forbidden-champion"),champion_config={"routing":{}},suite_id=ident("suite"),
                suite_version="1",suite_definition={"cases":[]},allowed_paths=("governance.*",))
        with self.assertRaises(Exception):
            self.service.register_scope(scope_id=ident("secret-scope"),description="credential rejection proof",
                champion_id=ident("secret-champion"),champion_config={
                    "routing":{"preference":{"default":"route-safe"}},"provider":{"api_key":"not-a-real-key"}},
                suite_id=ident("secret-suite"),suite_version="1",suite_definition={"cases":[]})

    def test_candidate_cannot_hide_an_unallowlisted_config_delta(self):
        candidate=ident("hidden-delta")
        config={"routing":{"preference":{"default":"route-fast"}},
                "security":{"allow_network":True}}
        digest=postgres_json_hash(self.db,config)
        self.admin_db.execute("""INSERT INTO evolution.system_genomes
            (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
             mutation_rationale,created_by,metadata)
            VALUES (%s,%s,'hidden-delta','CHALLENGER',%s,%s,'test','test','test-proposer','{}'::jsonb)""",
            (candidate,self.scope,f"sha256:{digest}",digest))
        self.admin_db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",
                              (candidate,self.champion))
        self.admin_db.commit()
        with self.assertRaises(Exception):
            self.db.execute("""INSERT INTO evolution.e1_genome_versions
                (genome_id,scope_id,tier,parent_genome_id,canonical_config,config_hash,changed_paths,provenance)
                VALUES (%s,%s,'E1',%s,%s,%s,%s,%s)""",
                (candidate,self.scope,self.champion,Jsonb(config),digest,
                 Jsonb(["routing.preference.default"]),Jsonb({"created_by":"test-proposer"})))
        self.db.rollback()

    def test_unknown_providerless_worker_cost_is_not_coerced_to_zero(self):
        row=self.db.execute("""SELECT m.model_run_id,m.task_id,m.attempt_id,m.executor_id
            FROM runtime.model_runs m JOIN runtime.artifacts a ON a.producer_task_id=m.task_id
              AND a.producer_attempt_id=m.attempt_id
            WHERE m.task_id IN (SELECT task_id FROM runtime.tasks WHERE campaign_id=%s)
            ORDER BY m.completed_at LIMIT 1""",(self.campaign,)).fetchone()
        self.db.execute("""UPDATE runtime.model_runs SET adapter_type='remote_worker',provider=NULL,
            requested_model=NULL,resolved_model=NULL,estimated_cost=NULL WHERE model_run_id=%s""",
            (row["model_run_id"],))
        artifact=self.db.execute("SELECT artifact_id FROM runtime.artifacts WHERE producer_task_id=%s AND producer_attempt_id=%s LIMIT 1",
            (row["task_id"],row["attempt_id"])).fetchone()["artifact_id"]
        measurement=ident("deterministic-cost")
        self.service.record_measurement(measurement_id=measurement,scope_id=self.scope,campaign_id=self.campaign,
            executor_id=row["executor_id"],capability="structured_output",task_id=row["task_id"],
            attempt_id=row["attempt_id"],model_run_id=row["model_run_id"],artifact_ref=artifact,
            metadata={"source":"providerless remote worker; usage unknown"})
        cost=self.db.execute("SELECT cost_units FROM evolution.e1_executor_measurements WHERE measurement_id=%s",
            (measurement,)).fetchone()["cost_units"]
        self.assertIsNone(cost)

    @unittest.skipUnless(all(os.environ.get(f"M4C_ROLE_DSN_{name}") for name in ("RUNTIME","EVALUATOR","E1PROMOTION")),
                         "run through separate M4C login identities")
    def test_m7_promotion_function_is_not_available_to_runtime_or_evaluator(self):
        for name in ("RUNTIME","EVALUATOR"):
            conn=connect(os.environ[f"M4C_ROLE_DSN_{name}"])
            try:
                with self.assertRaises(Exception):
                    conn.execute("SELECT evolution.authorize_and_execute_e1('missing','missing','attacker')")
                conn.rollback()
                with self.assertRaises(Exception):
                    conn.execute("UPDATE evolution.scope_champions SET genome_id=genome_id WHERE scope_id=%s",(self.scope,))
                conn.rollback()
            finally:
                conn.close()
        promoter=connect(os.environ["M4C_ROLE_DSN_E1PROMOTION"])
        try:
            with self.assertRaises(Exception):
                promoter.execute("UPDATE evolution.scope_champions SET genome_id=genome_id WHERE scope_id=%s",(self.scope,))
            promoter.rollback()
            with self.assertRaises(Exception):
                promoter.execute("SET ROLE agentic_promotion_executor")
            promoter.rollback()
        finally:
            promoter.close()


    def test_model_proposal_requires_durable_cognitive_artifact_provenance(self):
        provenance = {
            route: {"invocation_id": f"inv-{route}", "raw_hash": "a" * 64,
                    "normalized_hash": "b" * 64, "proposal_hash": "c" * 64}
            for route in ("route-fast", "route-modest", "route-slower")
        }
        with self.assertRaisesRegex(ValueError,"model proposal provenance must bind invocation and output hashes"):
            self.service.run(campaign_id=self.campaign, scope_id=self.scope,
                task_class=self.task_class, code_revision="model-provenance-test",
                challenger_limit=3, model_provenance_by_executor=provenance)

    def test_model_proposal_cannot_invent_unmeasured_executor(self):
        provenance = {
            route: {"invocation_id": f"inv-{route}", "raw_hash": "a" * 64,
                    "normalized_hash": "b" * 64, "proposal_hash": "c" * 64}
            for route in ("route-fast", "route-modest", "unmeasured")
        }
        with self.assertRaisesRegex(ValueError, "without verified measurements"):
            self.service.run(campaign_id=self.campaign, scope_id=self.scope,
                task_class=self.task_class, challenger_limit=3,
                model_provenance_by_executor=provenance)


if __name__ == "__main__":
    unittest.main()
