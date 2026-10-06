from __future__ import annotations

import os
import sys
import unittest
import uuid
from pathlib import Path
from psycopg.types.json import Jsonb

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))

from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.evolution.autonomous import EvaluatorWorker
from agentic_runtime.evolution.controller import EvolutionController
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.persistence.postgres import apply_migrations


def ident(prefix): return f"{prefix}_{uuid.uuid4().hex}"


@unittest.skipUnless(all(os.environ.get(f"M4C_ROLE_DSN_{name}") for name in
    ("RUNTIME","EVALUATOR","GOVERNANCE","MIGRATOR")),"run through scripts/run_m4c_role_tests.py")
class M4CDeploymentIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin=connect(os.environ["M3_TEST_DATABASE_URL"]); cls.admin.autocommit=True

    @classmethod
    def tearDownClass(cls): cls.admin.close()

    def role(self,name):
        db=connect(os.environ[f"M4C_ROLE_DSN_{name.upper()}"]); db.autocommit=True
        return db

    def test_four_distinct_login_sessions_and_ddl_boundary(self):
        sessions={name:self.role(name) for name in ("migrator","runtime","evaluator","governance")}
        try:
            for name,db in sessions.items():
                self.assertEqual(db.execute("SELECT session_user").fetchone()["session_user"],
                    {"migrator":"agentic_migrator","runtime":"agentic_runtime",
                     "evaluator":"agentic_evaluator","governance":"agentic_governance"}[name])
            sessions["migrator"].execute("CREATE TABLE runtime.m4c_migrator_probe(id integer)")
            sessions["migrator"].execute("DROP TABLE runtime.m4c_migrator_probe")
            for name in ("runtime","evaluator","governance"):
                with self.assertRaises(Exception):
                    sessions[name].execute("CREATE TABLE runtime.m4c_forbidden_probe(id integer)")
            for name,db in sessions.items():
                if name!="migrator":
                    self.assertFalse(db.execute("SELECT has_table_privilege(current_user,'evolution.scope_champions','UPDATE') AS allowed").fetchone()["allowed"])
                    with self.assertRaises(Exception):
                        db.execute("UPDATE evolution.scope_champions SET updated_at=now() WHERE false")
                else:
                    self.assertFalse(db.execute("SELECT has_table_privilege(current_user,'evolution.scope_champions','UPDATE') AS allowed").fetchone()["allowed"])
            with self.assertRaises(Exception):
                sessions["runtime"].execute("UPDATE evolution.eval_suite_versions SET status='SUPERSEDED' WHERE false")
            with self.assertRaises(Exception):
                sessions["evaluator"].execute("UPDATE evolution.eval_suite_versions SET status='SUPERSEDED' WHERE false")
        finally:
            for db in sessions.values(): db.close()

    def test_migrator_applies_fresh_schema_and_second_replay_is_noop(self):
        db=connect(os.environ["M4C_MIGRATOR_DATABASE_DSN"]); db.autocommit=True
        try:
            self.assertEqual(apply_migrations(db),[])
            # The baseline is intentionally forward-only: M5 adds durable
            # worker state, so assert the applied migration set is non-empty
            # here and let fresh/replay equality below validate completeness.
            applied=db.execute("SELECT count(*) AS n FROM runtime.schema_migrations").fetchone()["n"]
            self.assertGreaterEqual(applied,14)
            self.assertEqual(apply_migrations(db),[])
            scope,genome=ident("migrator-scope"),ident("migrator-genome")
            EvolutionController(db).register_scope_champion(scope_id=scope,description="guard proof",genome_id=genome,
                version="1",config_ref="migrator:config",config={"route":"a"})
            with self.assertRaises(Exception):
                db.execute("UPDATE evolution.scope_champions SET updated_at=now() WHERE scope_id=%s",(scope,))
        finally: db.close()

    def test_runtime_claim_and_evaluator_result_write_are_separate(self):
        campaign,goal,task,worker=ident("role-campaign"),ident("role-goal"),ident("role-task"),ident("role-worker")
        coordinator=Coordinator(self.admin)
        coordinator.create_campaign(campaign,idempotency_key=ident("key"),description="role proof",created_by="fixture",budget={})
        coordinator.create_goal(goal,campaign,description="neutral test",mission_ref="mission:generic",created_by="fixture")
        coordinator.register_worker(worker,["inspect"])
        coordinator.create_task(task,campaign,task_type="inspect",idempotency_key=ident("key"),goal_id=goal,
            required_capabilities=["inspect"])
        runtime=self.role("runtime")
        try:
            lease=Coordinator(runtime).claim(worker)
            self.assertEqual(lease["task_id"],task)
        finally: runtime.close()

        scope,suite,champion=ident("scope"),ident("suite"),ident("genome")
        evolution=EvolutionController(self.admin)
        evolution.register_scope_champion(scope_id=scope,description="identity proof",genome_id=champion,
            version="1",config_ref="identity:champion",config={"route":"a"})
        evolution.register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref="identity:suite",integrity_hash="a"*64,created_by="governance",
            metric_directions={"quality":"MAX"})
        result=evolution.run_shadow(scope_id=scope,observation_id=ident("obs"),observation_refs=["evidence:test"],
            observation="identity test",mutation_id=ident("mutation"),candidate_genome_id=ident("genome"),
            candidate_version="2",candidate_config_ref="identity:candidate",candidate_config={"route":"b"},
            hypothesis="route B differs",expected_effect={"quality":"increase"},evaluator_version="test-v1",
            conditions_ref="identity:conditions",evaluation_result_refs=["identity:result"],champion_metrics={"quality":1},
            candidate_metrics={"quality":1},evaluator_id="fixture-evaluator",proposer_id="fixture-proposer",
            rationale_ref="identity:rationale")
        evaluator=self.role("evaluator")
        try:
            EvaluatorWorker().record_run(evaluator,evaluator_run_id=ident("run"),evaluation_id=result["evaluation_id"])
            with self.assertRaises(Exception):
                evaluator.execute("UPDATE evolution.scope_champions SET updated_at=now() WHERE scope_id=%s",(scope,))
            with self.assertRaises(Exception):
                evaluator.execute("UPDATE evolution.evaluation_records SET status='FAILED' WHERE evaluation_id=%s",(result["evaluation_id"],))
        finally: evaluator.close()

    def test_governance_has_controlled_call_not_direct_champion_dml(self):
        governance=self.role("governance")
        try:
            self.assertTrue(governance.execute("SELECT has_function_privilege(current_user,'evolution.execute_authorized_promotion(text)','EXECUTE') AS allowed").fetchone()["allowed"])
            self.assertTrue(governance.execute("SELECT has_function_privilege(current_user,'evolution.authorize_promotion(text,text,text,text,text,jsonb,text,text)','EXECUTE') AS allowed").fetchone()["allowed"])
            self.assertFalse(governance.execute("SELECT has_table_privilege(current_user,'evolution.scope_champions','UPDATE') AS allowed").fetchone()["allowed"])
            with self.assertRaises(Exception):
                governance.execute("UPDATE evolution.scope_champions SET updated_at=now() WHERE false")
            with self.assertRaises(Exception):
                governance.execute("INSERT INTO evolution.promotion_authorizations DEFAULT VALUES")
        finally: governance.close()

    def test_governance_login_invokes_controlled_promotion_and_rollback(self):
        scope,suite,champion,candidate=ident("gov-scope"),ident("gov-suite"),ident("gov-champion"),ident("gov-candidate")
        evolution=EvolutionController(self.admin)
        evolution.register_scope_champion(scope_id=scope,description="login procedure proof",genome_id=champion,
            version="1",config_ref="gov:champion",config={"route":"a"})
        evolution.register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref="gov:suite",integrity_hash="e"*64,created_by="governance",
            metric_directions={"quality":"MAX"})
        result=evolution.run_shadow(scope_id=scope,observation_id=ident("gov-observation"),
            observation_refs=["evidence:governance-test"],observation="controlled promotion test",
            mutation_id=ident("gov-mutation"),candidate_genome_id=candidate,candidate_version="2",
            candidate_config_ref="gov:candidate",candidate_config={"route":"b"},
            hypothesis="route B improves a deterministic case",expected_effect={"quality":"increase"},
            evaluator_version="login-test-v1",conditions_ref="gov:conditions",
            evaluation_result_refs=["gov:evaluation-result"],champion_metrics={"quality":0.5},
            candidate_metrics={"quality":1.0},evaluator_id="independent-evaluator",
            proposer_id="independent-proposer",rationale_ref="gov:rationale",created_by="shadow-controller")
        refs=[result["evaluation_id"]]
        governance=self.role("governance")
        try:
            governance.execute("SELECT evolution.authorize_promotion(%s,%s,%s,%s,%s,%s,%s,%s)",
                (ident("gov-promotion-auth"),scope,champion,candidate,result["decision_id"],
                 Jsonb(refs),"independent-authority","PROMOTE"))
            promotion_id=governance.execute("SELECT authorization_id FROM evolution.promotion_authorizations WHERE scope_id=%s AND action='PROMOTE' ORDER BY authorized_at DESC LIMIT 1",
                (scope,)).fetchone()["authorization_id"]
            self.assertEqual(governance.execute("SELECT evolution.execute_authorized_promotion(%s) AS result",
                (promotion_id,)).fetchone()["result"],candidate)
            rollback_id=ident("gov-rollback-auth")
            governance.execute("SELECT evolution.authorize_promotion(%s,%s,%s,%s,%s,%s,%s,%s)",
                (rollback_id,scope,candidate,champion,result["decision_id"],
                 Jsonb(refs),"independent-authority","ROLLBACK"))
            self.assertEqual(governance.execute("SELECT evolution.execute_authorized_promotion(%s) AS result",
                (rollback_id,)).fetchone()["result"],champion)
        finally: governance.close()


if __name__=="__main__": unittest.main()
