from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import subprocess
import shutil
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.coordinator.state import TaskState
from agentic_runtime.evolution.autonomous import (
    AutonomousImprovementCampaign, AutonomousMutationPolicy, CampaignLimits,
    ChallengerGenerator, EvaluationPackService, EvaluatorWorker,
    EvalSuiteArtifactResolver, HumanPromotionService, HypothesisGenerator,
    ImprovementCampaignService, OpportunityDiscoveryService, StagnationDetector,
)
from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.evolution.controller import EvolutionController
from agentic_runtime.persistence.postgres import apply_migrations, connect
from agentic_runtime.persistence.outbox import OutboxDispatcher
from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.artifacts.gc import ArtifactGarbageCollector
from agentic_runtime.adapters.sandbox import GitWorktreeSandboxAdapter, SandboxCleanupReconciler
from agentic_runtime.evolution.evaluator import canonical_json


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


_SANDBOX_GIT_ROOT: Path | None = None


def sandbox_git_root() -> Path:
    """Use the checkout when available, or create an isolated git fixture.

    Public export candidates intentionally have no .git history, while these
    tests exercise GitWorktreeSandboxAdapter. Give the adapter a disposable
    repository without adding Git metadata to the candidate itself.
    """
    global _SANDBOX_GIT_ROOT
    if _SANDBOX_GIT_ROOT is not None:
        return _SANDBOX_GIT_ROOT
    existing = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--show-toplevel"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if existing.returncode == 0:
        _SANDBOX_GIT_ROOT = ROOT
        return ROOT

    parent = Path(tempfile.mkdtemp(prefix="m4-git-fixture-"))
    repository = parent / "repository"
    shutil.copytree(ROOT, repository, ignore=shutil.ignore_patterns(".venv", "__pycache__", ".pytest_cache"))
    subprocess.run(["git", "-C", str(repository), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repository), "config", "user.name", "Runtime Test"], check=True)
    subprocess.run(["git", "-C", str(repository), "config", "user.email", "runtime-test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repository), "commit", "-qm", "sandbox test fixture"], check=True)
    _SANDBOX_GIT_ROOT = repository
    return repository


def db_connect():
    db = connect(os.environ["M3_TEST_DATABASE_URL"])
    db.autocommit = True
    role = os.environ.get("M3_TEST_DATABASE_ROLE")
    if role:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", role):
            raise ValueError("unsafe test role identifier")
        db.execute(f'SET ROLE "{role}"')
    return db


def role_connect(role: str):
    dsn=os.environ.get(f"M4C_ROLE_DSN_{role.upper()}")
    if dsn:
        db=connect(dsn); db.autocommit=True
        return db
    db=connect(os.environ["M3_TEST_DATABASE_URL"])
    db.autocommit=True
    db.execute("SET ROLE agentic_runtime_test")
    db.execute(f"SET ROLE agentic_runtime_{role}")
    return db


def resource_sample():
    own_rss=0
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"): own_rss=int(line.split()[1]); break
    pg_rss=0
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit(): continue
        try:
            comm=(proc/"comm").read_text().strip()
            if comm.startswith("postgres"):
                pg_rss+=int((proc/"status").read_text().split("VmRSS:",1)[1].splitlines()[0].split()[0])
        except (OSError,IndexError,ValueError): pass
    mem={}
    for line in Path("/proc/meminfo").read_text().splitlines():
        bits=line.split()
        if len(bits)>1 and bits[0].rstrip(":") in {"MemAvailable","SwapTotal","SwapFree"}:
            mem[bits[0].rstrip(":")]=int(bits[1])
    return {"runtime_process_rss_kib":own_rss,"postgres_process_rss_kib":pg_rss,
        "mem_available_kib":mem.get("MemAvailable"),
        "swap_used_kib":mem.get("SwapTotal",0)-mem.get("SwapFree",0),
        "load_average":os.getloadavg(),
        "memory_psi":Path("/proc/pressure/memory").read_text().splitlines(),
        "io_psi":Path("/proc/pressure/io").read_text().splitlines()}


class M4AutonomousPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = db_connect()
        apply_migrations(cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def setUp(self):
        with self.db.transaction():
            self.db.execute("TRUNCATE runtime.campaigns, runtime.workers, runtime.executors CASCADE")
            self.db.execute("TRUNCATE evolution.scopes CASCADE")

    def campaign_goal(self):
        co = Coordinator(self.db)
        campaign, goal = ident("camp"), ident("goal")
        co.create_campaign(campaign, idempotency_key=ident("key"), description="neutral campaign",
                           created_by="m4-test", budget={"max_cost": 100})
        co.create_goal(goal, campaign, description="Reduce repeated generic task failures",
                       mission_ref="mission:generic", created_by="m4-test")
        return co, campaign, goal

    def test_evidence_autonomously_generates_deduplicated_opportunity_and_falsifiable_hypothesis(self):
        co, campaign, goal = self.campaign_goal()
        worker = ident("worker")
        co.register_worker(worker, ["inspect"])
        task_ids = []
        for _ in range(3):
            task = ident("task")
            task_ids.append(task)
            co.create_task(task, campaign, task_type="inspect", idempotency_key=ident("task-key"),
                           goal_id=goal, required_capabilities=["inspect"])
            lease = co.claim(worker)
            self.assertIsNotNone(lease)
            co.transition(task, lease["attempt_id"], lease["lease_epoch"], TaskState.RUNNING, actor_id=worker)
            co.transition(task, lease["attempt_id"], lease["lease_epoch"], TaskState.FAILED_PERMANENT, actor_id=worker)

        service = OpportunityDiscoveryService(self.db)
        first = service.scan_task_outcomes(scope_id="routing", goal_id=goal)
        self.assertEqual(len(first), 1)
        second = service.scan_task_outcomes(scope_id="routing", goal_id=goal)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0].opportunity_id, second[0].opportunity_id)
        rationale = service.prioritize(first[0])
        self.assertTrue(rationale)

        hypothesis = HypothesisGenerator().generate(first[0],
            evidence_summary={"task_type": "inspect"}, contradicting_refs=("eval:counterexample",))
        HypothesisGenerator().persist(self.db, hypothesis)
        relations = self.db.execute("SELECT relation FROM runtime.hypothesis_evidence WHERE hypothesis_id=%s ORDER BY relation",
                                    (hypothesis.hypothesis_id,)).fetchall()
        self.assertEqual([row["relation"] for row in relations], ["CONTRADICTS", "SUPPORTS"])
        self.assertIn("Falsify", hypothesis.falsification_criteria)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.opportunities WHERE fingerprint=%s",
            (first[0].fingerprint,)).fetchone()["n"], 1)

    def test_allowlist_and_frozen_evaluation_pack(self):
        evolution = EvolutionController(self.db)
        scope, champion, suite = ident("scope"), ident("genome"), ident("suite")
        evolution.register_scope_champion(scope_id=scope, description="neutral routing policy",
            genome_id=champion, version="1", config_ref="artifact://champion",
            config={"routing": {"preference": {"generic": "executor-a"}}})
        evolution.register_eval_suite(eval_suite_id=suite, version="1", scope_id=scope,
            definition_ref="artifact://suite", integrity_hash="a" * 64,
            created_by="governance", metric_directions={"quality": "MAX"})
        policy = AutonomousMutationPolicy(self.db)
        patch = {"routing.preference.generic": "executor-b"}
        self.assertTrue(policy.authorize(path="routing.preference.generic", tier="E1"))
        policy.validate_patch(tier="E1", changes=patch)
        with self.assertRaisesRegex(ValueError, "forbidden"):
            policy.validate_patch(tier="E1", changes={"mission.charter": "changed"})
        with self.assertRaisesRegex(ValueError, "forbidden"):
            policy.validate_patch(tier="E1", changes={"runtime_code.coordinator": "changed"})
        candidate_config = ChallengerGenerator(self.db).minimal_candidate(tier="E1",
            config={"routing": {"preference": {"generic": "executor-a"}}}, changes=patch)
        candidate = ident("genome")
        digest = hashlib.sha256(json.dumps(candidate_config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.db.execute("""INSERT INTO evolution.system_genomes
            (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,mutation_rationale,created_by)
            VALUES (%s,%s,'2','CHALLENGER','artifact://candidate',%s,'routing preference','test minimal mutation','autonomous')""",
            (candidate,scope,digest))
        self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)", (candidate,champion))
        pack_id, pack_hash = EvaluationPackService(self.db).freeze(scope_id=scope,candidate_id=candidate,
            champion_id=champion,suite_id=suite,suite_version="1",definition={"cases":["case-1"],"code_revision":"abc"},created_by="controller")
        pack = self.db.execute("SELECT pack_hash,definition FROM evolution.evaluation_packs WHERE pack_id=%s", (pack_id,)).fetchone()
        self.assertEqual(pack["pack_hash"],pack_hash)
        self.assertEqual(pack["definition"]["eval_suite_version"],"1")
        with self.assertRaises(Exception):
            with self.db.transaction():
                self.db.execute("UPDATE evolution.evaluation_packs SET pack_hash=%s WHERE pack_id=%s", ("b"*64,pack_id))

    def test_human_authorized_promotion_cas_and_transactional_rollback(self):
        evolution = EvolutionController(self.db)
        scope, champion, suite = ident("scope"), ident("genome"), ident("suite")
        evolution.register_scope_champion(scope_id=scope, description="generic config",
            genome_id=champion, version="1", config_ref="artifact://champion", config={"route":"a"})
        evolution.register_eval_suite(eval_suite_id=suite, version="1", scope_id=scope,
            definition_ref="artifact://suite", integrity_hash="b"*64, created_by="governance",
            metric_directions={"quality":"MAX"})
        result=evolution.run_shadow(scope_id=scope,observation_id=ident("obs"),observation_refs=["evidence:1"],
            observation="Observed neutral improvement opportunity",mutation_id=ident("mutation"),
            candidate_genome_id=ident("genome"),candidate_version="2",candidate_config_ref="artifact://challenger",
            candidate_config={"route":"b"},hypothesis="Alternative route improves quality",
            expected_effect={"quality":"increase"},evaluator_version="deterministic-v1",
            conditions_ref="artifact://conditions",evaluation_result_refs=["artifact://eval"],
            champion_metrics={"quality":0.7},candidate_metrics={"quality":0.9},evaluator_id="eval-worker",
            proposer_id="opportunity-scout",rationale_ref="artifact://rationale")
        governance_db=role_connect("governance")
        service=HumanPromotionService(governance_db)
        service.authorize(authorization_id=ident("auth"),scope_id=scope,expected_champion_id=champion,
            challenger_id=result["candidate_genome_id"],shadow_decision_id=result["decision_id"],
            evaluation_refs=[result["evaluation_id"]],authorized_by="human-governance")
        with self.assertRaisesRegex(ValueError,"distinct from proposer"):
            service.authorize(authorization_id=ident("proposer-auth"),scope_id=scope,
                expected_champion_id=champion,challenger_id=result["candidate_genome_id"],
                shadow_decision_id=result["decision_id"],evaluation_refs=[result["evaluation_id"]],
                authorized_by="opportunity-scout")
        auth=self.db.execute("SELECT authorization_id FROM evolution.promotion_authorizations WHERE scope_id=%s",(scope,)).fetchone()["authorization_id"]
        stale_id=ident("stale-auth")
        service.authorize(authorization_id=stale_id,scope_id=scope,expected_champion_id=champion,
            challenger_id=result["candidate_genome_id"],shadow_decision_id=result["decision_id"],
            evaluation_refs=[result["evaluation_id"]],authorized_by="human-governance")
        self.assertEqual(service.execute(auth),result["candidate_genome_id"])
        active=self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(scope,)).fetchone()["genome_id"]
        self.assertEqual(active,result["candidate_genome_id"])
        # A stale CAS is retained as STALE and never changes the pointer.
        with self.assertRaisesRegex(ValueError,"stale authorization"):
            service.execute(stale_id)
        self.assertEqual(self.db.execute("SELECT status FROM evolution.promotion_authorizations WHERE authorization_id=%s",(stale_id,)).fetchone()["status"],"STALE")
        # Reuse the original positive shadow record as the evidentiary basis for rollback.
        service.authorize_rollback(authorization_id=ident("rollback"),scope_id=scope,
            expected_champion_id=result["candidate_genome_id"],rollback_target_id=champion,
            shadow_decision_id=result["decision_id"],authorized_by="human-governance")
        rollback_auth=self.db.execute("SELECT authorization_id FROM evolution.promotion_authorizations WHERE action='ROLLBACK' AND scope_id=%s",(scope,)).fetchone()["authorization_id"]
        self.assertEqual(service.execute(rollback_auth),champion)
        self.assertEqual(self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(scope,)).fetchone()["genome_id"],champion)
        governance_db.close()

    def test_autonomous_campaign_discovers_routes_and_records_shadow_lineage(self):
        co, runtime_campaign, goal = self.campaign_goal()
        # No failures are seeded: this demonstrates exploration with a healthy champion.
        for executor in ("executor-general","executor-specialized"):
            self.db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,configuration_ref,capabilities,location)
                VALUES (%s,'fake','fake','1','test-config',%s,'LOCAL')""",(executor,Jsonb(["structured_output"])))

        definition=json.loads((ROOT/"tests/fixtures/routing-policy-eval-v1.json").read_text(encoding="utf-8"))
        suite_bytes=canonical_json(definition)
        suite_hash=hashlib.sha256(suite_bytes).hexdigest()
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4b-eval-suite-")))
        suite_artifact=store.put(suite_bytes,kind="evaluation_suite",producer_execution_id="m4-test",producer_attempt_id="m4-test")
        scope,suite,champion=ident("scope"),ident("suite"),ident("genome")
        config={"routes":{"structured_output":"executor-general","code_review":"executor-general","data_transform":"executor-general"}}
        EvolutionController(self.db).register_scope_champion(scope_id=scope,description="generic route config",
            genome_id=champion,version="1",config_ref="artifact://champion",config=config)
        EvolutionController(self.db).register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref=suite_artifact.artifact_id,integrity_hash=suite_hash,
            created_by="governance",metric_directions=definition["metric_directions"])
        campaign_id=ident("improve")
        ImprovementCampaignService(self.db).create(campaign_id=campaign_id,scope_id=scope,goal_id=goal,
            budget={"max_experiment_units":2},created_by="test",
            exploration={"experiment_units":2},
            limits=CampaignLimits(max_challengers_per_hypothesis=2))
        runtime_db=role_connect("runtime")
        evaluator_db=role_connect("evaluator")
        try:
            result=AutonomousImprovementCampaign(runtime_db,store,evaluator_db=evaluator_db).run_routing_once(campaign_id=campaign_id,
                scope_id=scope,goal_id=goal,champion_config=config,code_revision="test-revision")
        finally:
            runtime_db.close(); evaluator_db.close()
        active=self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(scope,)).fetchone()["genome_id"]
        self.assertEqual(active,champion)
        self.assertEqual(result.get("mode"),"SHADOW",result)
        self.assertEqual(result["decision"],"WOULD_PROMOTE")
        self.assertEqual(result["budget_class"],"EXPLORE")
        self.assertIn("routing executor changed",result["material_difference"])
        self.assertLess(result["candidate_metrics"]["cost"],result["champion_metrics"]["cost"])
        chain=self.db.execute("""SELECT p.definition->>'candidate_genome_id' AS candidate,
            e.eval_suite_version,d.mode,d.decision FROM evolution.evaluation_packs p
            JOIN evolution.evaluation_records e ON e.candidate_genome_id=p.candidate_genome_id
            JOIN evolution.promotion_decisions d ON d.candidate_genome_id=p.candidate_genome_id
            WHERE p.pack_id=%s""",(result["evaluation_pack_id"],)).fetchone()
        self.assertEqual(chain["candidate"],result["candidate_genome_id"])
        self.assertEqual((chain["eval_suite_version"],chain["mode"],chain["decision"]),("1","SHADOW","WOULD_PROMOTE"))
        self.assertEqual(self.db.execute("SELECT status FROM runtime.improvement_campaigns WHERE campaign_id=%s",(campaign_id,)).fetchone()["status"],"COMPLETED")
        accounting_rows=self.db.execute("""SELECT stage,status,dispatch_state FROM runtime.improvement_reservations
            WHERE campaign_id=%s ORDER BY stage""",(campaign_id,)).fetchall()
        self.assertEqual({r["stage"] for r in accounting_rows},{"opportunity_investigation","hypothesis_generation","challenger_creation","challenger_evaluation"})
        self.assertTrue(all((r["status"],r["dispatch_state"])==("SETTLED","TERMINAL") for r in accounting_rows))
        experiment_accounting=CampaignAccounting(self.db).snapshot(campaign_id)["experiment_units"]
        self.assertEqual(experiment_accounting,{"limit":2,"reserved":0.0,"consumed":1.0,
            "released":0.0,"remaining":1.0,"usage_status":"KNOWN"})
        repeat_campaign=ident("repeat")
        ImprovementCampaignService(self.db).create(campaign_id=repeat_campaign,scope_id=scope,goal_id=goal,
            budget={"max_experiment_units":2},created_by="test",exploration={"experiment_units":2})
        runtime_db=role_connect("runtime"); evaluator_db=role_connect("evaluator")
        try:
            with self.assertRaisesRegex(ValueError,"duplicate mutation"):
                AutonomousImprovementCampaign(runtime_db,store,evaluator_db=evaluator_db).run_routing_once(
                    campaign_id=repeat_campaign,scope_id=scope,goal_id=goal,
                    champion_config=config,code_revision="test-revision")
        finally:
            runtime_db.close(); evaluator_db.close()
        self.assertEqual(self.db.execute("SELECT stop_reason FROM runtime.improvement_campaigns WHERE campaign_id=%s",
            (repeat_campaign,)).fetchone()["stop_reason"],"DUPLICATE_LOOP_DETECTED")
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.improvement_budget_ledger WHERE campaign_id=%s",
            (repeat_campaign,)).fetchone()["n"],0)

    def test_campaign_reservations_enforce_class_quota_and_terminal_stop(self):
        _, _, goal = self.campaign_goal()
        campaign_id=ident("bounded-campaign")
        scope=ident("scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'bounded budget test')",(scope,))
        service=ImprovementCampaignService(self.db)
        service.create(campaign_id=campaign_id,scope_id=scope,goal_id=goal,
            budget={"max_experiment_units":2},created_by="test",
            exploitation={"experiment_units":1},exploration={"experiment_units":1},
            limits=CampaignLimits())
        service.reserve(campaign_id=campaign_id,budget_class="EXPLOIT",
            dimensions={"experiment_units":1,"challengers":1},idempotency_key="exploit-one")
        with self.assertRaisesRegex(ValueError,"BUDGET_EXHAUSTED"):
            service.reserve(campaign_id=campaign_id,budget_class="EXPLOIT",
                dimensions={"experiment_units":1},idempotency_key="exploit-two")
        self.assertEqual(self.db.execute("SELECT status,stop_reason FROM runtime.improvement_campaigns WHERE campaign_id=%s",
            (campaign_id,)).fetchone(),{"status":"STOPPED","stop_reason":"BUDGET_EXHAUSTED"})
        with self.assertRaisesRegex(ValueError,"stopped"):
            service.reserve(campaign_id=campaign_id,budget_class="EXPLORE",
                dimensions={"experiment_units":1},idempotency_key="after-stop")

    def test_stagnation_is_persisted_and_prioritized_as_exploration(self):
        co,_,goal=self.campaign_goal()
        scope,champion,suite=ident("scope"),ident("genome"),ident("suite")
        evolution=EvolutionController(self.db)
        evolution.register_scope_champion(scope_id=scope,description="plateau test",
            genome_id=champion,version="1",config_ref="config:champion",config={"route":"a"})
        evolution.register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref="suite:fixed",integrity_hash="c"*64,created_by="governance",
            metric_directions={"quality":"MAX"})
        improvement_campaign=ident("improve")
        ImprovementCampaignService(self.db).create(campaign_id=improvement_campaign,scope_id=scope,
            goal_id=goal,budget={"max_experiment_units":2},created_by="test",
            exploration={"experiment_units":2})
        for index in range(3):
            candidate=ident("genome")
            self.db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,mutation_rationale,created_by)
                VALUES (%s,%s,%s,'CHALLENGER',%s,%s,'no-op','plateau fixture','test')""",
                (candidate,scope,f"c{index}",f"candidate:{candidate}","d"*64))
            self.db.execute("""INSERT INTO evolution.evaluation_records
                (evaluation_id,scope_id,candidate_genome_id,champion_genome_id,eval_suite_id,eval_suite_version,
                 conditions_ref,evaluator_version,evaluator_id,metrics,status,completed_at)
                VALUES (%s,%s,%s,%s,%s,'1','conditions','v1','deterministic',%s,'COMPLETED',now())""",
                (ident("eval"),scope,candidate,champion,suite,Jsonb({"candidate":{"quality":1},"champion":{"quality":1}})))
        found=StagnationDetector(self.db).detect_and_persist(scope_id=scope,goal_id=goal,evaluation_count=3)
        self.assertIsNotNone(found)
        self.assertEqual(found["opportunity"].kind,"STAGNATION")
        OpportunityDiscoveryService(self.db).prioritize(found["opportunity"],campaign_id=improvement_campaign)
        decision=self.db.execute("SELECT budget_class FROM runtime.opportunity_decisions WHERE opportunity_id=%s",
            (found["opportunity"].opportunity_id,)).fetchone()
        self.assertEqual(decision["budget_class"],"EXPLORE")
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM evolution.observations WHERE observation_id=%s",
            (found["observation_id"],)).fetchone())

    def test_artifact_gc_requires_explicit_temporary_class_and_sweeps_after_grace(self):
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4b-gc-test-")))
        temporary=store.put(b"temporary evidence candidate"+os.urandom(12),kind="scratch",producer_execution_id="gc-test",
            producer_attempt_id="gc-test",metadata={"retention_class":"TEMPORARY"})
        gc=ArtifactGarbageCollector(self.db,store)
        self.assertEqual(gc.mark(temporary.artifact_id,grace_seconds=0),"MARKED")
        self.assertEqual(gc.sweep(temporary.artifact_id),"SWEPT")
        self.assertFalse(store.path_for(temporary.artifact_id).exists())
        unclassified=store.put(b"do not delete",kind="scratch",producer_execution_id="gc-test",
            producer_attempt_id="gc-test")
        with self.assertRaisesRegex(ValueError,"retention class"):
            gc.mark(unclassified.artifact_id,grace_seconds=0)
        self.assertTrue(store.path_for(unclassified.artifact_id).exists())
        during_grace=store.put(b"becomes referenced in grace",kind="working",producer_execution_id="gc-test",
            producer_attempt_id="gc-test",metadata={"retention_class":"TEMPORARY"})
        self.assertEqual(gc.mark(during_grace.artifact_id,grace_seconds=3600),"MARKED")
        self.db.execute("""INSERT INTO runtime.knowledge_objects
            (knowledge_id,kind,content_ref,created_by,status)
            VALUES (%s,'EVIDENCE',%s,'gc-test','ACTIVE')""",(ident("knowledge"),during_grace.artifact_id))
        self.assertEqual(gc.sweep(during_grace.artifact_id),"RETAINED")
        self.assertTrue(store.path_for(during_grace.artifact_id).exists())

    def test_runtime_and_evaluator_roles_cannot_mutate_champion_or_evaluations(self):
        scope,champion=ident("scope"),ident("genome")
        EvolutionController(self.db).register_scope_champion(scope_id=scope,description="role test",
            genome_id=champion,version="1",config_ref="role:champion",config={"route":"a"})
        runtime=role_connect("runtime"); evaluator=role_connect("evaluator")
        try:
            with self.assertRaises(Exception):
                runtime.execute("CREATE TABLE runtime.forbidden_probe(id int)")
            with self.assertRaises(Exception):
                runtime.execute("UPDATE evolution.scope_champions SET genome_id=%s WHERE scope_id=%s",(champion,scope))
            with self.assertRaises(Exception):
                evaluator.execute("UPDATE evolution.evaluation_records SET status='FAILED' WHERE false")
            with self.assertRaises(Exception):
                evaluator.execute("UPDATE evolution.scope_champions SET genome_id=%s WHERE scope_id=%s",(champion,scope))
        finally:
            runtime.close(); evaluator.close()

    def test_trusted_eval_suite_resolver_quarantines_corrupt_artifact(self):
        scope,suite=ident("scope"),ident("suite")
        EvolutionController(self.db).register_scope_champion(scope_id=scope,description="resolver test",
            genome_id=ident("genome"),version="1",config_ref="resolver:champion",config={"route":"a"})
        definition={"cases":[{"input":"neutral","expected":"ok"}]}
        content=canonical_json(definition)
        digest=hashlib.sha256(content).hexdigest()
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4b-tamper-test-")))
        manifest=store.put(content,kind="evaluation_suite",producer_execution_id="test",producer_attempt_id="test")
        EvolutionController(self.db).register_eval_suite(eval_suite_id=suite,version="1",scope_id=scope,
            definition_ref=manifest.artifact_id,integrity_hash=digest,created_by="governance",
            metric_directions={"quality":"MAX"})
        (store.path_for(manifest.artifact_id)/"content").write_bytes(b"tampered evaluator content")
        evaluator=role_connect("evaluator")
        try:
            with self.assertRaisesRegex(ValueError,"evaluation rejected"):
                EvalSuiteArtifactResolver(self.db,store,evaluator).resolve(suite,"1")
            self.assertEqual(evaluator.execute("SELECT count(*) AS n FROM evolution.eval_artifact_quarantine WHERE eval_suite_id=%s",(suite,)).fetchone()["n"],1)
        finally:
            evaluator.close()

    def test_terminal_attempt_sandbox_is_reconciled_and_audited(self):
        co,campaign,goal=self.campaign_goal()
        worker,task=ident("worker"),ident("task")
        co.register_worker(worker,["inspect"])
        co.create_task(task,campaign,task_type="inspect",idempotency_key=ident("task-key"),
            goal_id=goal,required_capabilities=["inspect"])
        lease=co.claim(worker)
        co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        workspace_root=Path(tempfile.mkdtemp(prefix="m4b-sandbox-cleanup-")).resolve()
        repository = sandbox_git_root()
        adapter=GitWorktreeSandboxAdapter(repository,workspace_root)
        revision=subprocess.check_output(["git","-C",str(repository),"rev-parse","HEAD"],text=True).strip()
        sandbox=adapter.create(task,lease["attempt_id"],revision)
        self.db.execute("""INSERT INTO runtime.sandboxes
            (sandbox_id,task_id,attempt_id,implementation,base_revision,workspace_ref,status,cleanup_status,policy_ref)
            VALUES (%s,%s,%s,'git-worktree',%s,%s,'ACTIVE','ACTIVE','test-policy')""",
            (sandbox.sandbox_id,task,lease["attempt_id"],sandbox.base_revision,str(sandbox.workspace_path)))
        self.db.execute("UPDATE runtime.attempts SET sandbox_id=%s WHERE attempt_id=%s",(sandbox.sandbox_id,lease["attempt_id"]))
        co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.FAILED_PERMANENT,actor_id=worker)
        original_destroy=adapter.destroy
        calls={"n":0}
        def fail_once(record):
            calls["n"]+=1
            if calls["n"]==1: raise OSError("injected cleanup interruption")
            return original_destroy(record)
        adapter.destroy=fail_once
        reconciler=SandboxCleanupReconciler(self.db,adapter)
        first=reconciler.reconcile()
        self.assertEqual(first["failed"],1)
        self.assertEqual(self.db.execute("SELECT cleanup_status FROM runtime.sandboxes WHERE sandbox_id=%s",(sandbox.sandbox_id,)).fetchone()["cleanup_status"],"FAILED")
        result=reconciler.reconcile()
        self.assertEqual(result["destroyed"],1)
        row=self.db.execute("SELECT status,cleanup_status FROM runtime.sandboxes WHERE sandbox_id=%s",(sandbox.sandbox_id,)).fetchone()
        self.assertEqual((row["status"],row["cleanup_status"]),("DESTROYED","DESTROYED"))
        self.assertFalse(sandbox.workspace_path.exists())
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.sandbox_cleanup_attempts WHERE sandbox_id=%s",(sandbox.sandbox_id,)).fetchone()["n"],2)

    def test_accelerated_recovery_soak_has_no_stale_leases_or_orphans(self):
        co,campaign,goal=self.campaign_goal()
        soak_campaigns=[(campaign,goal)]
        for _ in range(2):
            extra_campaign,extra_goal=ident("soak-campaign"),ident("soak-goal")
            co.create_campaign(extra_campaign,idempotency_key=ident("soak-campaign-key"),description="soak campaign",created_by="m4-soak",budget={})
            co.create_goal(extra_goal,extra_campaign,description="neutral recovery workload",mission_ref="mission:generic",created_by="m4-soak")
            soak_campaigns.append((extra_campaign,extra_goal))
        worker=ident("soak-worker")
        co.register_worker(worker,["transform"])
        injected=0; attempts=0; soak_started=time.monotonic(); soak_samples=[]
        sandbox_root=Path(tempfile.mkdtemp(prefix="m4c-soak-worktrees-"))
        repository = sandbox_git_root()
        sandbox_adapter=GitWorktreeSandboxAdapter(repository,sandbox_root)
        base_revision=subprocess.run(["git","rev-parse","HEAD"],cwd=repository,check=True,text=True,
            stdout=subprocess.PIPE).stdout.strip()
        sandboxes=[]
        artifact_root=Path(tempfile.mkdtemp(prefix="m4c-soak-artifacts-"))
        artifact_store=ArtifactStore(artifact_root)
        temporary_artifacts=[]
        for index in range(120):
            if index%3==0: soak_samples.append(resource_sample())
            task_campaign,task_goal=soak_campaigns[index//40]
            task=ident("soak-task")
            co.create_task(task,task_campaign,task_type="transform",idempotency_key=ident("soak-key"),
                goal_id=task_goal,required_capabilities=["transform"])
            lease=co.claim(worker); attempts+=1
            co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
            if index % 10 == 0:
                injected+=1
                self.db.execute("UPDATE runtime.leases SET lease_until=now()-interval '1 second' WHERE task_id=%s",(task,))
                report=co.reconcile_runtime(scan_id=ident("soak-reconcile"))
                self.assertIn(task,report["expired_leases_recovered"])
                lease=co.claim(worker); attempts+=1
                co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
            if index in {0,40,80}:
                sandbox=sandbox_adapter.create(task,lease["attempt_id"],base_revision)
                sandbox_adapter.prepare(sandbox,{"network":"disabled","test":"soak"})
                self.db.execute("""INSERT INTO runtime.sandboxes(sandbox_id,task_id,attempt_id,implementation,
                    base_revision,workspace_ref,status,cleanup_status,policy_ref)
                    VALUES (%s,%s,%s,'git-worktree',%s,%s,'ACTIVE','PENDING','policy:soak')""",
                    (sandbox.sandbox_id,task,lease["attempt_id"],base_revision,str(sandbox.workspace_path)))
                sandboxes.append(sandbox)
            if index in {0,40,80}:
                temporary_artifacts.append(artifact_store.put(f"uncommitted-neutral-result-{index}-{uuid.uuid4().hex}".encode(),
                    kind="temporary",producer_execution_id="m4c-soak",producer_attempt_id=lease["attempt_id"],
                    metadata={"retention_class":"TEMPORARY"}))
            co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.FAILED_PERMANENT,actor_id=worker)
        dispatch_failure={"injected":False,"count":0}
        def dispatch(key,payload):
            if not dispatch_failure["injected"]:
                dispatch_failure["injected"]=True
                raise RuntimeError("injected dispatcher interruption")
            dispatch_failure["count"]+=1
            return f"effect:{key}"
        dispatcher=OutboxDispatcher(self.db,dispatch)
        dispatched=0
        with self.assertRaisesRegex(RuntimeError,"injected dispatcher interruption"):
            dispatcher.dispatch_one("soak-dispatcher")
        self.db.execute("UPDATE runtime.outbox SET available_at=now() WHERE last_error='RuntimeError'")
        while dispatcher.dispatch_one("soak-dispatcher"):
            dispatched+=1
        cleanup_calls={"failed":False}
        original_destroy=sandbox_adapter.destroy
        def fail_cleanup_once(record):
            if not cleanup_calls["failed"]:
                cleanup_calls["failed"]=True
                raise OSError("injected sandbox cleanup interruption")
            return original_destroy(record)
        sandbox_adapter.destroy=fail_cleanup_once
        cleanup=SandboxCleanupReconciler(self.db,sandbox_adapter)
        cleanup_first=cleanup.reconcile()
        cleanup_second=cleanup.reconcile()
        self.assertEqual(cleanup_first["failed"],1)
        self.assertEqual(cleanup_second["destroyed"],1)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.sandboxes WHERE status<>'DESTROYED'",()).fetchone()["n"],0)
        gc=ArtifactGarbageCollector(self.db,artifact_store)
        for manifest in temporary_artifacts:
            self.assertEqual(gc.mark(manifest.artifact_id,grace_seconds=0),"MARKED")
            self.assertEqual(gc.sweep(manifest.artifact_id),"SWEPT")

        # Three independent improvement campaigns each derive their opportunity
        # from the just-persisted failure history and create a challenger/evaluation.
        for executor in ("executor-general","executor-specialized"):
            self.db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,
                configuration_ref,capabilities,location) VALUES (%s,'fake','fake','1','soak',%s,'LOCAL')
                ON CONFLICT(executor_id) DO NOTHING""",(executor,Jsonb(["transform"])))
        suite_definition=json.loads((ROOT/"tests/fixtures/routing-policy-eval-v1.json").read_text(encoding="utf-8"))
        suite_bytes=canonical_json(suite_definition)
        suite_hash=hashlib.sha256(suite_bytes).hexdigest()
        suite_artifact=artifact_store.put(suite_bytes,kind="evaluation_suite",producer_execution_id="m4c-soak",
            producer_attempt_id="m4c-soak",metadata={"retention_class":"GOVERNANCE"})
        evaluation_ids=[]
        improvement_ids=[]
        for task_campaign,task_goal in soak_campaigns:
            scope,suite_id,champion=ident("soak-scope"),ident("soak-suite"),ident("soak-champion")
            config={"routes":{"structured_output":"executor-general","code_review":"executor-general",
                "data_transform":"executor-general","transform":"executor-general"}}
            EvolutionController(self.db).register_scope_champion(scope_id=scope,description="soak routing scope",
                genome_id=champion,version="1",config_ref="soak:champion",config=config)
            EvolutionController(self.db).register_eval_suite(eval_suite_id=suite_id,version="1",scope_id=scope,
                definition_ref=suite_artifact.artifact_id,integrity_hash=suite_hash,created_by="soak",
                metric_directions=suite_definition["metric_directions"])
            improve_id=ident("soak-improvement")
            improvement_ids.append(improve_id)
            ImprovementCampaignService(self.db).create(campaign_id=improve_id,scope_id=scope,goal_id=task_goal,
                budget={"max_experiment_units":2},created_by="soak",exploration={"experiment_units":2})
            result=AutonomousImprovementCampaign(self.db,artifact_store).run_routing_once(campaign_id=improve_id,
                scope_id=scope,goal_id=task_goal,champion_config=config,code_revision=base_revision)
            self.assertEqual(result["mode"],"SHADOW")
            self.assertEqual(self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",
                (scope,)).fetchone()["genome_id"],champion)
            evaluation_ids.append(result["evaluation_id"])
        self.assertEqual(len(set(evaluation_ids)),3)
        sandbox_root.rmdir()
        shutil.rmtree(artifact_root,ignore_errors=True)
        final=co.reconcile_runtime(scan_id=ident("soak-final"))
        self.assertEqual(final["tasks_without_current_lease"],0)
        self.assertEqual(final["orphaned_attempts"],0)
        self.assertEqual(final["pending_outbox"],0)
        self.assertEqual(final["incomplete_sandbox_cleanup"],0)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.leases WHERE status='ACTIVE' AND lease_until<=now()").fetchone()["n"],0)
        self.assertEqual(attempts,132)
        self.assertEqual(injected,12)
        self.assertEqual(dispatched,120)
        self.assertTrue(dispatch_failure["injected"])
        self.assertEqual(dispatch_failure["count"],120)
        self.assertEqual(len(sandboxes),3)
        self.assertEqual(len(temporary_artifacts),3)
        self.assertEqual(self.db.execute("SELECT count(DISTINCT campaign_id) AS n FROM runtime.tasks WHERE task_type='transform' AND created_at >= now()-interval '10 minutes'").fetchone()["n"],3)
        final_invariants={
            "expired_active_leases":self.db.execute("SELECT count(*) AS n FROM runtime.leases WHERE status='ACTIVE' AND lease_until<=now()").fetchone()["n"],
            "unreconciled_running_attempts":self.db.execute("""SELECT count(*) AS n FROM runtime.attempts a
                LEFT JOIN runtime.leases l ON l.attempt_id=a.attempt_id AND l.lease_epoch=a.lease_epoch AND l.status='ACTIVE'
                WHERE a.status IN ('RUNNING','LEASED') AND l.task_id IS NULL""").fetchone()["n"],
            "stuck_outbox_records":self.db.execute("SELECT count(*) AS n FROM runtime.outbox WHERE delivered_at IS NULL").fetchone()["n"],
            "incomplete_sandboxes":self.db.execute("SELECT count(*) AS n FROM runtime.sandboxes WHERE status<>'DESTROYED' OR cleanup_status NOT IN ('DESTROYED','NOT_STARTED')").fetchone()["n"],
            "active_campaign_reservations":self.db.execute("SELECT count(*) AS n FROM runtime.improvement_reservations WHERE campaign_id=ANY(%s) AND status='RESERVED'",(improvement_ids,)).fetchone()["n"],
            "campaign_budget_overruns":self.db.execute("""SELECT count(*) AS n FROM runtime.improvement_reservation_dimensions d
                JOIN runtime.improvement_reservations r USING(reservation_id)
                WHERE r.campaign_id=ANY(%s) AND d.reserved IS NOT NULL AND d.consumed>d.reserved""",(improvement_ids,)).fetchone()["n"],
            "corrupt_accepted_artifacts":self.db.execute("""SELECT count(*) AS n FROM runtime.tasks t
                WHERE t.status='ACCEPTED' AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(t.result_refs) x(ref)
                    LEFT JOIN runtime.artifacts a ON a.artifact_id=x.ref
                    WHERE a.artifact_id IS NULL OR a.verification_status<>'VERIFIED' OR
                        a.producer_task_id<>t.task_id OR a.producer_attempt_id<>(SELECT l.attempt_id FROM runtime.leases l WHERE l.task_id=t.task_id))""").fetchone()["n"],
            "evaluation_records_without_frozen_pack":self.db.execute("""SELECT count(*) AS n FROM evolution.evaluation_records e
                WHERE e.conditions_ref LIKE 'evaluation-pack:%' AND NOT EXISTS(SELECT 1 FROM evolution.evaluation_packs p
                    WHERE e.conditions_ref='evaluation-pack:'||p.pack_id||':'||p.pack_hash)""").fetchone()["n"],
            "invalid_champion_pointers":self.db.execute("""SELECT count(*) AS n FROM evolution.scope_champions c
                JOIN evolution.system_genomes g ON g.genome_id=c.genome_id
                WHERE g.status<>'CHAMPION' OR g.scope_id<>c.scope_id""").fetchone()["n"],
        }
        self.assertEqual(final_invariants,{key:0 for key in final_invariants})
        soak_samples.append(resource_sample())
        soak_doc={"tasks":120,"attempts":attempts,"lease_expiry_injections":injected,
            "campaigns":3,"challengers":3,"evaluations":3,"sandboxes":len(sandboxes),
            "temporary_artifacts_written_and_gc_swept":len(temporary_artifacts),
            "dispatcher_failures":1,"dispatcher_recoveries":1,
            "cleanup_failures":cleanup_first["failed"],"cleanup_recoveries":cleanup_second["destroyed"],
            "final_invariants":final_invariants,
            "duration_seconds":time.monotonic()-soak_started,"samples":len(soak_samples),
            "runtime_peak_rss_kib":max(x["runtime_process_rss_kib"] for x in soak_samples),
            "postgres_peak_rss_kib":max(x["postgres_process_rss_kib"] for x in soak_samples),
            "mem_available_min_kib":min(x["mem_available_kib"] for x in soak_samples if x["mem_available_kib"] is not None),
            "swap_used_max_kib":max(x["swap_used_kib"] for x in soak_samples),
            "load_average_max":max(max(x["load_average"]) for x in soak_samples),
            "memory_psi_last":soak_samples[-1]["memory_psi"],"io_psi_last":soak_samples[-1]["io_psi"],
            "limitations":["sampled every third task and at completion","runtime RSS is the unittest process RSS","PostgreSQL RSS sums visible PostgreSQL processes"]}
        (Path(tempfile.gettempdir())/"agentic-runtime-test-soak-resource-measurements.json").write_text(json.dumps(soak_doc,indent=2)+"\n")

    def _accounting_campaign(self, budget=None):
        scope=ident("account-scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'accounting test')",(scope,))
        return ImprovementCampaignService(self.db).create(campaign_id=ident("account-campaign"),scope_id=scope,
            goal_id=None,budget=budget or {},created_by="accounting-test",
            exploration={"experiment_units":100},exploitation={"experiment_units":100})

    def test_m4c_campaign_accounting_settlement_unknown_and_reconciliation(self):
        service=CampaignAccounting(self.db)
        campaign=self._accounting_campaign({"max_cost":10,"max_tokens_input":100})
        reservation=service.reserve(campaign_id=campaign,stage="evaluation",idempotency_key="a1",
            amounts={"experiment_units":1,"monetary_cost":3,"tokens_input":50})
        service.mark_dispatched(reservation)
        self.assertEqual(service.settle(reservation,actual={"experiment_units":1,"monetary_cost":2,"tokens_input":30},terminal_status="SUCCEEDED"),"SETTLED")
        row=self.db.execute("SELECT dimension,consumed,released FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s ORDER BY dimension",(reservation,)).fetchall()
        values={r["dimension"]:(float(r["consumed"]),float(r["released"])) for r in row}
        self.assertEqual(values["monetary_cost"],(2.0,1.0))
        self.assertEqual(values["tokens_input"],(30.0,20.0))
        self.assertEqual(service.settle(reservation,actual={"monetary_cost":0},terminal_status="SUCCEEDED"),"SETTLED")

        with self.assertRaisesRegex(ValueError,"hard campaign limit"):
            service.reserve(campaign_id=campaign,stage="provider",idempotency_key="unknown-cost",
                amounts={"tokens_input":0,"tokens_output":0})
        bounded=service.reserve(campaign_id=campaign,stage="provider",idempotency_key="bounded-unknown",
            amounts={"monetary_cost":4,"tokens_input":20},usage_bounded=True)
        service.mark_dispatched(bounded)
        self.assertEqual(service.settle(bounded,actual={"monetary_cost":None,"tokens_input":None},terminal_status="FAILED"),"UNKNOWN")
        unknown=self.db.execute("SELECT consumed,unknown FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s AND dimension='monetary_cost'",(bounded,)).fetchone()
        self.assertEqual((float(unknown["consumed"]),unknown["unknown"]),(4.0,True))

        never=service.reserve(campaign_id=campaign,stage="hypothesis",idempotency_key="never-dispatched",
            amounts={"hypotheses":1,"monetary_cost":0,"tokens_input":0,"tokens_output":0})
        self.assertEqual(Coordinator(self.db).reconcile_runtime(scan_id=ident("undispatched-recovery"))[
            "campaign_reservations"]["released_undispatched"],1)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.improvement_reservations WHERE reservation_id=%s",(never,)).fetchone()["status"],"RELEASED")
        self.assertEqual(Coordinator(self.db).reconcile_runtime(scan_id=ident("idempotent-recovery"))[
            "campaign_reservations"],{"released_undispatched":0,"settled_unknown":0})
        negative=service.reserve(campaign_id=campaign,stage="negative-test",idempotency_key="negative-usage",
            amounts={"monetary_cost":1,"tokens_input":0})
        service.mark_dispatched(negative)
        with self.assertRaisesRegex(ValueError,"finite and nonnegative"):
            service.settle(negative,actual={"monetary_cost":-1,"tokens_input":0},terminal_status="SUCCEEDED")
        service.settle(negative,actual={"monetary_cost":0,"tokens_input":0},terminal_status="FAILED")

    def test_m4c_hard_provider_usage_requires_caps_before_task_dispatch(self):
        from psycopg.types.json import Jsonb
        co,runtime_campaign,goal=self.campaign_goal()
        scope=ident("usage-scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'usage cap test')",(scope,))
        improvement=ident("usage-campaign")
        ImprovementCampaignService(self.db).create(campaign_id=improvement,scope_id=scope,goal_id=goal,
            budget={"max_cost":10},created_by="test")
        worker=ident("usage-worker")
        co.register_worker(worker,["inspect"])
        unbounded=ident("unbounded")
        co.create_task(unbounded,runtime_campaign,task_type="inspect",idempotency_key=ident("unbounded-key"),
            goal_id=goal,required_capabilities=["inspect"])
        with self.assertRaisesRegex(ValueError,"unknown monetary_cost"):
            co.claim(worker)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(unbounded,)).fetchone()["status"],"QUEUED")
        task=ident("bounded")
        co.create_task(task,runtime_campaign,task_type="inspect",idempotency_key=ident("bounded-key"),
            goal_id=goal,budget={"max_cost":5,"max_tokens_input":100,"max_tokens_output":100,
                "max_wall_time_seconds":10},required_capabilities=["inspect"],priority=10)
        lease=co.claim(worker)
        executor=ident("usage-executor")
        self.db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,configuration_ref,
            capabilities,location) VALUES (%s,'fake','fake','1','test',%s,'LOCAL')""",
            (executor,Jsonb(["inspect"])))
        self.db.execute("""INSERT INTO runtime.model_runs(model_run_id,task_id,attempt_id,executor_id,adapter_type,
            requested_model,status,started_at,completed_at,input_tokens,output_tokens,estimated_cost)
            VALUES (%s,%s,%s,%s,'fake','fake-model','FAILED',now(),now(),40,20,2)""",
            (ident("model-run"),task,lease["attempt_id"],executor))
        co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.FAILED_PERMANENT,actor_id=worker)
        dims={r["dimension"]:(float(r["consumed"]),r["unknown"]) for r in self.db.execute("""SELECT d.dimension,d.consumed,d.unknown
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.task_id=%s AND r.stage='task_attempt'""",(task,)).fetchall()}
        self.assertEqual(dims["monetary_cost"],(2.0,False))
        self.assertEqual(dims["tokens_input"],(40.0,False))
        self.assertEqual(dims["tokens_output"],(20.0,False))

    def test_m4c_concurrent_reservations_respect_hard_limit_and_idempotency(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        campaign=self._accounting_campaign({"max_cost":10})
        barrier=Barrier(2)
        def reserve(key):
            db=connect(os.environ["M3_TEST_DATABASE_URL"]); db.autocommit=True
            role=os.environ.get("M3_TEST_DATABASE_ROLE")
            if role: db.execute(f'SET ROLE "{role}"')
            try:
                barrier.wait()
                return CampaignAccounting(db).reserve(campaign_id=campaign,stage="worker",idempotency_key=key,
                    amounts={"monetary_cost":6})
            except ValueError:
                return "REJECTED"
            finally: db.close()
        keys=["parallel-a","parallel-b"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=list(pool.map(reserve,keys))
        self.assertEqual(sum(x != "REJECTED" for x in outcomes),1)
        accepted_key=keys[next(i for i,x in enumerate(outcomes) if x != "REJECTED")]
        duplicate=CampaignAccounting(self.db).reserve(campaign_id=campaign,stage="worker",idempotency_key=accepted_key,
            amounts={"monetary_cost":6})
        self.assertIn(duplicate,outcomes)
        self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.improvement_reservations WHERE campaign_id=%s",(campaign,)).fetchone()["n"],1)

    def test_m4c_failure_usage_crash_reconciliation_stop_and_duplicate_settlement(self):
        service=CampaignAccounting(self.db)
        campaign=self._accounting_campaign({"max_cost":12})
        equal=service.reserve(campaign_id=campaign,stage="equal",idempotency_key="equal",
            amounts={"monetary_cost":3})
        service.mark_dispatched(equal)
        self.assertEqual(service.settle(equal,actual={"monetary_cost":3},terminal_status="SUCCEEDED"),"SETTLED")
        self.assertEqual(service.settle(equal,actual={"monetary_cost":0},terminal_status="SUCCEEDED"),"SETTLED")
        failed=service.reserve(campaign_id=campaign,stage="failed",idempotency_key="failed",
            amounts={"monetary_cost":5})
        service.mark_dispatched(failed)
        service.settle(failed,actual={"monetary_cost":2},terminal_status="FAILED")
        amount=self.db.execute("SELECT consumed,released FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",(failed,)).fetchone()
        self.assertEqual((float(amount["consumed"]),float(amount["released"])),(2.0,3.0))
        crashed=service.reserve(campaign_id=campaign,stage="crashed",idempotency_key="crash-after-dispatch",
            amounts={"monetary_cost":4})
        service.mark_dispatched(crashed)
        report=Coordinator(self.db).reconcile_runtime(scan_id=ident("accounting-recovery"))
        self.assertGreaterEqual(report["campaign_reservations"]["settled_unknown"],1)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.improvement_reservations WHERE reservation_id=%s",(crashed,)).fetchone()["status"],"UNKNOWN")
        snapshot=service.snapshot(campaign)
        self.assertEqual(snapshot["monetary_cost"]["consumed"],9.0)
        self.assertEqual(snapshot["monetary_cost"]["remaining"],3.0)
        ImprovementCampaignService(self.db).stop(campaign,"COMPLETED")
        with self.assertRaisesRegex(ValueError,"stopped"):
            service.reserve(campaign_id=campaign,stage="after-stop",idempotency_key="stopped",
                amounts={"monetary_cost":0})

    def test_m4c_campaign_linked_child_and_attempt_usage_is_reserved_and_settled(self):
        from agentic_runtime.evolution.autonomous import CampaignLimits
        co,runtime_campaign,goal=self.campaign_goal()
        self.db.execute("UPDATE runtime.campaigns SET budget=budget || '{\"compute\":2,\"max_wall_time_seconds\":30}'::jsonb WHERE campaign_id=%s",
                        (runtime_campaign,))
        scope=ident("campaign-scope")
        self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,'task accounting')",(scope,))
        improvement=ident("improvement-campaign")
        ImprovementCampaignService(self.db).create(campaign_id=improvement,scope_id=scope,goal_id=goal,
            budget={},created_by="test",limits=CampaignLimits(max_campaign_children=1,max_attempts=3))
        parent,worker=ident("parent"),ident("parent-worker")
        co.register_worker(worker,["coordinate"])
        co.create_task(parent,runtime_campaign,task_type="coordinate",idempotency_key=ident("parent-key"),
            goal_id=goal,budget={"compute":2,"max_wall_time_seconds":30},max_children=2,depth_remaining=1,
            required_capabilities=["coordinate"])
        lease=co.claim(worker)
        co.transition(parent,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        child=ident("child")
        result=co.authorize_child(request_id=ident("child-request"),parent_task_id=parent,
            parent_attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],child_task_id=child,
            idempotency_key=ident("child-key"),task_type="inspect",goal="inspect a neutral input",
            capabilities=["inspect"],input_refs=[],requested_budget={"compute":1,"max_wall_time_seconds":10})
        self.assertTrue(result["approved"])
        child_reservation=self.db.execute("SELECT reservation_id,status,dispatch_state,task_id FROM runtime.improvement_reservations WHERE task_id=%s AND stage='child_task'",(child,)).fetchone()
        self.assertEqual((child_reservation["status"],child_reservation["dispatch_state"],child_reservation["task_id"]),
            ("RESERVED","DISPATCHED",child))
        with self.assertRaisesRegex(ValueError,"child_tasks"):
            co.authorize_child(request_id=ident("child-request"),parent_task_id=parent,
                parent_attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],child_task_id=ident("child"),
                idempotency_key=ident("child-key"),task_type="inspect",goal="second child",
                capabilities=["inspect"],input_refs=[],requested_budget={"compute":1,"max_wall_time_seconds":10})
        child_worker=ident("child-worker")
        co.register_worker(child_worker,["inspect"])
        child_lease=co.claim(child_worker)
        self.assertEqual(child_lease["task_id"],child)
        attempt_reservation=self.db.execute("SELECT reservation_id,status,dispatch_state,attempt_id FROM runtime.improvement_reservations WHERE attempt_id=%s AND stage='task_attempt'",(child_lease["attempt_id"],)).fetchone()
        self.assertEqual((attempt_reservation["status"],attempt_reservation["dispatch_state"]),("RESERVED","DISPATCHED"))
        co.transition(child,child_lease["attempt_id"],child_lease["lease_epoch"],TaskState.RUNNING,actor_id=child_worker)
        co.transition(child,child_lease["attempt_id"],child_lease["lease_epoch"],TaskState.FAILED_PERMANENT,actor_id=child_worker)
        rows=self.db.execute("""SELECT r.stage,r.status,d.dimension,d.consumed,d.unknown
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.task_id=%s ORDER BY r.stage,d.dimension""",(child,)).fetchall()
        settled={(r["stage"],r["dimension"]):(r["status"],r["consumed"],r["unknown"]) for r in rows}
        self.assertEqual(settled[("child_task","child_tasks")],("SETTLED",1,False))
        self.assertEqual(settled[("task_attempt","attempts")],("UNKNOWN",1,False))
        self.assertTrue(settled[("task_attempt","monetary_cost")][2])

    def test_m4c_actual_overrun_is_persisted_and_stops_campaign(self):
        service=CampaignAccounting(self.db)
        campaign=self._accounting_campaign({"max_cost":5})
        reservation=service.reserve(campaign_id=campaign,stage="provider",idempotency_key="overrun",
            amounts={"monetary_cost":2})
        service.mark_dispatched(reservation)
        self.assertEqual(service.settle(reservation,actual={"monetary_cost":7},terminal_status="SUCCEEDED"),"SETTLED")
        snapshot=service.snapshot(campaign)["monetary_cost"]
        self.assertEqual((snapshot["consumed"],snapshot["remaining"]),(7.0,-2.0))
        self.assertEqual(self.db.execute("SELECT status,stop_reason FROM runtime.improvement_campaigns WHERE campaign_id=%s",(campaign,)).fetchone()["status"],"STOPPED")

    def test_m4c_campaign_stop_race_cannot_authorize_post_stop_reservation(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        campaign=self._accounting_campaign({"max_cost":100})
        barrier=Barrier(2)
        def attempt_reserve():
            db=connect(os.environ["M3_TEST_DATABASE_URL"]); db.autocommit=True
            role=os.environ.get("M3_TEST_DATABASE_ROLE")
            if role: db.execute(f'SET ROLE "{role}"')
            try:
                barrier.wait()
                return CampaignAccounting(db).reserve(campaign_id=campaign,stage="race",idempotency_key="stop-race",
                    amounts={"monetary_cost":1})
            except ValueError: return "REJECTED"
            finally: db.close()
        def stop_campaign():
            db=connect(os.environ["M3_TEST_DATABASE_URL"]); db.autocommit=True
            role=os.environ.get("M3_TEST_DATABASE_ROLE")
            if role: db.execute(f'SET ROLE "{role}"')
            try:
                barrier.wait()
                ImprovementCampaignService(db).stop(campaign,"COMPLETED")
            finally: db.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            reserve_future=pool.submit(attempt_reserve); stop_future=pool.submit(stop_campaign)
            outcome=reserve_future.result(timeout=10); stop_future.result(timeout=10)
        self.assertEqual(self.db.execute("SELECT status FROM runtime.improvement_campaigns WHERE campaign_id=%s",(campaign,)).fetchone()["status"],"COMPLETED")
        if outcome!="REJECTED":
            self.assertEqual(self.db.execute("SELECT status FROM runtime.improvement_reservations WHERE reservation_id=%s",(outcome,)).fetchone()["status"],"RESERVED")
        with self.assertRaisesRegex(ValueError,"stopped"):
            CampaignAccounting(self.db).reserve(campaign_id=campaign,stage="after-stop",idempotency_key="after-race-stop",
                amounts={"monetary_cost":1})

    def test_m4c_gc_reference_writer_race_is_serialized(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        import time
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4c-gc-race-")))
        manifest=store.put(b"race protected"+os.urandom(12),kind="scratch",producer_execution_id="race",producer_attempt_id="race",
            metadata={"retention_class":"TEMPORARY"})
        self.assertEqual(ArtifactGarbageCollector(self.db,store).mark(manifest.artifact_id,grace_seconds=0),"MARKED")
        checked,release,writer_started=Event(),Event(),Event()
        gc_conn=connect(os.environ["M3_TEST_DATABASE_URL"]); gc_conn.autocommit=True
        write_conn=connect(os.environ["M3_TEST_DATABASE_URL"]); write_conn.autocommit=True
        role=os.environ.get("M3_TEST_DATABASE_ROLE")
        if role:
            gc_conn.execute(f'SET ROLE "{role}"')
            write_conn.execute(f'SET ROLE "{role}"')
        def sweep():
            return ArtifactGarbageCollector(gc_conn,store).sweep(manifest.artifact_id,
                _after_reference_check=lambda:(checked.set(),release.wait(5)))
        def writer():
            checked.wait(5); writer_started.set()
            try:
                write_conn.execute("INSERT INTO runtime.knowledge_objects(knowledge_id,kind,content_ref,created_by,status) VALUES (%s,'EVIDENCE',%s,'race','ACTIVE')",
                    (ident("race-knowledge"),manifest.artifact_id))
                return "COMMITTED"
            except Exception:
                return "REJECTED"
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                gc_future=pool.submit(sweep)
                self.assertTrue(checked.wait(5))
                writer_future=pool.submit(writer)
                self.assertTrue(writer_started.wait(5))
                time.sleep(.05)
                release.set()
                self.assertEqual(gc_future.result(timeout=5),"SWEPT")
                self.assertEqual(writer_future.result(timeout=5),"REJECTED")
            self.assertFalse(store.path_for(manifest.artifact_id).exists())
            self.assertEqual(self.db.execute("SELECT count(*) AS n FROM runtime.knowledge_objects WHERE content_ref=%s",(manifest.artifact_id,)).fetchone()["n"],0)
        finally:
            release.set(); gc_conn.close(); write_conn.close()

    def test_m4c_gc_mark_survives_collector_restart_and_rechecks_references(self):
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4c-gc-restart-")))
        manifest=store.put(b"survive restart"+os.urandom(12),kind="scratch",producer_execution_id="restart",producer_attempt_id="restart",
            metadata={"retention_class":"TEMPORARY"})
        self.assertEqual(ArtifactGarbageCollector(self.db,store).mark(manifest.artifact_id,grace_seconds=0),"MARKED")
        self.db.execute("INSERT INTO runtime.knowledge_objects(knowledge_id,kind,content_ref,created_by,status) VALUES (%s,'EVIDENCE',%s,'restart','ACTIVE')",
            (ident("knowledge"),manifest.artifact_id))
        restarted=ArtifactGarbageCollector(self.db,store)
        self.assertEqual(restarted.sweep(manifest.artifact_id),"RETAINED")
        self.assertTrue(store.path_for(manifest.artifact_id).exists())

    def test_m4c_protected_reference_trigger_matrix_is_installed_and_enabled(self):
        tables={"runtime.tasks","runtime.attempts","runtime.model_runs","runtime.artifacts",
            "runtime.knowledge_objects","runtime.events","runtime.outbox","runtime.sandboxes",
            "runtime.routing_decisions","runtime.dispatch_receipts",
            "evolution.eval_suite_versions","evolution.evaluation_packs","evolution.evaluation_records",
            "evolution.eval_suite_change_proposals","evolution.evaluator_runs","evolution.system_genomes",
            "evolution.mutation_proposals","evolution.promotion_decisions","evolution.promotion_authorizations",
            "evolution.evolution_events","evolution.governance_events","evolution.eval_artifact_quarantine",
            "evolution.events"}
        rows=self.db.execute("""SELECT n.nspname||'.'||c.relname AS relation,t.tgenabled
            FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE t.tgname='artifact_reference_lock' AND NOT t.tgisinternal""").fetchall()
        installed={row["relation"] for row in rows if row["tgenabled"] in {"O","A"}}
        self.assertTrue(tables.issubset(installed),f"missing artifact reference guards: {sorted(tables-installed)}")

    def test_m4c_gc_protected_reference_categories_are_retained(self):
        store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m4c-reference-matrix-")))
        gc=ArtifactGarbageCollector(self.db,store)
        def artifact(label):
            return store.put((label+"-"+uuid.uuid4().hex).encode(),kind="reference-test",
                producer_execution_id="reference-matrix",producer_attempt_id="reference-matrix",
                metadata={"retention_class":"TEMPORARY"})
        def retained(manifest,label):
            self.assertEqual(gc.mark(manifest.artifact_id,grace_seconds=0),"RETAINED",label)

        co,runtime_campaign,goal=self.campaign_goal()
        # Accepted task inputs are protected even after the attempt is terminal.
        task_input=artifact("accepted-task-input")
        task,worker=ident("accepted-task"),ident("matrix-worker")
        co.register_worker(worker,["inspect"])
        co.create_task(task,runtime_campaign,task_type="inspect",idempotency_key=ident("matrix-task-key"),
            input_refs=[task_input.artifact_id],required_capabilities=["inspect"])
        lease=co.claim(worker)
        co.transition(task,lease["attempt_id"],lease["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        result_artifact=store.put(b'{"ok":true}',kind="result",producer_execution_id="matrix",
            producer_attempt_id=lease["attempt_id"],metadata={"retention_class":"TEMPORARY"})
        co.register_artifact(task_id=task,attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],
            manifest=result_artifact,artifact_store=store,input_manifest_hash="a"*64)
        co.commit_result(task_id=task,attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],
            artifact_refs=[result_artifact.artifact_id],result_hash=result_artifact.content_hash)
        co.accept_verified_result(task_id=task,attempt_id=lease["attempt_id"],lease_epoch=lease["lease_epoch"],artifact_store=store)
        retained(task_input,"accepted task input")
        retained(result_artifact,"accepted result artifact record")

        knowledge=artifact("knowledge")
        self.db.execute("""INSERT INTO runtime.knowledge_objects(knowledge_id,kind,content_ref,created_by,status)
            VALUES (%s,'EVIDENCE',%s,'matrix','ACTIVE')""",(ident("matrix-knowledge"),knowledge.artifact_id))
        retained(knowledge,"KnowledgeObject/Evidence")

        scope,suite_id,champion,candidate=(ident("matrix-scope"),ident("matrix-suite"),ident("matrix-champion"),ident("matrix-candidate"))
        controller=EvolutionController(self.db)
        controller.register_scope_champion(scope_id=scope,description="reference matrix",genome_id=champion,
            version="1",config_ref="matrix:champion",config={"routes":{"inspect":"x"}})
        suite_artifact=artifact("evaluation-suite")
        controller.register_eval_suite(eval_suite_id=suite_id,version="1",scope_id=scope,
            definition_ref=suite_artifact.artifact_id,integrity_hash=suite_artifact.content_hash,
            created_by="matrix",metric_directions={"quality":"MAX"})
        retained(suite_artifact,"EvalSuiteVersion")
        config_hash=hashlib.sha256(b"matrix candidate").hexdigest()
        self.db.execute("""INSERT INTO evolution.system_genomes(genome_id,scope_id,version,status,config_ref,
            config_hash,mutation_description,mutation_rationale,created_by)
            VALUES (%s,%s,'2','CHALLENGER','matrix:candidate',%s,'matrix','matrix','matrix')""",
            (candidate,scope,config_hash))
        pack_ref=artifact("evaluation-pack")
        self.db.execute("""INSERT INTO evolution.evaluation_packs(pack_id,scope_id,candidate_genome_id,
            champion_genome_id,eval_suite_id,eval_suite_version,pack_hash,definition,created_by)
            VALUES (%s,%s,%s,%s,%s,'1',%s,%s,'matrix')""",
            (ident("matrix-pack"),scope,candidate,champion,suite_id,"b"*64,Jsonb({"input_ref":pack_ref.artifact_id})))
        retained(pack_ref,"EvaluationPack")
        evaluation_ref=artifact("evaluation-record")
        evaluation_id=ident("matrix-evaluation")
        self.db.execute("""INSERT INTO evolution.evaluation_records(evaluation_id,scope_id,candidate_genome_id,
            champion_genome_id,eval_suite_id,eval_suite_version,conditions_ref,evaluator_version,evaluator_id,
            result_refs,status,completed_at)
            VALUES (%s,%s,%s,%s,%s,'1',%s,'matrix-v1','independent',%s,'COMPLETED',now())""",
            (evaluation_id,scope,candidate,champion,suite_id,evaluation_ref.artifact_id,Jsonb([evaluation_ref.artifact_id])))
        retained(evaluation_ref,"EvaluationRecord")
        evaluator_ref=artifact("evaluator-run")
        self.db.execute("""INSERT INTO evolution.evaluator_runs(evaluator_run_id,evaluation_id,evaluator_id,
            evaluator_type,implementation_version,independence_relation,execution_ref)
            VALUES (%s,%s,'independent','DETERMINISTIC','v1','DETERMINISTIC',%s)""",
            (ident("matrix-evaluator"),evaluation_id,evaluator_ref.artifact_id))
        retained(evaluator_ref,"EvaluatorRun")

        genome_ref=artifact("genome-config")
        genome_with_ref=ident("matrix-config-genome")
        self.db.execute("""INSERT INTO evolution.system_genomes(genome_id,scope_id,version,status,config_ref,
            config_hash,mutation_description,mutation_rationale,created_by)
            VALUES (%s,%s,'4','DRAFT',%s,%s,'config reference','matrix','matrix')""",
            (genome_with_ref,scope,genome_ref.artifact_id,hashlib.sha256(b"reference config").hexdigest()))
        retained(genome_ref,"SystemGenome configuration")
        rollback_ref=artifact("rollback-target")
        rollback_genome=ident("matrix-rollback-genome")
        self.db.execute("""INSERT INTO evolution.system_genomes(genome_id,scope_id,version,status,config_ref,
            config_hash,mutation_description,mutation_rationale,created_by,rollback_ref)
            VALUES (%s,%s,'3','DRAFT','matrix:draft',%s,'rollback','rollback','matrix',%s)""",
            (rollback_genome,scope,hashlib.sha256(b"rollback").hexdigest(),rollback_ref.artifact_id))
        retained(rollback_ref,"rollback target")
        proposal_ref=artifact("mutation-proposal")
        self.db.execute("""INSERT INTO evolution.mutation_proposals(mutation_id,scope_id,observation_refs,
            parent_genome_id,candidate_config_ref,candidate_config_hash,hypothesis,created_by,status)
            VALUES (%s,%s,'[]',%s,%s,%s,'matrix hypothesis','matrix','PROPOSED')""",
            (ident("matrix-mutation"),scope,champion,proposal_ref.artifact_id,"c"*64))
        retained(proposal_ref,"MutationProposal")
        decision_ref=artifact("promotion-decision")
        decision_id=ident("matrix-decision")
        self.db.execute("""INSERT INTO evolution.promotion_decisions(decision_id,scope_id,candidate_genome_id,
            champion_genome_id,evaluation_refs,decision,mode,rationale_ref,created_by)
            VALUES (%s,%s,%s,%s,%s,'WOULD_PROMOTE','SHADOW',%s,'matrix')""",
            (decision_id,scope,candidate,champion,Jsonb([evaluation_id]),decision_ref.artifact_id))
        retained(decision_ref,"PromotionDecision")
        authorization_ref=artifact("promotion-authorization")
        self.db.execute("""INSERT INTO evolution.promotion_authorizations(authorization_id,scope_id,
            expected_champion_id,challenger_id,shadow_decision_id,evaluation_refs,authorized_by,action,status)
            VALUES (%s,%s,%s,%s,%s,%s,'independent-governance','PROMOTE','AUTHORIZED')""",
            (ident("matrix-authorization"),scope,champion,candidate,decision_id,Jsonb([authorization_ref.artifact_id])))
        retained(authorization_ref,"PromotionAuthorization")

        attempt_ref=artifact("active-attempt")
        attempt_task=ident("matrix-attempt-task")
        co.create_task(attempt_task,runtime_campaign,task_type="inspect",idempotency_key=ident("matrix-attempt-key"),
            required_capabilities=["inspect"])
        active=co.claim(worker)
        co.transition(attempt_task,active["attempt_id"],active["lease_epoch"],TaskState.RUNNING,actor_id=worker)
        self.db.execute("UPDATE runtime.attempts SET telemetry=%s WHERE attempt_id=%s",
            (Jsonb({"artifact_ref":attempt_ref.artifact_id}),active["attempt_id"]))
        retained(attempt_ref,"active attempt")

        sandbox_ref=artifact("sandbox-policy")
        self.db.execute("""INSERT INTO runtime.sandboxes(sandbox_id,task_id,attempt_id,implementation,
            base_revision,workspace_ref,status,cleanup_status,policy_ref)
            VALUES (%s,%s,%s,'reference-test','revision','/tmp/matrix','ACTIVE','PENDING',%s)""",
            (ident("matrix-sandbox"),attempt_task,active["attempt_id"],sandbox_ref.artifact_id))
        retained(sandbox_ref,"sandbox policy")
        event_ref=artifact("runtime-event")
        event_id=co._event(event_type="REFERENCE_MATRIX",actor_id="matrix",correlation_id=attempt_task,
            task_id=attempt_task,attempt_id=active["attempt_id"],payload={"artifact_ref":event_ref.artifact_id})
        retained(event_ref,"runtime Event")
        outbox_ref=artifact("outbox")
        self.db.execute("""INSERT INTO runtime.outbox(outbox_id,event_id,topic,idempotency_key,payload)
            VALUES (%s,%s,'test','matrix-outbox',%s)""",
            (ident("matrix-outbox"),event_id,Jsonb({"artifact_ref":outbox_ref.artifact_id})))
        retained(outbox_ref,"outbox")
        evo_event_ref=artifact("evolution-event")
        controller._event("REFERENCE_MATRIX",scope,ident("matrix-actor"),{"artifact_ref":evo_event_ref.artifact_id},"matrix")
        retained(evo_event_ref,"evolution Event")
        change_ref=artifact("eval-change")
        self.db.execute("""INSERT INTO evolution.eval_suite_change_proposals(change_id,scope_id,observation_refs,
            current_eval_suite_id,current_eval_suite_version,proposed_definition_ref,proposed_integrity_hash,
            rationale,status,created_by)
            VALUES (%s,%s,'[]',%s,'1',%s,%s,'matrix','PROPOSED','matrix')""",
            (ident("matrix-eval-change"),scope,suite_id,change_ref.artifact_id,"d"*64))
        retained(change_ref,"EvalSuiteChangeProposal")
