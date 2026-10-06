from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from pathlib import Path

from psycopg.types.json import Jsonb

from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.evolution.e1 import E1EvolutionService
from agentic_runtime.artifacts.gc import ArtifactGarbageCollector
from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.persistence.postgres import connect, apply_migrations


def ident(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


@unittest.skipUnless(all(os.environ.get(f"M4C_ROLE_DSN_{n}") for n in ("RUNTIME","GOVERNANCE","EVALUATOR","E1PROMOTION")),
                     "separate M8 PostgreSQL identities required")
class M8CampaignCostPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin=connect(os.environ["M3_TEST_DATABASE_URL"])
        apply_migrations(cls.admin)

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()

    def setUp(self):
        for key in ("RUNTIME","GOVERNANCE","EVALUATOR","E1PROMOTION"):
            setattr(self,key.lower(),connect(os.environ[f"M4C_ROLE_DSN_{key}"]))
        self.service=E1EvolutionService(self.runtime,governance_db=self.governance,
            evaluator_db=self.evaluator,promotion_db=self.e1promotion)
        self.scope=ident("m8costscope")
        self.champion=ident("m8costchampion")
        self.suite=ident("m8costsuite")
        self.service.register_scope(scope_id=self.scope,description="generic E1 cost policy regression",
            champion_id=self.champion,champion_config={"routing":{"preference":{"default":"route-a"}}},
            suite_id=self.suite,suite_version="1",suite_definition={"expected":{"ok":True}},
            promotion_policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
                "required_improvement":0.05,"protected_quality_metric":"quality","maximum_cost_units":1},
            mode="ACTIVE_E1",allowed_paths=("routing.preference.default",))

    def tearDown(self):
        for name in ("runtime","governance","evaluator","e1promotion"):
            connection=getattr(self,name,None)
            if connection: connection.close()

    def create_campaign(self, *, budget=None):
        campaign,goal=ident("m8costcampaign"),ident("m8costgoal")
        self.service.create_campaign(campaign_id=campaign,goal_id=goal,scope_id=self.scope,
            budget=budget or {"max_experiment_units":20})
        return campaign

    def test_definer_role_can_read_frozen_policy_without_mutating_it(self):
        privileges=self.runtime.execute("""SELECT
          has_table_privilege('agentic_promotion_executor','evolution.e1_campaign_comparison_policies','SELECT') AS can_read,
          has_table_privilege('agentic_promotion_executor','evolution.e1_campaign_comparison_policies','INSERT') AS can_insert,
          has_table_privilege('agentic_promotion_executor','evolution.e1_campaign_comparison_policies','UPDATE') AS can_update,
          has_table_privilege('agentic_promotion_executor','evolution.e1_campaign_comparison_policies','DELETE') AS can_delete""").fetchone()
        self.assertEqual(dict(privileges),{"can_read":True,"can_insert":False,"can_update":False,"can_delete":False})

    def freeze(self,campaign):
        policy={"improvement_metric":"latency_ms","improvement_direction":"MIN",
            "required_improvement":0.05,"minimum_improvement_fraction":0.05,
            "protected_quality_metric":"quality","quality_floor":1,
            "monetary_evidence_mode":"CAMPAIGN_SAFETY_ONLY",
            "monetary_cost_role":"CAMPAIGN_BUDGET_ONLY",
            "token_and_ratecard_estimates":"TELEMETRY_ONLY",
            "actual_billing_provenance":"AUTHORITATIVE_ONLY"}
        version="m8-e1-quality-latency-no-money-v2"
        digest=self.runtime.execute("""SELECT encode(sha256(convert_to(jsonb_build_object(
            'scope_id',%s::text,'policy_version',%s::text,'comparison_policy',%s::jsonb)::text,'UTF8')),'hex') AS hash""",
            (self.scope,version,Jsonb(policy))).fetchone()["hash"]
        self.runtime.execute("""INSERT INTO evolution.e1_campaign_comparison_policies
            (campaign_id,scope_id,policy_version,comparison_policy,policy_hash,created_by)
            VALUES (%s,%s,%s,%s,%s,'m8-test-governance')""",
            (campaign,self.scope,version,Jsonb(policy),digest))
        return policy,digest

    def test_policy_is_versioned_frozen_and_ignores_unknown_money_only_for_comparison(self):
        campaign=self.create_campaign();policy,digest=self.freeze(campaign);self.runtime.commit()
        stored=self.runtime.execute("SELECT policy_version,policy_hash,comparison_policy FROM evolution.e1_campaign_comparison_policies WHERE campaign_id=%s",(campaign,)).fetchone()
        self.assertEqual(stored["policy_version"],"m8-e1-quality-latency-no-money-v2")
        self.assertEqual(stored["policy_hash"],digest)
        self.assertEqual(stored["comparison_policy"]["monetary_evidence_mode"],"CAMPAIGN_SAFETY_ONLY")
        with self.assertRaises(Exception):
            self.runtime.execute("UPDATE evolution.e1_campaign_comparison_policies SET comparison_policy='{}' WHERE campaign_id=%s",(campaign,))
        self.runtime.rollback()
        self.runtime.execute("UPDATE runtime.improvement_campaigns SET status='EVALUATING' WHERE campaign_id=%s",(campaign,));self.runtime.commit()
        late=self.create_campaign();self.runtime.execute("UPDATE runtime.improvement_campaigns SET status='EVALUATING' WHERE campaign_id=%s",(late,));self.runtime.commit()
        with self.assertRaises(Exception): self.freeze(late)
        self.runtime.rollback()

    def test_unknown_cost_cannot_bypass_hard_campaign_spend_ceiling(self):
        campaign=self.create_campaign(budget={"max_experiment_units":20,"max_cost":1})
        with self.assertRaisesRegex(ValueError,"unknown monetary_cost usage"):
            CampaignAccounting(self.runtime).reserve(campaign_id=campaign,stage="unknown-cost",
                idempotency_key=ident("idem"),amounts={"experiment_units":1,"monetary_cost":None})
        self.runtime.rollback()

    def test_e1_event_artifact_reference_is_visible_to_gc(self):
        campaign=self.create_campaign(); store=ArtifactStore(Path(tempfile.mkdtemp(prefix="m8-e1-gc-")))
        artifact=store.put(b"frozen E1 evidence",kind="frozen-evidence",producer_execution_id=campaign,
            producer_attempt_id="freeze",metadata={"retention_class":"TEMPORARY"})
        payload={"frozen_pack_artifact_ref":artifact.artifact_id}
        digest=self.runtime.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS h",
            (Jsonb(payload),)).fetchone()["h"]
        self.runtime.execute("""INSERT INTO evolution.e1_events
            (event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
            VALUES (%s,%s,%s,'M8_TEST_FROZEN_PACK','m8-gc-test',%s,%s)""",
            (ident("m8gc-event"),self.scope,campaign,Jsonb(payload),digest)); self.runtime.commit()
        gc=ArtifactGarbageCollector(self.runtime,store)
        self.assertTrue(gc._referenced(artifact.artifact_id))
        self.assertEqual(gc.mark(artifact.artifact_id,grace_seconds=0),"RETAINED")
        self.assertTrue(store.path_for(artifact.artifact_id).exists())


if __name__=="__main__": unittest.main()
