from __future__ import annotations

import hashlib
import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from agentic_runtime.cognitive.contracts import CognitiveCapabilities, UsageState
from agentic_runtime.cognitive.ledger import CognitiveLedger
from agentic_runtime.persistence.postgres import apply_migrations, connect


@unittest.skipUnless(os.environ.get("M3_TEST_DATABASE_URL"), "M8 persistence checks need disposable PostgreSQL")
class M8CognitivePersistenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = connect(os.environ["M3_TEST_DATABASE_URL"])
        apply_migrations(cls.db)
        cls.db.commit()

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def test_route_registry_persists_observed_capabilities_without_secrets(self):
        worker = "m8worker_" + uuid.uuid4().hex
        route = "m8route_" + uuid.uuid4().hex
        self.db.execute("""INSERT INTO runtime.worker_identities
            (worker_id,token_sha256,granted_capabilities,allowed_task_types,resource_classes,
             status,protocol_major,token_expires_at,created_by)
            VALUES (%s,%s,'[]','[]','[]','ACTIVE',1,%s,'m8-test')""",
            (worker, hashlib.sha256(worker.encode()).hexdigest(),
             datetime.now(timezone.utc) + timedelta(hours=1)))
        self.db.commit()
        capabilities = CognitiveCapabilities(route_id=route, provider="provider-family-a",
            model_family="family-a", requested_model="model-label", supported_roles=frozenset({"hypothesis_generator"}),
            structured_output=True, tool_use=False, max_input_tokens=12000, concurrency_limit=1,
            supports_cancel=True, cost_state=UsageState.SUBSCRIPTION_UNPRICED,
            declared=frozenset({"structured_output"}), observed=frozenset({"structured_output"}),
            health="HEALTHY", last_probe=datetime.now(timezone.utc).isoformat())
        CognitiveLedger(self.db).observe_route(capabilities=capabilities, worker_id=worker,
            adapter_version="m8-test-v1", resource_class="remote_api_call", config_ref="test:route")
        row = self.db.execute("SELECT * FROM runtime.cognitive_routes WHERE route_id=%s", (route,)).fetchone()
        self.assertEqual(row["health"], "HEALTHY")
        self.assertEqual(row["cost_state"], "SUBSCRIPTION_UNPRICED")
        self.assertEqual(row["observed_capabilities"], ["structured_output"])
        columns = {r["column_name"] for r in self.db.execute("""SELECT column_name FROM information_schema.columns
            WHERE table_schema='runtime' AND table_name='cognitive_routes'""").fetchall()}
        self.assertFalse(any(name in columns for name in {"api_key", "token", "secret", "credential_value"}))
        self.db.rollback()

    def test_terminal_invocation_provenance_is_database_immutable(self):
        trigger = self.db.execute("""SELECT t.tgenabled FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='runtime' AND c.relname='cognitive_invocations'
              AND t.tgname='cognitive_invocation_immutable'""").fetchone()
        self.assertIsNotNone(trigger)
        self.assertEqual(trigger["tgenabled"], "O")
        self.assertTrue(self.db.execute("""SELECT EXISTS (
            SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='runtime' AND c.relname='cognitive_invocations'
              AND t.tgname='artifact_reference_lock' AND t.tgenabled='O') AS ok""").fetchone()["ok"])


if __name__ == "__main__":
    unittest.main()
