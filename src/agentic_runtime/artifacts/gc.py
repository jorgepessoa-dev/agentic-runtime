"""Conservative artifact mark/grace/sweep collection.

Only manifests explicitly classified TEMPORARY or WORKING are eligible. All
unknown classes fail closed and remain stored.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from agentic_runtime.artifacts.store import ArtifactStore


class ArtifactGarbageCollector:
    def __init__(self, db: Any, store: ArtifactStore) -> None:
        self.db, self.store = db, store

    def _referenced(self, artifact_id: str) -> bool:
        checks = (
            ("SELECT 1 FROM runtime.artifacts WHERE artifact_id=%s", (artifact_id,)),
            ("SELECT 1 FROM runtime.tasks WHERE input_refs::text LIKE %s OR result_refs::text LIKE %s OR metadata::text LIKE %s", (f"%{artifact_id}%", f"%{artifact_id}%", f"%{artifact_id}%")),
            ("SELECT 1 FROM runtime.attempts WHERE telemetry::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM runtime.model_runs WHERE telemetry::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM runtime.cognitive_invocations WHERE raw_artifact_id=%s OR normalized_artifact_id=%s", (artifact_id, artifact_id)),
            ("SELECT 1 FROM runtime.cognitive_artifacts WHERE blob_artifact_id=%s", (artifact_id,)),
            ("SELECT 1 FROM runtime.sandboxes WHERE policy_ref=%s OR metadata::text LIKE %s", (artifact_id, f"%{artifact_id}%")),
            ("SELECT 1 FROM runtime.routing_decisions WHERE policy_ref=%s OR reason LIKE %s", (artifact_id, f"%{artifact_id}%")),
            ("SELECT 1 FROM runtime.dispatch_receipts WHERE logical_effect_ref=%s", (artifact_id,)),
            ("SELECT 1 FROM runtime.knowledge_objects WHERE content_ref=%s OR source_artifact_refs::text LIKE %s", (artifact_id, f"%{artifact_id}%")),
            ("SELECT 1 FROM evolution.eval_suite_versions WHERE definition_ref=%s", (artifact_id,)),
            ("SELECT 1 FROM evolution.evaluation_packs WHERE definition::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.evaluation_records WHERE result_refs::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.evaluation_records WHERE conditions_ref=%s", (artifact_id,)),
            ("SELECT 1 FROM evolution.evaluator_runs WHERE execution_ref=%s", (artifact_id,)),
            ("SELECT 1 FROM evolution.system_genomes WHERE config_ref=%s OR rollback_ref=%s", (artifact_id, artifact_id)),
            ("SELECT 1 FROM evolution.mutation_proposals WHERE candidate_config_ref=%s OR metadata::text LIKE %s", (artifact_id, f"%{artifact_id}%")),
            ("SELECT 1 FROM evolution.system_genomes WHERE metadata::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.e1_cognitive_proposal_lineage WHERE raw_artifact_ref=%s OR normalized_artifact_ref=%s OR proposal_artifact_ref=%s", (artifact_id, artifact_id, artifact_id)),
            ("SELECT 1 FROM evolution.e1_campaign_candidate_refs WHERE raw_artifact_ref=%s OR normalized_artifact_ref=%s OR proposal_artifact_ref=%s", (artifact_id, artifact_id, artifact_id)),
            ("SELECT 1 FROM evolution.eval_suite_change_proposals WHERE proposed_definition_ref=%s OR observation_refs::text LIKE %s OR validation_refs::text LIKE %s", (artifact_id, f"%{artifact_id}%", f"%{artifact_id}%")),
            ("SELECT 1 FROM evolution.promotion_decisions WHERE rationale_ref=%s OR evaluation_refs::text LIKE %s", (artifact_id, f"%{artifact_id}%")),
            ("SELECT 1 FROM evolution.promotion_authorizations WHERE evaluation_refs::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.evolution_events WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.e1_events WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.governance_events WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.events WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM evolution.eval_artifact_quarantine WHERE definition_ref=%s", (artifact_id,)),
            ("SELECT 1 FROM runtime.events WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
            ("SELECT 1 FROM runtime.outbox WHERE payload::text LIKE %s", (f"%{artifact_id}%",)),
        )
        for sql, params in checks:
            if self.db.execute(sql + " LIMIT 1", params).fetchone():
                return True
        return False

    def mark(self, artifact_id: str, *, grace_seconds: int = 86400) -> str:
        manifest = self.store.verify(artifact_id)
        retention = str(manifest.metadata.get("retention_class", "UNCLASSIFIED"))
        if retention not in {"TEMPORARY", "WORKING", "EVIDENCE", "ACCEPTED", "GOVERNANCE", "ROLLBACK_CRITICAL"}:
            # Older cognitive adapters omitted this label after persisting
            # protected PostgreSQL references. Preserve those blobs under the
            # same artifact lock used by protected-reference writers.
            with self.db.transaction():
                self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (artifact_id,))
                if self._referenced(artifact_id):
                    return "RETAINED"
            raise ValueError("artifact retention class is absent or invalid")
        if retention not in {"TEMPORARY", "WORKING"}:
            return "RETAINED"
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (artifact_id,))
            existing = self.db.execute("SELECT status FROM runtime.artifact_gc_candidates WHERE artifact_id=%s FOR UPDATE", (artifact_id,)).fetchone()
            if existing and existing["status"] == "SWEPT":
                return "SWEPT"
            if self._referenced(artifact_id):
                return "RETAINED"
            self.db.execute("""INSERT INTO runtime.artifact_gc_candidates
                (artifact_id,retention_class,status,delete_after,last_check)
                VALUES (%s,%s,'MARKED',now()+(%s * interval '1 second'),%s)
                ON CONFLICT (artifact_id) DO UPDATE SET
                  retention_class=EXCLUDED.retention_class,
                  status=CASE WHEN runtime.artifact_gc_candidates.status='SWEPT' THEN 'SWEPT' ELSE 'MARKED' END,
                  delete_after=GREATEST(runtime.artifact_gc_candidates.delete_after,EXCLUDED.delete_after),
                  last_check=EXCLUDED.last_check""",
                (artifact_id, retention, grace_seconds, json.dumps({"referenced": False})))
        return "MARKED"

    def sweep(self, artifact_id: str, *, _after_reference_check: Any | None = None) -> str:
        folder = self.store.path_for(artifact_id)
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (artifact_id,))
            row = self.db.execute("SELECT * FROM runtime.artifact_gc_candidates WHERE artifact_id=%s FOR UPDATE", (artifact_id,)).fetchone()
            if not row:
                return "NOT_MARKED"
            if row["status"] == "SWEPT":
                # Logical deletion commits before unlinking. This path repairs a
                # crash between that commit and filesystem cleanup.
                pass
            elif row["status"] != "MARKED":
                return "NOT_MARKED"
            else:
                if row["retention_class"] not in {"TEMPORARY", "WORKING"} or self._referenced(artifact_id):
                    self.db.execute("UPDATE runtime.artifact_gc_candidates SET status='RETAINED',last_check=%s WHERE artifact_id=%s",
                                    (json.dumps({"referenced": True}), artifact_id))
                    return "RETAINED"
                if self.db.execute("SELECT now() >= %s AS elapsed", (row["delete_after"],)).fetchone()["elapsed"] is False:
                    return "GRACE_PERIOD"
                if _after_reference_check is not None:
                    _after_reference_check()
                if folder.exists():
                    manifest = self.store.verify(artifact_id)
                    if str(manifest.metadata.get("retention_class")) != row["retention_class"]:
                        raise ValueError("artifact retention metadata changed after marking")
                # Every protected reference writer takes this same advisory
                # lock via runtime.lock_artifact_references(). Mark SWEPT and
                # commit while holding it; later references are rejected.
                self.db.execute("UPDATE runtime.artifact_gc_candidates SET status='SWEPT',swept_at=now(),last_check=%s WHERE artifact_id=%s",
                                (json.dumps({"hash_verified": True,"logical_delete":True}), artifact_id))
                payload = {"artifact_id": artifact_id}
                digest = hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(",",":")).encode()).hexdigest()
                self.db.execute("""INSERT INTO evolution.evolution_events
                    (event_id,event_type,actor_id,payload,payload_hash)
                    VALUES (%s,'ARTIFACT_GC_SWEPT','artifact-gc',%s,%s)""",
                    ("gc_" + uuid.uuid4().hex, json.dumps(payload), digest))
        if folder.exists():
            import shutil
            shutil.rmtree(folder)
        return "SWEPT"
