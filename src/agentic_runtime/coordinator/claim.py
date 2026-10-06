"""Atomic task claim and lease issuance."""

from __future__ import annotations

from typing import Any, Callable

from agentic_runtime.accounting.campaign import CampaignAccounting


class TaskClaimService:
    """Owns queue selection, attempt creation, and lease acquisition as one unit."""

    def __init__(self, connection: Any, emit_event: Callable[..., str]) -> None:
        self.db = connection
        self._event = emit_event

    def claim(
        self,
        worker_id: str,
        *,
        attempt_id: str,
        lease_seconds: int = 60,
        allowed_task_types: list[str] | None = None,
        resource_classes: list[str] | None = None,
        worker_instance_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Claim one ready task; all locks are released before return."""
        with self.db.transaction():
            row = self.db.execute(
                """SELECT t.* FROM runtime.tasks t
                JOIN runtime.campaigns c ON c.campaign_id=t.campaign_id AND c.status='ACTIVE'
                JOIN runtime.workers w ON w.worker_id=%s AND w.status IN ('IDLE','BUSY')
                LEFT JOIN runtime.coordination_plan_versions pv
                  ON pv.plan_version_id=t.plan_version_id AND pv.status='ACCEPTED'
                WHERE t.status IN ('QUEUED','RETRY_PENDING') AND t.run_after<=now()
                  AND w.capabilities @> t.required_capabilities
                  AND (%s::text[] IS NULL OR t.task_type=ANY(%s))
                  AND (%s::text[] IS NULL OR coalesce(t.budget->>'resource_class','deterministic_compute_job')=ANY(%s))
                  AND (t.plan_version_id IS NULL OR
                       (pv.plan_version_id IS NOT NULL AND pv.version=(SELECT max(latest.version)
                            FROM runtime.coordination_plan_versions latest
                            WHERE latest.plan_id=pv.plan_id AND latest.status='ACCEPTED') AND
                        pg_try_advisory_xact_lock(hashtextextended('m9plan:' || pv.plan_version_id,0)) AND
                        (SELECT count(*) FROM runtime.tasks active
                         WHERE active.plan_version_id=pv.plan_version_id
                           AND active.status IN ('LEASED','CONTEXT_VALIDATED','RUNNING','WAITING_CHILDREN',
                             'WAITING_TOOL','WAITING_IO','BLOCKED','CHECKPOINTED'))
                         < (pv.limits->>'max_concurrent_descendants')::integer))
                  AND (t.max_attempts IS NULL OR
                       (SELECT count(*) FROM runtime.attempts used WHERE used.task_id=t.task_id)
                           < t.max_attempts)
                  AND NOT EXISTS (
                    SELECT 1 FROM runtime.task_dependencies d JOIN runtime.tasks p
                      ON p.task_id=d.depends_on_task_id
                    WHERE d.task_id=t.task_id AND (
                      (t.plan_version_id IS NULL
                        AND d.dependency_type='REQUIRES_ACCEPTED' AND p.status<>'ACCEPTED') OR
                      (t.plan_version_id IS NOT NULL AND NOT (
                        (d.dependency_type='REQUIRES_ACCEPTED' AND p.status='ACCEPTED') OR
                        (d.dependency_type='REQUIRES_ARTIFACT' AND p.status IN ('RESULT_COMMITTED','VERIFIED','ACCEPTED')
                          AND CASE WHEN jsonb_typeof(p.result_refs)='array'
                                   THEN jsonb_array_length(p.result_refs)>0 ELSE false END) OR
                        (d.dependency_type='ORDER_ONLY' AND p.status IN
                          ('ACCEPTED','REJECTED','NEEDS_REVIEW','FAILED_TRANSIENT','FAILED_PERMANENT',
                           'CANCELLED','QUARANTINED','BUDGET_EXCEEDED'))))
                  ))
                ORDER BY t.priority DESC,t.run_after,t.created_at
                LIMIT 1 FOR UPDATE OF t SKIP LOCKED""",
                (
                    worker_id,
                    allowed_task_types,
                    allowed_task_types,
                    resource_classes,
                    resource_classes,
                ),
            ).fetchone()
            if not row:
                return None
            accounting = CampaignAccounting(self.db)
            attempt_reservation = accounting.reserve_task_attempt(
                goal_id=row["goal_id"],
                task_id=row["task_id"],
                attempt_id=attempt_id,
                task_budget=row["budget"] or {},
            )
            epoch = row["lease_epoch"] + 1
            self.db.execute(
                "UPDATE runtime.tasks SET status='LEASED',lease_epoch=%s,updated_at=now() WHERE task_id=%s",
                (epoch, row["task_id"]),
            )
            self.db.execute(
                """INSERT INTO runtime.attempts
                (attempt_id,task_id,worker_id,lease_epoch,status,worker_instance_id)
                VALUES (%s,%s,%s,%s,'LEASED',%s)""",
                (attempt_id, row["task_id"], worker_id, epoch, worker_instance_id),
            )
            if attempt_reservation:
                accounting.attach_and_dispatch(
                    attempt_reservation, task_id=row["task_id"], attempt_id=attempt_id
                )
            self.db.execute(
                """INSERT INTO runtime.leases
                (task_id,worker_id,attempt_id,lease_epoch,lease_until,last_heartbeat,status)
                VALUES (%s,%s,%s,%s,now()+(%s * interval '1 second'),now(),'ACTIVE')
                ON CONFLICT(task_id) DO UPDATE SET worker_id=EXCLUDED.worker_id,
                  attempt_id=EXCLUDED.attempt_id,lease_epoch=EXCLUDED.lease_epoch,
                  lease_until=EXCLUDED.lease_until,last_heartbeat=now(),status='ACTIVE',updated_at=now()""",
                (row["task_id"], worker_id, attempt_id, epoch, lease_seconds),
            )
            self.db.execute(
                "UPDATE runtime.workers SET status='BUSY',last_heartbeat=now(),updated_at=now() WHERE worker_id=%s",
                (worker_id,),
            )
            self._event(
                event_type="TASK_LEASED",
                actor_id=worker_id,
                correlation_id=row["task_id"],
                campaign_id=row["campaign_id"],
                goal_id=row["goal_id"],
                task_id=row["task_id"],
                attempt_id=attempt_id,
                payload={
                    "lease_epoch": epoch,
                    "worker_instance_id": worker_instance_id,
                },
            )
            delegation = (
                self.db.execute(
                    """UPDATE runtime.coordination_delegations
                SET status='AUTHORIZED',parent_attempt_id=coalesce(parent_attempt_id,
                    (SELECT attempt_id FROM runtime.attempts pa WHERE pa.task_id=%s
                     ORDER BY pa.lease_epoch DESC LIMIT 1)),updated_at=now()
                WHERE child_task_id=%s AND status='PLAN_ACCEPTED'
                RETURNING delegation_id,plan_version_id""",
                    (row["parent_task_id"], row["task_id"]),
                ).fetchone()
                if row["parent_task_id"]
                else None
            )
            if delegation:
                self._event(
                    event_type="DELEGATION_AUTHORIZED",
                    actor_id="coordinator",
                    correlation_id=delegation["delegation_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    attempt_id=attempt_id,
                    payload={
                        "delegation_id": delegation["delegation_id"],
                        "plan_version_id": delegation["plan_version_id"],
                        "lease_epoch": epoch,
                    },
                )
            response = dict(row)
            # Preserve the established worker/facade envelope while storing
            # scheduling authority in typed columns instead of JSONB.
            metadata = dict(response.get("metadata") or {})
            if response.get("plan_version_id"):
                metadata.setdefault("plan_version_id", response["plan_version_id"])
            response["metadata"] = metadata
            budget = dict(response.get("budget") or {})
            if response.get("max_attempts") is not None:
                budget.setdefault("max_attempts", response["max_attempts"])
            response["budget"] = budget
            return {
                **response,
                "status": "LEASED",
                "lease_epoch": epoch,
                "attempt_id": attempt_id,
                "worker_id": worker_id,
            }
