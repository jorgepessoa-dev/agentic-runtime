"""Lease, attempt, plan, and campaign reconciliation services."""

from __future__ import annotations

import uuid
from typing import Any

from agentic_runtime.accounting.campaign import CampaignAccounting


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class ReconciliationService:
    def __init__(self, connection: Any, emit_event: Any) -> None:
        self.db = connection
        self._event = emit_event

    def reconcile_expired_leases(self, *, retry_delay_seconds: int = 0) -> list[str]:
        """Fence expired attempts and put tasks into explicit retry state."""
        recovered: list[str] = []
        with self.db.transaction():
            rows = self.db.execute("""SELECT t.task_id,t.campaign_id,t.goal_id,t.status,t.lease_epoch,
                t.budget,t.max_attempts,l.worker_id,l.attempt_id,l.lease_epoch AS active_epoch
                FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE l.status='ACTIVE' AND l.lease_until<=now() FOR UPDATE OF t,l SKIP LOCKED""").fetchall()
            for row in rows:
                if row["status"] in {
                    "ACCEPTED",
                    "REJECTED",
                    "FAILED_TRANSIENT",
                    "FAILED_PERMANENT",
                    "FAILED",
                    "CANCELLED",
                    "QUARANTINED",
                    "BUDGET_EXCEEDED",
                }:
                    # Repair legacy/stale terminal leases without changing the
                    # task outcome or manufacturing a retry.
                    self.db.execute(
                        "UPDATE runtime.leases SET status='RELEASED',updated_at=now() WHERE task_id=%s",
                        (row["task_id"],),
                    )
                    self._event(
                        event_type="TERMINAL_LEASE_RELEASED",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={
                            "terminal_task_status": row["status"],
                            "lease_epoch": row["active_epoch"],
                        },
                    )
                    continue
                self.db.execute(
                    "UPDATE runtime.leases SET status='EXPIRED',updated_at=now() WHERE task_id=%s",
                    (row["task_id"],),
                )
                self.db.execute(
                    "UPDATE runtime.attempts SET status='ABANDONED',completed_at=now() WHERE attempt_id=%s",
                    (row["attempt_id"],),
                )
                self.db.execute(
                    "UPDATE runtime.workers SET status='IDLE',updated_at=now() WHERE worker_id=%s",
                    (row["worker_id"],),
                )
                max_attempts = row["max_attempts"]
                attempt_count = self.db.execute(
                    "SELECT count(*) AS n FROM runtime.attempts WHERE task_id=%s",
                    (row["task_id"],),
                ).fetchone()["n"]
                if (
                    isinstance(max_attempts, int)
                    and not isinstance(max_attempts, bool)
                    and max_attempts > 0
                    and attempt_count >= max_attempts
                ):
                    # The state machine represents lease recovery as
                    # RETRY_PENDING before a terminal budget decision. Keep
                    # both updates inside this transaction so the transient
                    # intermediate state is never externally observable.
                    self.db.execute(
                        "UPDATE runtime.tasks SET status='RETRY_PENDING',updated_at=now() WHERE task_id=%s",
                        (row["task_id"],),
                    )
                    self.db.execute(
                        "UPDATE runtime.tasks SET status='BUDGET_EXCEEDED',updated_at=now() WHERE task_id=%s",
                        (row["task_id"],),
                    )
                    self._event(
                        event_type="LEASE_EXPIRED_RECONCILED",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={
                            "stale_epoch": row["active_epoch"],
                            "new_state": "BUDGET_EXCEEDED",
                        },
                    )
                    self._event(
                        event_type="TASK_BUDGET_EXCEEDED",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={"reason": "maximum task attempts exhausted"},
                    )
                    self._event(
                        event_type="TASK_ATTEMPT_BUDGET_EXCEEDED",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={
                            "max_attempts": max_attempts,
                            "attempt_count": attempt_count,
                            "stale_epoch": row["active_epoch"],
                        },
                    )
                    CampaignAccounting(self.db).finish_task_reservation(
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        terminal_status="BUDGET_EXCEEDED",
                    )
                else:
                    self.db.execute(
                        "UPDATE runtime.tasks SET status='RETRY_PENDING',updated_at=now(),run_after=now()+(%s * interval '1 second') WHERE task_id=%s",
                        (retry_delay_seconds, row["task_id"]),
                    )
                    self._event(
                        event_type="LEASE_EXPIRED_RECONCILED",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={
                            "stale_epoch": row["active_epoch"],
                            "new_state": "RETRY_PENDING",
                        },
                    )
                    self._event(
                        event_type="TASK_RETRY_PENDING",
                        actor_id="reconciler",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={"reason": "lease expired"},
                    )
                recovered.append(row["task_id"])
        return recovered

    def reconcile_orphaned_attempts(self, *, batch_size: int = 100) -> list[str]:
        """Fence attempts whose task no longer has their current live lease."""
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 500:
            raise ValueError("orphan reconciliation batch must be between 1 and 500")
        states = [
            "LEASED",
            "CONTEXT_VALIDATED",
            "RUNNING",
            "WAITING_CHILDREN",
            "WAITING_TOOL",
            "WAITING_IO",
            "CHECKPOINTED",
        ]
        reconciled: list[str] = []
        with self.db.transaction():
            rows = self.db.execute(
                """SELECT a.attempt_id,a.task_id,a.lease_epoch,
                    t.campaign_id,t.goal_id,t.max_attempts,
                    (SELECT count(*) FROM runtime.attempts used WHERE used.task_id=t.task_id) AS attempt_count
                FROM runtime.attempts a JOIN runtime.tasks t USING(task_id)
                WHERE a.status=ANY(%s) AND t.status IN ('LEASED','RUNNING')
                  AND NOT EXISTS (SELECT 1 FROM runtime.leases l
                    WHERE l.task_id=t.task_id AND l.attempt_id=a.attempt_id
                      AND l.lease_epoch=a.lease_epoch AND l.status='ACTIVE' AND l.lease_until>now())
                ORDER BY a.started_at,a.attempt_id LIMIT %s
                FOR UPDATE OF a,t SKIP LOCKED""",
                (states, batch_size),
            ).fetchall()
            for row in rows:
                self.db.execute(
                    "UPDATE runtime.attempts SET status='ABANDONED',completed_at=now() WHERE attempt_id=%s",
                    (row["attempt_id"],),
                )
                fenced = self.db.execute(
                    """UPDATE runtime.leases SET status='EXPIRED',
                    lease_until=LEAST(lease_until,now()),updated_at=now()
                    WHERE task_id=%s AND attempt_id=%s AND lease_epoch=%s""",
                    (row["task_id"], row["attempt_id"], row["lease_epoch"]),
                )
                if fenced.rowcount == 0:
                    self.db.execute(
                        """INSERT INTO runtime.leases
                        (task_id,worker_id,attempt_id,lease_epoch,lease_until,last_heartbeat,status)
                        SELECT t.task_id,a.worker_id,a.attempt_id,a.lease_epoch,now(),now(),'EXPIRED'
                        FROM runtime.tasks t JOIN runtime.attempts a USING(task_id)
                        WHERE t.task_id=%s AND a.attempt_id=%s AND a.lease_epoch=%s""",
                        (row["task_id"], row["attempt_id"], row["lease_epoch"]),
                    )
                max_attempts = row["max_attempts"]
                exhausted = (
                    isinstance(max_attempts, int)
                    and not isinstance(max_attempts, bool)
                    and max_attempts > 0
                    and row["attempt_count"] >= max_attempts
                )
                target = "BUDGET_EXCEEDED" if exhausted else "RETRY_PENDING"
                self.db.execute(
                    "UPDATE runtime.tasks SET status=%s,updated_at=now() WHERE task_id=%s",
                    (target, row["task_id"]),
                )
                self._event(
                    event_type=f"TASK_{target}",
                    actor_id="coordinator",
                    correlation_id=row["task_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    attempt_id=row["attempt_id"],
                    payload={"reason": "orphan attempt reconciliation"},
                )
                if exhausted:
                    self._event(
                        event_type="TASK_ATTEMPT_BUDGET_EXCEEDED",
                        actor_id="coordinator",
                        correlation_id=row["task_id"],
                        campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        payload={
                            "max_attempts": max_attempts,
                            "attempt_count": row["attempt_count"],
                            "orphan_attempt_reconciled": True,
                        },
                    )
                self._event(
                    event_type="ORPHAN_ATTEMPT_RECONCILED",
                    actor_id="coordinator",
                    correlation_id=row["task_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    attempt_id=row["attempt_id"],
                    payload={
                        "orphan_attempt_id": row["attempt_id"],
                        "stale_lease_epoch": row["lease_epoch"],
                        "new_task_state": target,
                        "reason": "in-flight attempt has no matching current lease",
                    },
                )
                if exhausted:
                    CampaignAccounting(self.db).finish_task_reservation(
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        terminal_status="BUDGET_EXCEEDED",
                    )
                reconciled.append(row["task_id"])
        return reconciled

    def reconcile_impossible_joins(self, *, batch_size: int = 100) -> list[str]:
        """Cancel bounded queued graph nodes whose required input became impossible."""
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 500:
            raise ValueError("impossible-join reconciliation batch must be between 1 and 500")
        cancelled = []
        terminal_failure = (
            "REJECTED",
            "NEEDS_REVIEW",
            "FAILED_TRANSIENT",
            "FAILED_PERMANENT",
            "CANCELLED",
            "QUARANTINED",
            "BUDGET_EXCEEDED",
        )
        with self.db.transaction():
            rows = self.db.execute(
                """SELECT t.task_id,t.campaign_id,t.goal_id,
                    t.plan_version_id,
                    (SELECT d.depends_on_task_id FROM runtime.task_dependencies d
                     JOIN runtime.tasks parent ON parent.task_id=d.depends_on_task_id
                     WHERE d.task_id=t.task_id
                       AND d.dependency_type IN ('REQUIRES_ACCEPTED','REQUIRES_ARTIFACT')
                       AND parent.status=ANY(%s)
                     ORDER BY d.depends_on_task_id LIMIT 1) AS depends_on_task_id,
                    (SELECT d.dependency_type FROM runtime.task_dependencies d
                     JOIN runtime.tasks parent ON parent.task_id=d.depends_on_task_id
                     WHERE d.task_id=t.task_id
                       AND d.dependency_type IN ('REQUIRES_ACCEPTED','REQUIRES_ARTIFACT')
                       AND parent.status=ANY(%s)
                     ORDER BY d.depends_on_task_id LIMIT 1) AS dependency_type,
                    (SELECT parent.status FROM runtime.task_dependencies d
                     JOIN runtime.tasks parent ON parent.task_id=d.depends_on_task_id
                     WHERE d.task_id=t.task_id
                       AND d.dependency_type IN ('REQUIRES_ACCEPTED','REQUIRES_ARTIFACT')
                       AND parent.status=ANY(%s)
                     ORDER BY d.depends_on_task_id LIMIT 1) AS prerequisite_status
                FROM runtime.tasks t
                JOIN runtime.coordination_plan_nodes n ON n.task_id=t.task_id
                WHERE t.status IN ('QUEUED','RETRY_PENDING')
                  AND EXISTS (SELECT 1 FROM runtime.task_dependencies d
                    JOIN runtime.tasks parent ON parent.task_id=d.depends_on_task_id
                    WHERE d.task_id=t.task_id
                      AND d.dependency_type IN ('REQUIRES_ACCEPTED','REQUIRES_ARTIFACT')
                      AND parent.status=ANY(%s))
                ORDER BY t.task_id LIMIT %s FOR UPDATE OF t SKIP LOCKED""",
                (
                    list(terminal_failure),
                    list(terminal_failure),
                    list(terminal_failure),
                    list(terminal_failure),
                    batch_size,
                ),
            ).fetchall()
            for row in rows:
                changed = self.db.execute(
                    """UPDATE runtime.tasks SET status='CANCELLED',updated_at=now()
                    WHERE task_id=%s AND status IN ('QUEUED','RETRY_PENDING') RETURNING task_id""",
                    (row["task_id"],),
                ).fetchone()
                if not changed:
                    continue
                CampaignAccounting(self.db).finish_task_reservation(
                    task_id=row["task_id"], attempt_id=None, terminal_status="CANCELLED"
                )
                delegation = self.db.execute(
                    """UPDATE runtime.coordination_delegations
                    SET status='CANCELLED',updated_at=now() WHERE child_task_id=%s
                      AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED') RETURNING delegation_id""",
                    (row["task_id"],),
                ).fetchone()
                self._event(
                    event_type="M9_NODE_CANCELLED_IMPOSSIBLE_DEPENDENCY",
                    actor_id="coordinator",
                    correlation_id=row["task_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    payload={
                        "plan_version_id": row["plan_version_id"],
                        "failed_dependency_task_id": row["depends_on_task_id"],
                        "dependency_type": row["dependency_type"],
                        "dependency_terminal_status": row["prerequisite_status"],
                        "delegation_id": delegation["delegation_id"] if delegation else None,
                    },
                )
                cancelled.append(row["task_id"])
        return cancelled

    def reconcile_abandoned_delegations(self, *, batch_size: int = 100) -> list[str]:
        """Cancel a current plan when failed parent authority leaves live child work."""
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 500:
            raise ValueError("abandoned-delegation reconciliation batch must be between 1 and 500")
        rows = self.db.execute(
            """SELECT DISTINCT d.plan_version_id
            FROM runtime.coordination_delegations d
            JOIN runtime.tasks parent ON parent.task_id=d.parent_task_id
            JOIN runtime.tasks child ON child.task_id=d.child_task_id
            JOIN runtime.coordination_plan_versions p ON p.plan_version_id=d.plan_version_id
            JOIN runtime.goals g ON g.goal_id=p.goal_id
            WHERE parent.status IN ('REJECTED','NEEDS_REVIEW','FAILED_TRANSIENT',
                'FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED')
              AND child.status NOT IN ('ACCEPTED','REJECTED','NEEDS_REVIEW',
                'FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED')
              AND p.status='ACCEPTED' AND g.status='ACTIVE'
              AND NOT EXISTS (SELECT 1 FROM runtime.coordination_plan_versions newer
                WHERE newer.plan_id=p.plan_id AND newer.version>p.version
                  AND newer.status='ACCEPTED')
              AND (child.status IN ('DRAFT','QUEUED','RETRY_PENDING','BLOCKED')
                OR NOT EXISTS (SELECT 1 FROM runtime.worker_commands wc
                  JOIN runtime.leases l ON l.task_id=child.task_id
                  WHERE wc.task_id=child.task_id AND wc.attempt_id=l.attempt_id
                    AND wc.lease_epoch=l.lease_epoch AND wc.command='CANCEL_ATTEMPT'
                    AND wc.status IN ('PENDING','DELIVERED','ACKNOWLEDGED')))
            ORDER BY d.plan_version_id LIMIT %s""",
            (batch_size,),
        ).fetchall()
        from agentic_runtime.coordinator.coordination import CoordinationService

        reconciled = []
        for row in rows:
            try:
                CoordinationService(self.db).request_plan_cancellation(
                    row["plan_version_id"],
                    requested_by="m9-abandoned-delegation-reconciler",
                )
            except ValueError as exc:
                if str(exc) != "only the current accepted plan version can be cancelled":
                    raise
                continue
            reconciled.append(row["plan_version_id"])
        return reconciled

    def reconcile_expired_plan_deadlines(self, *, batch_size: int = 100) -> list[str]:
        """Bound unavailable queued graph work by its accepted mission deadline."""
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 500:
            raise ValueError("plan deadline reconciliation batch must be between 1 and 500")
        expired = []
        with self.db.transaction():
            plans = self.db.execute(
                """SELECT p.plan_version_id,p.plan_id,p.goal_id,p.limits,
                    g.campaign_id,g.status AS goal_status,
                    COALESCE((SELECT min(first.accepted_at) FROM runtime.coordination_plan_versions first
                      WHERE first.plan_id=p.plan_id AND first.status='ACCEPTED'),p.accepted_at)
                      + ((p.limits->>'max_wall_time_seconds')::integer * interval '1 second') AS deadline
                FROM runtime.coordination_plan_versions p JOIN runtime.goals g USING(goal_id)
                WHERE p.status='ACCEPTED' AND g.status='ACTIVE'
                  AND p.limits->>'max_wall_time_seconds' ~ '^[0-9]+$'
                  AND COALESCE((SELECT min(first.accepted_at) FROM runtime.coordination_plan_versions first
                    WHERE first.plan_id=p.plan_id AND first.status='ACCEPTED'),p.accepted_at)
                    + ((p.limits->>'max_wall_time_seconds')::integer * interval '1 second') <= clock_timestamp()
                  AND NOT EXISTS (SELECT 1 FROM runtime.coordination_plan_versions newer
                    WHERE newer.plan_id=p.plan_id AND newer.version>p.version AND newer.status='ACCEPTED')
                  AND EXISTS (SELECT 1 FROM runtime.coordination_plan_nodes n
                    JOIN runtime.tasks t USING(task_id) WHERE n.plan_version_id=p.plan_version_id
                      AND t.status IN ('QUEUED','RETRY_PENDING'))
                  AND NOT EXISTS (SELECT 1 FROM runtime.coordination_plan_nodes n
                    JOIN runtime.tasks t USING(task_id) WHERE n.plan_version_id=p.plan_version_id
                      AND t.status NOT IN ('QUEUED','RETRY_PENDING','ACCEPTED','REJECTED','NEEDS_REVIEW',
                        'FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED'))
                ORDER BY p.plan_id,p.version LIMIT %s FOR UPDATE OF p,g SKIP LOCKED""",
                (batch_size,),
            ).fetchall()
            for plan in plans:
                waiting = self.db.execute(
                    """SELECT t.task_id,t.goal_id FROM runtime.coordination_plan_nodes n
                    JOIN runtime.tasks t USING(task_id) WHERE n.plan_version_id=%s
                      AND t.status IN ('QUEUED','RETRY_PENDING') FOR UPDATE OF t""",
                    (plan["plan_version_id"],),
                ).fetchall()
                for task in waiting:
                    self.db.execute(
                        """UPDATE runtime.tasks SET status='BUDGET_EXCEEDED',updated_at=now()
                        WHERE task_id=%s""",
                        (task["task_id"],),
                    )
                    CampaignAccounting(self.db).finish_task_reservation(
                        task_id=task["task_id"],
                        attempt_id=None,
                        terminal_status="BUDGET_EXCEEDED",
                    )
                    delegation = self.db.execute(
                        """UPDATE runtime.coordination_delegations
                        SET status='CANCELLED',updated_at=now() WHERE child_task_id=%s
                          AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED') RETURNING delegation_id""",
                        (task["task_id"],),
                    ).fetchone()
                    self._event(
                        event_type="M9_PLAN_DEADLINE_NODE_EXCEEDED",
                        actor_id="coordinator",
                        correlation_id=task["task_id"],
                        campaign_id=plan["campaign_id"],
                        goal_id=task["goal_id"],
                        task_id=task["task_id"],
                        payload={
                            "plan_id": plan["plan_id"],
                            "plan_version_id": plan["plan_version_id"],
                            "deadline": plan["deadline"].isoformat(),
                            "delegation_id": delegation["delegation_id"] if delegation else None,
                            "reason": "accepted plan deadline elapsed before required work became dispatchable",
                        },
                    )
                self.db.execute(
                    "UPDATE runtime.goals SET status='BLOCKED' WHERE goal_id=%s AND status='ACTIVE'",
                    (plan["goal_id"],),
                )
                self._event(
                    event_type="M9_PLAN_DEADLINE_EXCEEDED",
                    actor_id="coordinator",
                    correlation_id=plan["plan_version_id"],
                    campaign_id=plan["campaign_id"],
                    goal_id=plan["goal_id"],
                    payload={
                        "plan_id": plan["plan_id"],
                        "plan_version_id": plan["plan_version_id"],
                        "deadline": plan["deadline"].isoformat(),
                        "budget_exceeded_tasks": [task["task_id"] for task in waiting],
                        "disposition": "GOAL_BLOCKED",
                    },
                )
                expired.append(plan["plan_version_id"])
        return expired

    def reconcile_runtime(
        self, *, scan_id: str | None = None, retry_delay_seconds: int = 0
    ) -> dict[str, Any]:
        """Recover expired leases and durably report remaining startup anomalies."""
        recovered = self.reconcile_expired_leases(retry_delay_seconds=retry_delay_seconds)
        orphan_attempts = self.reconcile_orphaned_attempts()
        abandoned_delegations = self.reconcile_abandoned_delegations()
        impossible_joins = self.reconcile_impossible_joins()
        expired_plans = self.reconcile_expired_plan_deadlines()
        exhausted: list[str] = []
        with self.db.transaction():
            rows = self.db.execute("""SELECT t.task_id,t.campaign_id,t.goal_id,t.max_attempts
                FROM runtime.tasks t
                WHERE t.status IN ('QUEUED','RETRY_PENDING')
                  AND t.max_attempts IS NOT NULL
                  AND (SELECT count(*) FROM runtime.attempts a WHERE a.task_id=t.task_id)
                      >= t.max_attempts
                FOR UPDATE OF t SKIP LOCKED""").fetchall()
            for row in rows:
                cap = int(row["max_attempts"])
                count = self.db.execute(
                    "SELECT count(*) AS n FROM runtime.attempts WHERE task_id=%s",
                    (row["task_id"],),
                ).fetchone()["n"]
                self.db.execute(
                    "UPDATE runtime.tasks SET status='BUDGET_EXCEEDED',updated_at=now() WHERE task_id=%s",
                    (row["task_id"],),
                )
                self._event(
                    event_type="TASK_BUDGET_EXCEEDED",
                    actor_id="reconciler",
                    correlation_id=row["task_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    payload={"reason": "maximum task attempts exhausted"},
                )
                self._event(
                    event_type="TASK_ATTEMPT_BUDGET_EXCEEDED",
                    actor_id="reconciler",
                    correlation_id=row["task_id"],
                    campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],
                    task_id=row["task_id"],
                    payload={
                        "max_attempts": cap,
                        "attempt_count": count,
                        "recovered_dispatchable_task": True,
                    },
                )
                CampaignAccounting(self.db).finish_task_reservation(
                    task_id=row["task_id"],
                    attempt_id=None,
                    terminal_status="BUDGET_EXCEEDED",
                )
                exhausted.append(row["task_id"])
        # Campaign reservations are a second durable ledger. Reconcile it from
        # persisted dispatch/task/attempt state on the same startup recovery pass.
        accounting_recovery = CampaignAccounting(self.db).reconcile()
        completed_campaigns = self.reconcile_exhausted_campaigns()
        scan_id = scan_id or _id("reconcile")
        with self.db.transaction():
            stale_invocations = self.db.execute("""UPDATE runtime.cognitive_invocations ci
                SET status='STALE',completed_at=now(),error_class='LEASE_OR_WORKER_AUTHORITY_LOST'
                WHERE ci.status='PENDING' AND NOT EXISTS (
                    SELECT 1 FROM runtime.leases l
                    JOIN runtime.attempts a USING(attempt_id)
                    JOIN runtime.worker_instances wi ON wi.worker_instance_id=a.worker_instance_id
                    JOIN runtime.worker_identities w ON w.worker_id=ci.worker_id
                    WHERE l.task_id=ci.task_id AND l.attempt_id=ci.attempt_id
                      AND l.lease_epoch=ci.lease_epoch AND l.worker_id=ci.worker_id
                      AND l.status='ACTIVE' AND l.lease_until>now()
                      AND wi.worker_instance_id=ci.worker_instance_id
                      AND wi.status IN ('READY','BUSY','DRAINING') AND w.status='ACTIVE'
                ) RETURNING invocation_id,task_id,campaign_id,attempt_id,worker_id,lease_epoch""").fetchall()
            for invocation in stale_invocations:
                self._event(
                    event_type="COGNITIVE_INVOCATION_STALE_RECONCILED",
                    actor_id="coordinator",
                    correlation_id=invocation["invocation_id"],
                    campaign_id=invocation["campaign_id"],
                    task_id=invocation["task_id"],
                    attempt_id=invocation["attempt_id"],
                    payload={
                        "invocation_id": invocation["invocation_id"],
                        "worker_id": invocation["worker_id"],
                        "lease_epoch": invocation["lease_epoch"],
                        "reason": "lease, worker instance, or worker identity is no longer authoritative",
                    },
                )
            stale_busy_workers = self.db.execute("""SELECT w.worker_id FROM runtime.workers w
                WHERE w.status='BUSY' AND NOT EXISTS (
                    SELECT 1 FROM runtime.leases l WHERE l.worker_id=w.worker_id
                      AND l.status='ACTIVE' AND l.lease_until>now())
                FOR UPDATE OF w SKIP LOCKED""").fetchall()
            for worker in stale_busy_workers:
                self.db.execute(
                    "UPDATE runtime.workers SET status='IDLE',updated_at=now() WHERE worker_id=%s",
                    (worker["worker_id"],),
                )
                self._event(
                    event_type="WORKER_IDLE_RECONCILED",
                    actor_id="coordinator",
                    correlation_id=scan_id,
                    payload={
                        "worker_id": worker["worker_id"],
                        "reason": "no live lease exists for BUSY worker",
                    },
                )
            counts = self.db.execute("""SELECT
                (SELECT count(*) FROM runtime.tasks t LEFT JOIN runtime.leases l USING(task_id)
                 WHERE t.status IN ('LEASED','CONTEXT_VALIDATED','RUNNING','RESULT_COMMITTED')
                   AND (l.task_id IS NULL OR l.status<>'ACTIVE' OR l.attempt_id IS NULL
                        OR l.lease_epoch<>t.lease_epoch)) AS tasks_without_current_lease,
                (SELECT count(*) FROM runtime.attempts a LEFT JOIN runtime.leases l
                 ON l.task_id=a.task_id AND l.attempt_id=a.attempt_id AND l.lease_epoch=a.lease_epoch
                   AND l.status='ACTIVE'
                 WHERE a.status IN ('LEASED','CONTEXT_VALIDATED','RUNNING','WAITING_CHILDREN',
                                    'WAITING_TOOL','WAITING_IO','CHECKPOINTED')
                   AND l.task_id IS NULL) AS orphaned_attempts,
                (SELECT count(*) FROM runtime.outbox WHERE delivered_at IS NULL) AS pending_outbox,
                (SELECT count(*) FROM runtime.sandboxes
                 WHERE status IN ('CLEANUP_PENDING','CLEANUP_FAILED')) AS incomplete_sandbox_cleanup""").fetchone()
            self._event(
                event_type="RUNTIME_RECONCILIATION_COMPLETED",
                actor_id="coordinator",
                correlation_id=scan_id,
                payload={
                    "expired_leases_recovered": recovered,
                    "orphan_attempts_reconciled": orphan_attempts,
                    "abandoned_m9_delegations_reconciled": abandoned_delegations,
                    "impossible_m9_joins_cancelled": impossible_joins,
                    "expired_m9_plans_blocked": expired_plans,
                    "exhausted_campaigns_completed": completed_campaigns,
                    "campaign_reservations": accounting_recovery,
                    "attempt_budget_exhausted_tasks": exhausted,
                    "stale_cognitive_invocations": [
                        row["invocation_id"] for row in stale_invocations
                    ],
                    "stale_busy_workers_reconciled": [
                        worker["worker_id"] for worker in stale_busy_workers
                    ],
                    "tasks_without_current_lease": counts["tasks_without_current_lease"],
                    "orphaned_attempts": counts["orphaned_attempts"],
                    "pending_outbox": counts["pending_outbox"],
                    "incomplete_sandbox_cleanup": counts["incomplete_sandbox_cleanup"],
                },
            )
            return {
                "scan_id": scan_id,
                "expired_leases_recovered": recovered,
                "orphan_attempts_reconciled": orphan_attempts,
                "abandoned_m9_delegations_reconciled": abandoned_delegations,
                "impossible_m9_joins_cancelled": impossible_joins,
                "expired_m9_plans_blocked": expired_plans,
                "exhausted_campaigns_completed": completed_campaigns,
                "campaign_reservations": accounting_recovery,
                "attempt_budget_exhausted_tasks": exhausted,
                "stale_cognitive_invocations": [row["invocation_id"] for row in stale_invocations],
                "stale_busy_workers_reconciled": [
                    worker["worker_id"] for worker in stale_busy_workers
                ],
                **{
                    key: counts[key]
                    for key in (
                        "tasks_without_current_lease",
                        "orphaned_attempts",
                        "pending_outbox",
                        "incomplete_sandbox_cleanup",
                    )
                },
            }

    def reconcile_exhausted_campaigns(self) -> int:
        """Complete bounded campaigns only after declared exhaustion and quiescence.

        An empty queue alone is not terminal because new tasks may still be
        added. Completion requires a configured cognitive-call or wall-time
        cap to be exhausted, existing task history, only terminal tasks, and
        no pending invocations, active leases, reservations, or outbox work.
        The shared advisory lock serializes this with cognitive call booking;
        the campaign row lock serializes it with task creation.
        """
        candidates = self.db.execute("""SELECT campaign_id FROM runtime.campaigns c
            WHERE c.status='ACTIVE' AND (
              CASE WHEN c.budget->>'max_cognitive_calls' ~ '^[0-9]+$'
                THEN (SELECT count(*) FROM runtime.cognitive_invocations ci
                      WHERE ci.campaign_id=c.campaign_id) >= (c.budget->>'max_cognitive_calls')::integer
                ELSE false END
              OR CASE WHEN c.budget->>'max_wall_time_seconds' ~ '^[0-9]+$'
                THEN c.created_at + ((c.budget->>'max_wall_time_seconds')::integer * interval '1 second') <= now()
                ELSE false END)
            ORDER BY c.created_at,c.campaign_id""").fetchall()
        completed = 0
        for candidate in candidates:
            campaign_id = candidate["campaign_id"]
            with self.db.transaction():
                self.db.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"cognitive-campaign:{campaign_id}",),
                )
                campaign = self.db.execute(
                    "SELECT status,budget,created_at FROM runtime.campaigns WHERE campaign_id=%s FOR UPDATE",
                    (campaign_id,),
                ).fetchone()
                if not campaign or campaign["status"] != "ACTIVE":
                    continue
                ready = self.db.execute(
                    """SELECT
                    EXISTS(SELECT 1 FROM runtime.tasks WHERE campaign_id=c.campaign_id) AS has_tasks,
                    NOT EXISTS(SELECT 1 FROM runtime.tasks WHERE campaign_id=c.campaign_id AND status NOT IN
                      ('ACCEPTED','REJECTED','NEEDS_REVIEW','FAILED_TRANSIENT','FAILED_PERMANENT',
                       'CANCELLED','QUARANTINED','BUDGET_EXCEEDED')) AS tasks_terminal,
                    NOT EXISTS(SELECT 1 FROM runtime.cognitive_invocations WHERE campaign_id=c.campaign_id AND status='PENDING') AS no_pending_invocations,
                    NOT EXISTS(SELECT 1 FROM runtime.leases l JOIN runtime.tasks t USING(task_id)
                      WHERE t.campaign_id=c.campaign_id AND l.status='ACTIVE') AS no_active_leases,
                    NOT EXISTS(SELECT 1 FROM runtime.improvement_reservations
                      WHERE campaign_id=c.campaign_id AND status='RESERVED') AS no_reservations,
                    NOT EXISTS(SELECT 1 FROM runtime.outbox o JOIN runtime.events e USING(event_id)
                      WHERE e.campaign_id=c.campaign_id AND o.delivered_at IS NULL) AS no_pending_outbox,
                    (CASE WHEN c.budget->>'max_cognitive_calls' ~ '^[0-9]+$'
                       THEN (SELECT count(*) FROM runtime.cognitive_invocations WHERE campaign_id=c.campaign_id)
                            >= (c.budget->>'max_cognitive_calls')::integer ELSE false END
                     OR CASE WHEN c.budget->>'max_wall_time_seconds' ~ '^[0-9]+$'
                       THEN c.created_at + ((c.budget->>'max_wall_time_seconds')::integer * interval '1 second') <= now()
                       ELSE false END) AS budget_exhausted
                    FROM runtime.campaigns c WHERE c.campaign_id=%s""",
                    (campaign_id,),
                ).fetchone()
                if not all(
                    ready[key]
                    for key in (
                        "has_tasks",
                        "tasks_terminal",
                        "no_pending_invocations",
                        "no_active_leases",
                        "no_reservations",
                        "no_pending_outbox",
                        "budget_exhausted",
                    )
                ):
                    continue
                changed = self.db.execute(
                    """UPDATE runtime.campaigns SET status='COMPLETED'
                    WHERE campaign_id=%s AND status='ACTIVE' RETURNING campaign_id""",
                    (campaign_id,),
                ).fetchone()
                if changed:
                    self._event(
                        event_type="CAMPAIGN_COMPLETED",
                        actor_id="campaign-reconciler",
                        correlation_id=campaign_id,
                        campaign_id=campaign_id,
                        payload={
                            "reason": "declared budget exhausted and durable work terminal",
                            "budget": campaign["budget"],
                        },
                    )
                    completed += 1
        return completed
