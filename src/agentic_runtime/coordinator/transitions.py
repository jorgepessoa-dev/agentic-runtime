"""Fenced task state transitions and worker lease heartbeats."""

from __future__ import annotations

from typing import Any, Mapping

from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.coordinator.state import TaskState, require_transition


class TaskTransitionService:
    def __init__(self, connection: Any, emit_event: Any) -> None:
        self.db = connection
        self._event = emit_event

    def transition(
        self,
        task_id: str,
        attempt_id: str,
        lease_epoch: int,
        target: TaskState,
        *,
        actor_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        with self.db.transaction():
            row = self.db.execute(
                """SELECT t.status,t.campaign_id,t.goal_id,t.lease_epoch,
                l.attempt_id,l.lease_epoch AS active_epoch,l.status AS lease_status
                FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE t.task_id=%s FOR UPDATE OF t,l""",
                (task_id,),
            ).fetchone()
            if (
                not row
                or row["lease_status"] != "ACTIVE"
                or row["attempt_id"] != attempt_id
                or row["lease_epoch"] != lease_epoch
                or row["active_epoch"] != lease_epoch
            ):
                raise ValueError("stale or missing lease fencing token")
            require_transition(row["status"], target)
            self.db.execute(
                "UPDATE runtime.tasks SET status=%s,updated_at=now() WHERE task_id=%s",
                (target.value, task_id),
            )
            self.db.execute(
                "UPDATE runtime.attempts SET status=%s,completed_at=CASE WHEN %s THEN now() ELSE completed_at END WHERE attempt_id=%s",
                (
                    target.value,
                    target
                    in {
                        TaskState.ACCEPTED,
                        TaskState.REJECTED,
                        TaskState.NEEDS_REVIEW,
                        TaskState.FAILED_TRANSIENT,
                        TaskState.FAILED_PERMANENT,
                        TaskState.CANCELLED,
                        TaskState.QUARANTINED,
                        TaskState.BUDGET_EXCEEDED,
                    },
                    attempt_id,
                ),
            )
            terminal_statuses = {
                TaskState.ACCEPTED,
                TaskState.REJECTED,
                TaskState.NEEDS_REVIEW,
                TaskState.FAILED_TRANSIENT,
                TaskState.FAILED_PERMANENT,
                TaskState.CANCELLED,
                TaskState.QUARANTINED,
                TaskState.BUDGET_EXCEEDED,
            }
            if target in terminal_statuses:
                # Terminal task decisions retire the fencing lease in the same
                # transaction. Leaving it ACTIVE lets an expired-lease sweep
                # incorrectly resurrect CANCELLED or otherwise terminal work.
                self.db.execute(
                    "UPDATE runtime.leases SET status='RELEASED',updated_at=now() WHERE task_id=%s AND attempt_id=%s AND lease_epoch=%s",
                    (task_id, attempt_id, lease_epoch),
                )
                # Retire a stale logical BUSY state with the terminal lease.
                # Preserve DRAINING and do not mark idle while another live
                # attempt still belongs to this worker.
                self.db.execute(
                    """UPDATE runtime.workers w SET status='IDLE',updated_at=now()
                    WHERE w.worker_id=(SELECT worker_id FROM runtime.attempts WHERE attempt_id=%s)
                      AND w.status='BUSY'
                      AND NOT EXISTS (SELECT 1 FROM runtime.leases l WHERE l.worker_id=w.worker_id
                          AND l.status='ACTIVE' AND l.lease_until>now())""",
                    (attempt_id,),
                )
                mapped = (
                    "CANCELLED"
                    if target == TaskState.CANCELLED
                    else "QUARANTINED"
                    if target == TaskState.QUARANTINED
                    else "FAILED"
                    if target
                    in {
                        TaskState.REJECTED,
                        TaskState.NEEDS_REVIEW,
                        TaskState.FAILED_TRANSIENT,
                        TaskState.FAILED_PERMANENT,
                        TaskState.BUDGET_EXCEEDED,
                    }
                    else "SUCCEEDED"
                )
                CampaignAccounting(self.db).finish_task_reservation(
                    task_id=task_id, attempt_id=attempt_id, terminal_status=mapped
                )
                if target in {
                    TaskState.CANCELLED,
                    TaskState.REJECTED,
                    TaskState.FAILED_TRANSIENT,
                    TaskState.FAILED_PERMANENT,
                    TaskState.BUDGET_EXCEEDED,
                    TaskState.QUARANTINED,
                }:
                    delegation_status = "CANCELLED" if target == TaskState.CANCELLED else "REJECTED"
                    delegation = self.db.execute(
                        """UPDATE runtime.coordination_delegations
                        SET status=%s,updated_at=now() WHERE child_task_id=%s
                        AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED')
                        RETURNING delegation_id""",
                        (delegation_status, task_id),
                    ).fetchone()
                    if delegation:
                        self._event(
                            event_type=f"DELEGATION_{delegation_status}",
                            actor_id=actor_id,
                            correlation_id=delegation["delegation_id"],
                            campaign_id=row["campaign_id"],
                            goal_id=row["goal_id"],
                            task_id=task_id,
                            attempt_id=attempt_id,
                            payload={
                                "delegation_id": delegation["delegation_id"],
                                "terminal_task_state": target.value,
                            },
                        )
            self._event(
                event_type=f"TASK_{target.value}",
                actor_id=actor_id,
                correlation_id=task_id,
                campaign_id=row["campaign_id"],
                goal_id=row["goal_id"],
                task_id=task_id,
                attempt_id=attempt_id,
                payload=payload,
            )

    def heartbeat(
        self,
        task_id: str,
        worker_id: str,
        attempt_id: str,
        lease_epoch: int,
        *,
        lease_seconds: int = 60,
    ) -> bool:
        with self.db.transaction():
            row = self.db.execute(
                """UPDATE runtime.leases SET last_heartbeat=now(),
                lease_until=now()+(%s * interval '1 second'),updated_at=now()
                WHERE task_id=%s AND worker_id=%s AND attempt_id=%s AND lease_epoch=%s
                  AND status='ACTIVE' AND lease_until>now() RETURNING task_id""",
                (lease_seconds, task_id, worker_id, attempt_id, lease_epoch),
            ).fetchone()
            if row:
                self.db.execute(
                    "UPDATE runtime.workers SET last_heartbeat=now(),updated_at=now() WHERE worker_id=%s",
                    (worker_id,),
                )
            return row is not None
