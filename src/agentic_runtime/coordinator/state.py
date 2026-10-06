from __future__ import annotations

from enum import StrEnum


class TaskState(StrEnum):
    DRAFT = "DRAFT"
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    CONTEXT_VALIDATED = "CONTEXT_VALIDATED"
    RUNNING = "RUNNING"
    WAITING_CHILDREN = "WAITING_CHILDREN"
    WAITING_TOOL = "WAITING_TOOL"
    WAITING_IO = "WAITING_IO"
    BLOCKED = "BLOCKED"
    CHECKPOINTED = "CHECKPOINTED"
    RESULT_COMMITTED = "RESULT_COMMITTED"
    VERIFIED = "VERIFIED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    FAILED_TRANSIENT = "FAILED_TRANSIENT"
    FAILED_PERMANENT = "FAILED_PERMANENT"
    CANCELLED = "CANCELLED"
    QUARANTINED = "QUARANTINED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    RETRY_PENDING = "RETRY_PENDING"


TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.DRAFT: frozenset({TaskState.QUEUED, TaskState.CANCELLED}),
    TaskState.QUEUED: frozenset({TaskState.LEASED, TaskState.CANCELLED, TaskState.BUDGET_EXCEEDED}),
    TaskState.RETRY_PENDING: frozenset({TaskState.LEASED, TaskState.CANCELLED, TaskState.BUDGET_EXCEEDED}),
    TaskState.LEASED: frozenset({TaskState.CONTEXT_VALIDATED, TaskState.RUNNING, TaskState.CANCELLED,
                                TaskState.FAILED_TRANSIENT, TaskState.FAILED_PERMANENT,
                                TaskState.RETRY_PENDING}),
    TaskState.CONTEXT_VALIDATED: frozenset({TaskState.RUNNING, TaskState.CANCELLED,
                                            TaskState.FAILED_TRANSIENT, TaskState.FAILED_PERMANENT}),
    TaskState.RUNNING: frozenset({TaskState.WAITING_CHILDREN, TaskState.WAITING_TOOL, TaskState.WAITING_IO,
                                  TaskState.BLOCKED, TaskState.CHECKPOINTED, TaskState.RESULT_COMMITTED,
                                  TaskState.NEEDS_REVIEW, TaskState.FAILED_TRANSIENT,
                                  TaskState.FAILED_PERMANENT, TaskState.CANCELLED,
                                  TaskState.BUDGET_EXCEEDED, TaskState.QUARANTINED}),
    TaskState.WAITING_CHILDREN: frozenset({TaskState.QUEUED, TaskState.RUNNING, TaskState.BLOCKED,
                                           TaskState.CANCELLED}),
    TaskState.WAITING_TOOL: frozenset({TaskState.RUNNING, TaskState.BLOCKED, TaskState.CANCELLED}),
    TaskState.WAITING_IO: frozenset({TaskState.RUNNING, TaskState.BLOCKED, TaskState.CANCELLED}),
    TaskState.BLOCKED: frozenset({TaskState.QUEUED, TaskState.CANCELLED}),
    TaskState.CHECKPOINTED: frozenset({TaskState.QUEUED, TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RESULT_COMMITTED: frozenset({TaskState.VERIFIED, TaskState.QUARANTINED, TaskState.NEEDS_REVIEW}),
    TaskState.VERIFIED: frozenset({TaskState.ACCEPTED, TaskState.REJECTED, TaskState.NEEDS_REVIEW}),
    TaskState.NEEDS_REVIEW: frozenset({TaskState.ACCEPTED, TaskState.REJECTED, TaskState.QUARANTINED}),
}


def require_transition(current: str | TaskState, target: str | TaskState) -> None:
    source, destination = TaskState(current), TaskState(target)
    if destination not in TRANSITIONS.get(source, frozenset()):
        raise ValueError(f"invalid task state transition: {source.value} -> {destination.value}")
