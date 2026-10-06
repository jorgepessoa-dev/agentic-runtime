from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Protocol


class InvocationStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    UNAVAILABLE = "UNAVAILABLE"
    MALFORMED = "MALFORMED"
    STALE = "STALE"


class UsageState(StrEnum):
    MEASURED = "MEASURED"
    PROVIDER_REPORTED = "PROVIDER_REPORTED"
    SUBSCRIPTION_UNPRICED = "SUBSCRIPTION_UNPRICED"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class CognitiveInvocationRequest:
    invocation_id: str
    task_id: str
    attempt_id: str
    lease_epoch: int
    campaign_id: str | None
    logical_role: str
    purpose: str
    instruction: str
    context_refs: tuple[str, ...]
    response_schema: Mapping[str, Any]
    allowed_tools: tuple[str, ...]
    requested_route: str
    max_tokens: int | None
    timeout_seconds: float
    deadline_epoch: float
    cancellation_id: str
    code_revision: str
    genome_revision: str
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.invocation_id or not self.task_id or not self.attempt_id or not self.requested_route:
            raise ValueError("invocation, task and route identities are required")
        if self.lease_epoch < 1:
            raise ValueError("positive lease fencing epoch is required")
        if not self.logical_role or not self.purpose or not self.instruction:
            raise ValueError("role, purpose and instruction are required")
        if self.timeout_seconds <= 0 or self.deadline_epoch <= 0:
            raise ValueError("positive timeout and absolute deadline are required")
        if self.max_tokens is not None and self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive when specified")
        if self.allowed_tools:
            raise ValueError("cognitive adapter calls must not grant tools")


@dataclass(frozen=True)
class CognitiveCapabilities:
    route_id: str
    provider: str
    model_family: str
    requested_model: str | None
    supported_roles: frozenset[str]
    structured_output: bool
    tool_use: bool
    max_input_tokens: int | None
    concurrency_limit: int
    supports_cancel: bool
    cost_state: UsageState
    declared: frozenset[str] = frozenset()
    observed: frozenset[str] = frozenset()
    health: str = "UNKNOWN"
    last_probe: str | None = None


@dataclass(frozen=True)
class CognitiveInvocationResult:
    invocation_id: str
    worker_id: str
    worker_instance_id: str
    provider: str
    requested_route: str
    resolved_route: str | None
    resolution_state: str
    model_version: str | None
    adapter_version: str
    status: InvocationStatus
    started_at: str
    completed_at: str
    latency_ms: int
    raw_artifact_id: str | None
    raw_hash: str | None
    normalized_artifact_id: str | None
    normalized_hash: str | None
    structured_output: Mapping[str, Any] | None
    finish_reason: str | None
    input_tokens: int | None
    output_tokens: int | None
    cached_tokens: int | None
    reasoning_tokens: int | None
    monetary_cost: float | None
    cost_state: UsageState
    tool_summary: tuple[str, ...] = ()
    stderr_class: str | None = None
    error_class: str | None = None
    telemetry: Mapping[str, Any] = field(default_factory=dict)


class CognitiveAdapter(Protocol):
    def probe(self) -> Mapping[str, Any]: ...
    def capabilities(self) -> CognitiveCapabilities: ...
    def invoke(self, request: CognitiveInvocationRequest) -> CognitiveInvocationResult: ...
    def cancel(self, invocation_id: str) -> bool: ...
    def health(self) -> Mapping[str, Any]: ...
