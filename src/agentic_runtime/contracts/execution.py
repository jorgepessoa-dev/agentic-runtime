from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping, Protocol, Sequence


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExecutorClass(StrEnum):
    NATIVE_AGENT_PROCESS = "native_agent_process"
    REMOTE_API_CALL = "remote_api_call"
    DETERMINISTIC_COMPUTE_JOB = "deterministic_compute_job"
    DECISION_MODEL_CALL = "decision_model_call"


class ExecutionMode(StrEnum):
    CHEAP = "CHEAP"
    DIVERSE = "DIVERSE"
    CRITICAL = "CRITICAL"


class ExecutionLocation(StrEnum):
    LOCAL = "LOCAL"
    REMOTE = "REMOTE"
    EPHEMERAL = "EPHEMERAL"
    API = "API"


class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"
    ABANDONED = "ABANDONED"


@dataclass(frozen=True)
class ExecutionRequest:
    execution_id: str
    task_id: str
    attempt_id: str
    capability: str
    executor_class: ExecutorClass
    requested_backend: str
    requested_model: str | None
    workspace_ref: str
    input_refs: tuple[str, ...] = ()
    output_contract: str = "text"
    resource_class: str = "default"
    execution_mode: ExecutionMode = ExecutionMode.CHEAP
    timeout_seconds: float = 120.0
    idempotency_key: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)
    execution_location: ExecutionLocation = ExecutionLocation.LOCAL


@dataclass(frozen=True)
class Usage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost: float | None = None


@dataclass(frozen=True)
class ExecutionResult:
    execution_id: str
    task_id: str
    attempt_id: str
    backend: str
    requested_model: str | None
    resolved_model: str | None
    started_at: str
    ended_at: str
    status: ExecutionStatus
    exit_code: int | None = None
    artifact_refs: tuple[str, ...] = ()
    output_hash: str | None = None
    usage: Usage = field(default_factory=Usage)
    cost: float | None = None
    error: str | None = None
    telemetry: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionResult":
        data = dict(value)
        data["status"] = ExecutionStatus(data["status"])
        data["artifact_refs"] = tuple(data.get("artifact_refs", ()))
        data["usage"] = Usage(**data.get("usage", {}))
        return cls(**data)


@dataclass(frozen=True)
class AdapterCapabilities:
    adapter_id: str
    adapter_version: str
    executor_class: ExecutorClass
    capabilities: frozenset[str]
    operations: frozenset[str]
    models: tuple[str, ...] = ()
    location: str = "LOCAL"
    supports_interrupt: bool = False
    supports_subtasks: bool = False


class HarnessAdapter(Protocol):
    def health(self) -> Mapping[str, Any]: ...
    def capabilities(self) -> AdapterCapabilities: ...
    def execute(self, request: ExecutionRequest) -> ExecutionResult: ...
    def status(self, execution_id: str) -> Mapping[str, Any]: ...
    def interrupt(self, execution_id: str) -> bool: ...
    def terminate(self, execution_id: str) -> bool: ...
    def collect_result(self, execution_id: str) -> ExecutionResult: ...


@dataclass(frozen=True)
class ModelRequest:
    request_id: str
    capability: str
    requested_model: str | None
    input_refs: tuple[str, ...]
    prompt: str
    output_contract: str
    timeout_seconds: float
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelResponse:
    request_id: str
    backend: str
    requested_model: str | None
    resolved_model: str | None
    content: str
    usage: Usage = field(default_factory=Usage)
    cost: float | None = None
    telemetry: Mapping[str, Any] = field(default_factory=dict)


class ModelAdapter(Protocol):
    def health(self) -> Mapping[str, Any]: ...
    def capabilities(self) -> AdapterCapabilities: ...
    def complete(self, request: ModelRequest) -> ModelResponse: ...


class DecisionAdapter(Protocol):
    def decide(self, policy_id: str, candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]: ...


class SandboxAdapter(Protocol):
    def create(self, task_id: str, attempt_id: str, base_revision: str) -> Any: ...
    def prepare(self, sandbox: Any, resource_policy: Mapping[str, Any]) -> Any: ...
    def collect_artifacts(self, sandbox: Any) -> Sequence[str]: ...
    def cleanup(self, sandbox: Any) -> Mapping[str, Any]: ...
    def destroy(self, sandbox: Any) -> Mapping[str, Any]: ...
