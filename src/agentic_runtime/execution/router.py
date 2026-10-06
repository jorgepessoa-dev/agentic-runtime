from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Callable

from agentic_runtime.contracts.execution import ExecutionRequest, ExecutionResult, ExecutionStatus
from agentic_runtime.execution.registry import ExecutorRegistry


class IdempotencyConflict(RuntimeError):
    pass


class ExecutionInProgress(RuntimeError):
    pass


def _request_digest(request: ExecutionRequest) -> str:
    body = {"task_id": request.task_id, "attempt_id": request.attempt_id,
            "capability": request.capability, "executor_class": request.executor_class.value,
            "backend": request.requested_backend, "model": request.requested_model,
            "workspace_ref": request.workspace_ref, "input_refs": request.input_refs,
            "output_contract": request.output_contract, "resource_class": request.resource_class,
            "execution_mode": request.execution_mode.value, "timeout_seconds": request.timeout_seconds,
            "execution_location": request.execution_location.value,
            "metadata": dict(request.metadata)}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


class ExecutionRouter:
    """Deterministic routing and duplicate-acceptance control; not exactly-once."""

    def __init__(self, registry: ExecutorRegistry, ledger_path: Path,
                 artifact_verifier: Callable[[str], object] | None = None) -> None:
        self.registry = registry
        self.ledger_path = Path(ledger_path)
        self.artifact_verifier = artifact_verifier
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.ledger_path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS executions (idempotency_key TEXT PRIMARY KEY, "
                       "request_digest TEXT NOT NULL, state TEXT NOT NULL, result_json TEXT)")

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        if not request.idempotency_key:
            raise ValueError("idempotency_key is required")
        matches = [e for e in self.registry.eligible(request.capability, request.executor_class)
                   if e.backend == request.requested_backend]
        if len(matches) != 1:
            raise LookupError(f"expected one eligible executor for backend {request.requested_backend!r}; found {len(matches)}")
        registration = matches[0]
        digest = _request_digest(request)
        with sqlite3.connect(self.ledger_path, timeout=10) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT request_digest,state,result_json FROM executions WHERE idempotency_key=?",
                             (request.idempotency_key,)).fetchone()
            if row:
                if row[0] != digest:
                    raise IdempotencyConflict("idempotency key reused for a different logical request")
                if row[1] in {"COMMITTED", "FAILED"}:
                    return ExecutionResult.from_dict(json.loads(row[2]))
                raise ExecutionInProgress(f"execution unresolved in state {row[1]}; reconcile before retry")
            db.execute("INSERT INTO executions VALUES (?, ?, 'RUNNING', NULL)", (request.idempotency_key, digest))
            db.commit()
        try:
            result = registration.adapter.execute(request)
            if (result.execution_id, result.task_id, result.attempt_id) != (request.execution_id, request.task_id, request.attempt_id):
                raise ValueError("adapter returned mismatched execution identity")
            if result.status == ExecutionStatus.SUCCEEDED and (not result.output_hash or not result.artifact_refs):
                raise ValueError("successful execution must return verifiable artifact references and a hash")
            if result.status == ExecutionStatus.SUCCEEDED:
                if self.artifact_verifier is None:
                    raise ValueError("successful execution cannot be accepted without an artifact verifier")
                manifests = [self.artifact_verifier(ref) for ref in result.artifact_refs]
                if not any(getattr(manifest, "content_hash", None) == result.output_hash for manifest in manifests):
                    raise ValueError("no verified artifact matches the declared output hash")
        except BaseException:
            with sqlite3.connect(self.ledger_path) as db:
                db.execute("UPDATE executions SET state='UNRESOLVED' WHERE idempotency_key=?", (request.idempotency_key,))
            raise
        state = "COMMITTED" if result.status == ExecutionStatus.SUCCEEDED else "FAILED"
        with sqlite3.connect(self.ledger_path) as db:
            db.execute("UPDATE executions SET state=?,result_json=? WHERE idempotency_key=?",
                       (state, json.dumps(result.to_dict(), sort_keys=True), request.idempotency_key))
        return result
