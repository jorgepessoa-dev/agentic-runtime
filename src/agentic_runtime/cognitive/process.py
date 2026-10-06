from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

from agentic_runtime.artifacts.store import ArtifactStore
from .contracts import (CognitiveCapabilities, CognitiveInvocationRequest,
                        CognitiveInvocationResult, InvocationStatus)
from .validation import canonical_json, parse_strict_json


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _Invocation:
    process: subprocess.Popen[bytes]
    cancelled: threading.Event


class SubprocessCognitiveAdapter:
    """Tool-disabled, bounded CLI bridge; argv is supplied by a trusted adapter.

    A CLI profile must explicitly disable agent tools. Requests cannot turn
    tools on, and model response text is treated only as untrusted data.
    """

    def __init__(self, *, capabilities: CognitiveCapabilities, executable: str,
                 argv_builder: Callable[[CognitiveInvocationRequest, Path], Sequence[str]],
                 artifacts: ArtifactStore, worker_id: str, worker_instance_id: str,
                 environment: Mapping[str, str] | None = None, tools_disabled: bool,
                 adapter_version: str = "subprocess-cognitive-v1", max_output_bytes: int = 256 * 1024,
                 max_stderr_bytes: int = 16 * 1024, max_timeout_seconds: float = 180.0,
                 memory_limit_bytes: int = 1024 * 1024 * 1024, cpu_limit_seconds: int = 180,
                 prlimit_path: str = "/usr/bin/prlimit") -> None:
        if not tools_disabled:
            raise ValueError("cognitive proposal adapter must disable model tools")
        resolved = Path(executable).resolve(strict=True)
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            raise ValueError("adapter executable must be an existing executable file")
        self.capability = capabilities
        self.executable = str(resolved)
        self.argv_builder = argv_builder
        self.artifacts = artifacts
        self.worker_id, self.worker_instance_id = worker_id, worker_instance_id
        self.environment = {k: str(v) for k, v in (environment or {}).items()}
        allowed_environment = {"PATH", "HOME", "LANG", "TMPDIR", "NO_COLOR", "TERM",
                               "CODEX_HOME", "ANTHROPIC_CONFIG_DIR"}
        if set(self.environment) - allowed_environment:
            raise ValueError("cognitive process environment contains a non-allowlisted variable")
        self.adapter_version = adapter_version
        self.max_output_bytes = max_output_bytes
        self.max_stderr_bytes = max_stderr_bytes
        self.max_timeout_seconds = max_timeout_seconds
        self.prlimit_path = str(Path(prlimit_path).resolve(strict=True))
        if memory_limit_bytes < 64 * 1024 * 1024 or cpu_limit_seconds < 1:
            raise ValueError("cognitive process resource limits are too small")
        self.memory_limit_bytes, self.cpu_limit_seconds = memory_limit_bytes, cpu_limit_seconds
        self._active: dict[str, _Invocation] = {}
        self._lock = threading.Lock()

    def probe(self) -> Mapping[str, object]:
        return {"route_id": self.capability.route_id, "executable": self.executable,
                "executable_present": True, "health": self.health()["status"]}

    def capabilities(self) -> CognitiveCapabilities:
        return self.capability

    def health(self) -> Mapping[str, object]:
        return {"status": "HEALTHY" if Path(self.executable).is_file() else "UNAVAILABLE",
                "route_id": self.capability.route_id}

    @staticmethod
    def _read_bounded(pipe, limit: int, exceeded: threading.Event) -> bytes:
        data = bytearray()
        while True:
            chunk = pipe.read(8192)
            if not chunk:
                break
            remaining = limit - len(data)
            if remaining > 0:
                data.extend(chunk[:remaining])
            if len(chunk) > remaining:
                exceeded.set()
        return bytes(data)

    @staticmethod
    def _kill_tree(process: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=0.4)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()

    def invoke(self, request: CognitiveInvocationRequest) -> CognitiveInvocationResult:
        if request.requested_route != self.capability.route_id:
            raise ValueError("requested route does not match adapter identity")
        if request.logical_role not in self.capability.supported_roles:
            raise PermissionError("route is not observed for the requested logical role")
        if request.allowed_tools:
            raise PermissionError("cognitive subprocess requests cannot grant tools")
        if len(request.instruction.encode()) > 64 * 1024:
            raise ValueError("cognitive instruction exceeds limit")
        timeout = min(request.timeout_seconds, self.max_timeout_seconds,
                      max(0.05, request.deadline_epoch - time.time()))
        started_at = _utc()
        started = time.monotonic()
        status = InvocationStatus.FAILED
        raw_id = raw_hash = normalized_id = normalized_hash = None
        structured = None
        error_class = None
        stderr_class = None
        exit_code = None
        output = b""
        stderr = b""
        with tempfile.TemporaryDirectory(prefix="agentic-cognitive-") as cwd:
            schema_path = Path(cwd) / "response.schema.json"
            schema_path.write_bytes(canonical_json(request.response_schema))
            cli_args = list(self.argv_builder(request, schema_path))
            argv = [self.prlimit_path, f"--cpu={self.cpu_limit_seconds}:{self.cpu_limit_seconds+1}",
                    f"--as={self.memory_limit_bytes}:{self.memory_limit_bytes}", "--nofile=128:128",
                    "--core=0:0", "--", self.executable, *cli_args]
            if any(not isinstance(arg, str) or "\x00" in arg for arg in argv):
                raise ValueError("argv must be a sequence of valid strings")
            exceeded = threading.Event()
            try:
                process = subprocess.Popen(argv, cwd=cwd, env=dict(self.environment),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    start_new_session=True, close_fds=True)
            except OSError as exc:
                error_class = type(exc).__name__
                status = InvocationStatus.UNAVAILABLE
                process = None
            if process is not None:
                current = _Invocation(process, threading.Event())
                with self._lock:
                    self._active[request.invocation_id] = current
                out_thread = threading.Thread(target=lambda: None)
                stdout_box: list[bytes] = []
                stderr_box: list[bytes] = []
                out_thread = threading.Thread(target=lambda: stdout_box.append(
                    self._read_bounded(process.stdout, self.max_output_bytes, exceeded)), daemon=True)
                err_thread = threading.Thread(target=lambda: stderr_box.append(
                    self._read_bounded(process.stderr, self.max_stderr_bytes, threading.Event())), daemon=True)
                out_thread.start(); err_thread.start()
                try:
                    process.stdin.write(request.instruction.encode("utf-8"))
                    process.stdin.close()
                    end = time.monotonic() + timeout
                    while process.poll() is None and time.monotonic() < end:
                        if exceeded.is_set():
                            error_class = "OutputLimitExceeded"
                            self._kill_tree(process)
                            break
                        if current.cancelled.is_set():
                            status = InvocationStatus.CANCELLED
                            self._kill_tree(process)
                            break
                        time.sleep(0.02)
                    if process.poll() is None:
                        status = InvocationStatus.TIMED_OUT
                        self._kill_tree(process)
                    exit_code = process.returncode
                finally:
                    if process.poll() is None:
                        self._kill_tree(process)
                    out_thread.join(timeout=2); err_thread.join(timeout=2)
                    # The leader may have exited while a descendant keeps its
                    # pipes open. The process group remains the cancellation unit.
                    if out_thread.is_alive() or err_thread.is_alive():
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        out_thread.join(timeout=0.5); err_thread.join(timeout=0.5)
                        if out_thread.is_alive() or err_thread.is_alive():
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            out_thread.join(timeout=1); err_thread.join(timeout=1)
                    output = stdout_box[0] if stdout_box else b""
                    stderr = stderr_box[0] if stderr_box else b""
                    process.stdin.close() if not process.stdin.closed else None
                    process.stdout.close(); process.stderr.close()
                    with self._lock:
                        self._active.pop(request.invocation_id, None)
                if exceeded.is_set():
                    error_class = "OutputLimitExceeded"
                    status = InvocationStatus.MALFORMED
                elif status not in {InvocationStatus.CANCELLED, InvocationStatus.TIMED_OUT}:
                    status = InvocationStatus.SUCCEEDED if exit_code == 0 else InvocationStatus.FAILED
                    error_class = None if exit_code == 0 else "ProcessExit"
                stderr_class = "NONEMPTY_REDACTED" if stderr else None
        latency = int((time.monotonic() - started) * 1000)
        if output and error_class != "OutputLimitExceeded":
            raw = self.artifacts.put_shared(output, kind="cognitive_raw_output",
                producer_execution_id=request.invocation_id, producer_attempt_id=request.task_id,
                metadata={"retention_class":"EVIDENCE","route_id": self.capability.route_id, "provider": self.capability.provider,
                          "adapter_version": self.adapter_version})
            self.artifacts.verify(raw.artifact_id)
            raw_id, raw_hash = raw.artifact_id, raw.content_hash
        if status == InvocationStatus.SUCCEEDED:
            try:
                structured = parse_strict_json(output, request.response_schema)
                normalized = canonical_json(structured)
                artifact = self.artifacts.put_shared(normalized, kind="cognitive_normalized_output",
                    producer_execution_id=request.invocation_id, producer_attempt_id=request.task_id,
                    metadata={"retention_class":"EVIDENCE","raw_hash": raw_hash, "parser_version": "strict-json-v1"})
                normalized_id, normalized_hash = artifact.artifact_id, artifact.content_hash
            except (ValueError, UnicodeError):
                status = InvocationStatus.MALFORMED
                structured = None
                error_class = "StrictSchemaValidationError"
        if status != InvocationStatus.SUCCEEDED and error_class is None:
            error_class = status.value
        return CognitiveInvocationResult(
            invocation_id=request.invocation_id, worker_id=self.worker_id,
            worker_instance_id=self.worker_instance_id, provider=self.capability.provider,
            requested_route=request.requested_route, resolved_route=None, resolution_state="UNKNOWN",
            model_version=None, adapter_version=self.adapter_version, status=status,
            started_at=started_at, completed_at=_utc(), latency_ms=latency,
            raw_artifact_id=raw_id, raw_hash=raw_hash, normalized_artifact_id=normalized_id,
            normalized_hash=normalized_hash, structured_output=structured, finish_reason=None,
            input_tokens=None, output_tokens=None, cached_tokens=None, reasoning_tokens=None,
            monetary_cost=None, cost_state=self.capability.cost_state, stderr_class=stderr_class,
            error_class=error_class, telemetry={"exit_code": exit_code, "output_bytes": len(output),
                "stderr_bytes": len(stderr), "usage_state": "UNKNOWN",
                "resource_limits": {"memory_bytes":self.memory_limit_bytes,"cpu_seconds":self.cpu_limit_seconds}})

    def cancel(self, invocation_id: str) -> bool:
        with self._lock:
            current = self._active.get(invocation_id)
            if not current:
                return False
            current.cancelled.set()
            return True
