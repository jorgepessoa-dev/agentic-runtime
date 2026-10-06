from __future__ import annotations

import hashlib
import json
from agentic_runtime.remote.config import setting
import ssl
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 0
PROTOCOL_VERSION = f"{PROTOCOL_MAJOR}.{PROTOCOL_MINOR}"
MAX_CONTROL_BYTES = 64 * 1024
MAX_ARTIFACT_BYTES = 8 * 1024 * 1024


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_json(value: Mapping[str, Any]) -> str:
    return digest_bytes(canonical_bytes(value))


def build_worker_tls_context(ca_file: str, cert_file: str, key_file: str) -> ssl.SSLContext:
    context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH,cafile=ca_file)
    context.minimum_version=ssl.TLSVersion.TLSv1_2
    context.check_hostname=True
    context.load_cert_chain(certfile=cert_file,keyfile=key_file)
    return context


class ProtocolError(ValueError):
    pass


def parse_protocol(value: str) -> tuple[int, int]:
    try:
        major, minor = value.split(".", 1)
        if not major.isdigit() or not minor.isdigit():
            raise ValueError
        return int(major), int(minor)
    except (AttributeError, ValueError) as exc:
        raise ProtocolError("malformed protocol version") from exc


@dataclass(frozen=True)
class WorkerIdentity:
    worker_id: str
    token: str
    worker_instance_id: str

    @classmethod
    def new_instance(cls, worker_id: str, token: str) -> "WorkerIdentity":
        return cls(worker_id, token, "wi_" + uuid.uuid4().hex)


class RemoteClientError(RuntimeError):
    def __init__(self, status: int, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.details = details


class WorkerClient:
    """Small HTTP client; it contains no database or governance access."""

    def __init__(self, endpoint: str, identity: WorkerIdentity, *, timeout: float = 5.0,
                 tls_context: ssl.SSLContext | None=None) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.identity = identity
        self.timeout = timeout
        self.tls_context=tls_context
        if self.endpoint.startswith("https://") and tls_context is None:
            raise ValueError("HTTPS worker endpoints require an explicit CA-verified mutual-TLS context")
        if self.endpoint.startswith("http://") and setting("AGENTIC_RUNTIME_REQUIRE_TLS")=="1":
            raise ValueError("plaintext worker transport is disabled by AGENTIC_RUNTIME_REQUIRE_TLS")

    def request(self, method: str, path: str, value: Mapping[str, Any] | None = None,
                *, body: bytes | None = None, headers: Mapping[str, str] | None = None,
                request_id: str | None = None) -> Any:
        payload = body if body is not None else canonical_bytes({**dict(value or {}),
            "protocol_version": PROTOCOL_VERSION, "request_id": request_id or uuid.uuid4().hex})
        request_headers = {"Content-Length": str(len(payload)),
            "Authorization": "Bearer " + self.identity.token,
            "X-Worker-ID": self.identity.worker_id,
            "X-Worker-Instance-ID": self.identity.worker_instance_id,
            "X-Agentic-Protocol": PROTOCOL_VERSION,
            "Content-Type": "application/json" if body is None else "application/octet-stream"}
        request_headers.update(headers or {})
        req = urllib.request.Request(self.endpoint + path, data=payload, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout,context=self.tls_context) as response:
                raw = response.read(MAX_CONTROL_BYTES + 1)
                if len(raw) > MAX_CONTROL_BYTES:
                    raise RemoteClientError(502, "oversized control-plane response")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read(MAX_CONTROL_BYTES + 1)
            try:
                details = json.loads(raw)
                message = details.get("error", "remote request rejected")
            except Exception:
                details, message = None, "remote request rejected"
            raise RemoteClientError(exc.code, message, details) from exc

    def register(self, *, software_version: str, capabilities: list[str], task_types: list[str],
                 resource_classes: list[str], resource_policy: Mapping[str, int],
                 allowed_tools: list[str] | None = None, worker_class: str="REMOTE_PERSISTENT",
                 max_concurrency: int=1) -> Any:
        return self.request("POST", "/v1/register", {
            "software_version": software_version, "capabilities": capabilities,
            "allowed_tools": list(allowed_tools or []), "task_types": task_types, "resource_classes": resource_classes,
            "resource_policy": dict(resource_policy),"worker_class":worker_class,
            "max_concurrency":max_concurrency})

    def heartbeat(self, active_attempts: list[Mapping[str, Any]] | None = None) -> Any:
        return self.request("POST", "/v1/heartbeat", {"active_attempts": list(active_attempts or [])})

    def claim(self, idempotency_key: str) -> Any:
        return self.request("POST", "/v1/claim", {"idempotency_key": idempotency_key}, request_id=idempotency_key)

    def start_attempt(self, task_id: str, attempt_id: str, lease_epoch: int) -> Any:
        return self.request("POST", "/v1/attempt/start", {"task_id": task_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch})

    def sandbox_created(self, task_id: str, attempt_id: str, lease_epoch: int,
                        sandbox_id: str, policy: Mapping[str, Any]) -> Any:
        return self.request("POST", "/v1/sandbox/created", {"task_id": task_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch,
            "sandbox_id": sandbox_id, "policy": dict(policy)})

    def upload_artifact(self, task_id: str, attempt_id: str, lease_epoch: int,
                        content: bytes, artifact_hash: str, *, request_id: str | None = None) -> Any:
        return self.request("PUT", f"/v1/artifacts/{artifact_hash}", body=content,
            request_id=request_id, headers={"X-Task-ID": task_id, "X-Attempt-ID": attempt_id,
                "X-Lease-Epoch": str(lease_epoch), "X-Transfer-ID": request_id or uuid.uuid4().hex,
                "Content-Length": str(len(content))})

    def submit_result(self, *, task_id: str, attempt_id: str, lease_epoch: int,
                      artifact_id: str, result_hash: str, idempotency_key: str,
                      usage: Mapping[str, Any] | None = None) -> Any:
        return self.request("POST", "/v1/result", {"task_id": task_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch,
            "artifact_id": artifact_id, "result_hash": result_hash,
            "idempotency_key": idempotency_key, "usage": dict(usage or {})}, request_id=idempotency_key)

    def cleanup_sandbox(self, task_id: str, attempt_id: str, lease_epoch: int,
                        sandbox_id: str, *, workspace_absent: bool) -> Any:
        return self.request("POST", "/v1/sandbox/cleanup", {"task_id": task_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch,
            "sandbox_id": sandbox_id, "workspace_absent": workspace_absent})

    def cancel_attempt(self, task_id: str, attempt_id: str, lease_epoch: int, *,
                       invocation_id: str | None = None, command_id: str | None = None,
                       process_termination_confirmed: bool = False) -> Any:
        return self.request("POST", "/v1/cancel", {"task_id": task_id,
            "attempt_id": attempt_id, "lease_epoch": lease_epoch,
            "invocation_id": invocation_id,"command_id":command_id,
            "process_termination_confirmed":bool(process_termination_confirmed)})

    def reconcile_local(self, sandboxes: list[Mapping[str, Any]]) -> Any:
        return self.request("POST", "/v1/reconcile-local", {"sandboxes": sandboxes})


class RemoteWorkerExecutionAdapter(WorkerClient):
    """Named replaceable execution-location adapter for the versioned worker API.

    It only transports normalized task/result/artifact operations. The worker
    has no PostgreSQL handle and receives no coordinator or governance methods.
    """

    adapter_type = "remote_worker"
