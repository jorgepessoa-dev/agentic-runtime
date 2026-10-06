from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping
from urllib.parse import urlsplit

from agentic_runtime.artifacts.store import ArtifactStore
from .contracts import (CognitiveCapabilities, CognitiveInvocationRequest,
                        CognitiveInvocationResult, InvocationStatus, UsageState)
from .validation import StructuredOutputError, canonical_json, parse_strict_json, sha256


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class TokenPriceSchedule:
    """Versioned external price evidence; rates are USD per million tokens."""

    schedule_id: str
    input_usd_per_million: float
    output_usd_per_million: float
    source_ref: str
    observed_at: str

    def __post_init__(self) -> None:
        if not self.schedule_id or not self.source_ref or not self.observed_at:
            raise ValueError("price schedule needs versioned source metadata")
        if min(self.input_usd_per_million, self.output_usd_per_million) < 0:
            raise ValueError("token prices cannot be negative")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class OpenAICompatibleCognitiveAdapter:
    """One-shot, no-tools JSON adapter with enforced byte/token/cost ceilings.

    Credentials are provided by a trusted local resolver and are only used to
    create the Authorization header. They never enter the request contract,
    logs, artifacts, error text, or child process environment.
    """

    def __init__(self, *, capabilities: CognitiveCapabilities, endpoint: str,
                 model: str, credential_resolver: Callable[[], str],
                 price_schedule: TokenPriceSchedule, artifacts: ArtifactStore,
                 worker_id: str, worker_instance_id: str,
                 max_instruction_bytes: int = 4096, max_input_tokens: int = 8192,
                 max_output_tokens: int = 256, max_response_bytes: int = 65536,
                 max_call_cost_usd: float = 0.01,
                 on_dispatch: Callable[[], None] | None = None) -> None:
        parts = urlsplit(endpoint)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
            raise ValueError("cognitive API endpoint must be a fixed HTTPS URL")
        if parts.query or parts.fragment:
            raise ValueError("cognitive API endpoint cannot contain query or fragment")
        if not model or not worker_id or not worker_instance_id:
            raise ValueError("model and worker identities are required")
        if min(max_instruction_bytes, max_input_tokens, max_output_tokens,
               max_response_bytes) <= 0 or max_call_cost_usd <= 0:
            raise ValueError("API adapter limits must be positive")
        self.capability = capabilities
        self.endpoint = endpoint
        self.model = model
        self.credential_resolver = credential_resolver
        self.price_schedule = price_schedule
        self.artifacts = artifacts
        self.worker_id = worker_id
        self.worker_instance_id = worker_instance_id
        self.max_instruction_bytes = max_instruction_bytes
        self.max_input_tokens = max_input_tokens
        self.max_output_tokens = max_output_tokens
        self.max_response_bytes = max_response_bytes
        self.max_call_cost_usd = max_call_cost_usd
        self.on_dispatch = on_dispatch

    def probe(self) -> Mapping[str, object]:
        return {"route_id": self.capability.route_id, "endpoint_host": urlsplit(self.endpoint).hostname,
                "health": "CONFIGURED"}

    def capabilities(self) -> CognitiveCapabilities:
        return self.capability

    def health(self) -> Mapping[str, object]:
        return {"status": "CONFIGURED", "route_id": self.capability.route_id}

    def _worst_case_cost(self, input_bytes: int, output_tokens: int) -> float:
        # UTF-8 bytes bound token count conservatively for the text payload;
        # a small fixed allowance covers provider message framing.
        input_bound = input_bytes + 64
        if input_bound > self.max_input_tokens:
            raise ValueError("conservative input token bound exceeds configured ceiling")
        return (input_bound * self.price_schedule.input_usd_per_million
                + output_tokens * self.price_schedule.output_usd_per_million) / 1_000_000

    def invoke(self, request: CognitiveInvocationRequest) -> CognitiveInvocationResult:
        if request.requested_route != self.capability.route_id:
            raise ValueError("requested route does not match adapter identity")
        if request.logical_role not in self.capability.supported_roles:
            raise PermissionError("route is not observed for the requested logical role")
        if request.allowed_tools:
            raise PermissionError("cognitive API requests cannot grant tools")
        instruction = request.instruction.encode("utf-8")
        if len(instruction) > self.max_instruction_bytes:
            raise ValueError("cognitive instruction exceeds configured byte ceiling")
        output_limit = min(request.max_tokens or self.max_output_tokens, self.max_output_tokens)
        body = canonical_json({
            "model": self.model,
            "messages": [{"role": "user", "content": request.instruction}],
            "max_tokens": output_limit,
            "temperature": 0,
            "stream": False,
            "response_format": {"type": "json_object"},
        })
        if len(body) > 8192:
            raise ValueError("serialized cognitive request exceeds configured byte ceiling")
        reservation = self._worst_case_cost(len(body), output_limit)
        if reservation > self.max_call_cost_usd:
            raise PermissionError("worst-case per-call cost exceeds the configured ceiling")

        started_at = _utc()
        started = time.monotonic()
        status = InvocationStatus.FAILED
        error_class = None
        finish_reason = None
        raw_artifact_id = raw_hash = normalized_artifact_id = normalized_hash = None
        structured = None
        input_tokens = output_tokens = cached_tokens = reasoning_tokens = None
        monetary_cost = None
        usage_state = cost_state = UsageState.UNKNOWN
        resolved_route = None
        raw_response = b""
        computed_cost_estimate = None
        failure_layer = "resolver"
        http_status = None
        dispatched = False
        try:
            api_key = self.credential_resolver()
            if not isinstance(api_key, str) or not api_key.strip() or "\n" in api_key:
                raise ValueError("credential resolver returned an invalid credential")
            failure_layer = "adapter"
            http_request = urllib.request.Request(self.endpoint, data=body, method="POST", headers={
                "Authorization": f"Bearer {api_key.strip()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
            timeout = min(request.timeout_seconds,
                          max(0.05, request.deadline_epoch - time.time()))
            opener = urllib.request.build_opener(_NoRedirect())
            failure_layer = "adapter"
            if self.on_dispatch is not None:
                self.on_dispatch()
            failure_layer = "transport"
            dispatched = True
            with opener.open(http_request, timeout=timeout) as response:
                http_status = response.getcode()
                failure_layer = "response"
                if response.geturl() != self.endpoint:
                    raise ValueError("provider endpoint redirected; refusing credential forwarding")
                raw_response = response.read(self.max_response_bytes + 1)
            if (api_key.encode("utf-8") in raw_response or
                    json.dumps(api_key)[1:-1].encode("utf-8") in raw_response):
                raw_response = b""
                status, error_class = InvocationStatus.MALFORMED, "CredentialReflection"
                raise ValueError("provider response rejected")
            if len(raw_response) > self.max_response_bytes:
                status, error_class = InvocationStatus.MALFORMED, "ResponseLimitExceeded"
            else:
                payload = json.loads(raw_response)
                choice = payload["choices"][0]
                message = choice["message"]
                if message.get("tool_calls") or message.get("function_call"):
                    status, error_class = InvocationStatus.MALFORMED, "UnexpectedToolRequest"
                else:
                    content = message.get("content")
                    if not isinstance(content, str):
                        status, error_class = InvocationStatus.MALFORMED, "MissingStructuredContent"
                    else:
                        try:
                            structured = parse_strict_json(content, request.response_schema)
                            status = InvocationStatus.SUCCEEDED
                        except StructuredOutputError:
                            status, error_class = InvocationStatus.MALFORMED, "InvalidStructuredOutput"
                    finish_reason = choice.get("finish_reason")
                    resolved_route = payload.get("model") if isinstance(payload.get("model"), str) else None
                    usage = payload.get("usage") or {}
                    input_tokens = usage.get("prompt_tokens") if isinstance(usage.get("prompt_tokens"), int) else None
                    output_tokens = usage.get("completion_tokens") if isinstance(usage.get("completion_tokens"), int) else None
                    details = usage.get("prompt_tokens_details") or {}
                    cached_tokens = details.get("cached_tokens") if isinstance(details.get("cached_tokens"), int) else None
                    reasoning = (usage.get("completion_tokens_details") or {}).get("reasoning_tokens")
                    reasoning_tokens = reasoning if isinstance(reasoning, int) else None
                    if input_tokens is not None and output_tokens is not None:
                        usage_state = UsageState.PROVIDER_REPORTED
                        computed_cost_estimate = (input_tokens * self.price_schedule.input_usd_per_million
                                         + output_tokens * self.price_schedule.output_usd_per_million) / 1_000_000
                        # A rate-card calculation is not an actual billed cost.
                        monetary_cost = None
                        cost_state = UsageState.UNKNOWN
                    else:
                        usage_state = cost_state = UsageState.UNKNOWN
        except urllib.error.HTTPError as exc:
            status = InvocationStatus.FAILED
            error_class = f"HTTP{exc.code}"
            http_status = exc.code
            failure_layer = {401:"authentication",403:"authorization_model_access",
                429:"rate_limit"}.get(exc.code,"transport" if exc.code>=500 else "response")
            exc.close()
        except (TimeoutError, socket.timeout):
            status, error_class = InvocationStatus.TIMED_OUT, "Timeout"
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                status, error_class = InvocationStatus.TIMED_OUT, "Timeout"
            else:
                status = InvocationStatus.FAILED
                error_class = type(exc.reason).__name__
        except json.JSONDecodeError:
            status, error_class = InvocationStatus.MALFORMED, "InvalidProviderJSON"
        except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
            status = InvocationStatus.FAILED if status != InvocationStatus.MALFORMED else status
            error_class = error_class or type(exc).__name__

        if raw_response:
            raw_manifest = self.artifacts.put_shared(raw_response, kind="cognitive_raw_output",
                producer_execution_id=request.invocation_id, producer_attempt_id=request.task_id,
                metadata={"retention_class":"EVIDENCE","route_id": self.capability.route_id,
                          "model": self.model, "price_schedule": self.price_schedule.schedule_id})
            self.artifacts.verify(raw_manifest.artifact_id)
            raw_artifact_id, raw_hash = raw_manifest.artifact_id, raw_manifest.content_hash
        if status == InvocationStatus.SUCCEEDED and structured is not None:
            normalized = canonical_json(structured)
            normalized_manifest = self.artifacts.put_shared(normalized, kind="cognitive_normalized_output",
                producer_execution_id=request.invocation_id, producer_attempt_id=request.task_id,
                metadata={"retention_class":"EVIDENCE","route_id": self.capability.route_id})
            self.artifacts.verify(normalized_manifest.artifact_id)
            normalized_artifact_id, normalized_hash = normalized_manifest.artifact_id, normalized_manifest.content_hash
        completed_at = _utc()
        latency_ms = int((time.monotonic() - started) * 1000)
        return CognitiveInvocationResult(invocation_id=request.invocation_id,
            worker_id=self.worker_id, worker_instance_id=self.worker_instance_id,
            provider=self.capability.provider, requested_route=self.capability.route_id,
            resolved_route=resolved_route, resolution_state="VERIFIED" if resolved_route else "UNKNOWN",
            model_version=resolved_route, adapter_version="openai-compatible-cognitive-v1",
            status=status, started_at=started_at, completed_at=completed_at,
            latency_ms=latency_ms, raw_artifact_id=raw_artifact_id, raw_hash=raw_hash,
            normalized_artifact_id=normalized_artifact_id, normalized_hash=normalized_hash,
            structured_output=structured, finish_reason=finish_reason,
            input_tokens=input_tokens, output_tokens=output_tokens, cached_tokens=cached_tokens,
            reasoning_tokens=reasoning_tokens, monetary_cost=monetary_cost,
            cost_state=cost_state, error_class=error_class,
            telemetry={"usage_state": usage_state.value,
                "failure_layer": failure_layer if status != InvocationStatus.SUCCEEDED else None,
                "http_status": http_status, "transport_dispatched": dispatched,
                "computed_cost_estimate_usd": computed_cost_estimate,
                "cost_estimate_basis": "historical rate schedule times provider-reported tokens; not billing evidence",
                "price_schedule": self.price_schedule.schedule_id,
                "price_source": self.price_schedule.source_ref,
                "reserved_worst_case_usd": round(reservation, 9),
                "request_sha256": sha256(body), "response_bytes": len(raw_response)})
