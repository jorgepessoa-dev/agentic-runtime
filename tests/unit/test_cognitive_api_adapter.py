from __future__ import annotations

import json
import socket
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.cognitive.api import OpenAICompatibleCognitiveAdapter, TokenPriceSchedule
from agentic_runtime.cognitive.contracts import (CognitiveCapabilities,
    CognitiveInvocationRequest, InvocationStatus, UsageState)


class _Response:
    def __init__(self, payload: bytes, url: str):
        self.payload, self.url = payload, url

    def __enter__(self): return self
    def __exit__(self, *args): return None
    def geturl(self): return self.url
    def getcode(self): return 200
    def read(self, limit=-1): return self.payload[:limit]


def request(route="test:api", *, instruction='Return {"answer":"ok"}.', max_tokens=64,
            allowed_tools=()):
    return CognitiveInvocationRequest(invocation_id="inv-1", task_id="task-1", attempt_id="att-1",
        lease_epoch=1, campaign_id=None, logical_role="hypothesis_generator", purpose="test",
        instruction=instruction, context_refs=(), response_schema={"type":"object",
        "required":["answer"],"properties":{"answer":{"type":"string"}},
        "additionalProperties":False}, allowed_tools=tuple(allowed_tools), requested_route=route,
        max_tokens=max_tokens, timeout_seconds=5, deadline_epoch=time.time()+5,
        cancellation_id="cancel-1", code_revision="rev-1", genome_revision="genome-1")


class CognitiveApiAdapterTests(unittest.TestCase):
    def adapter(self, artifacts, *, max_call_cost_usd=1.0):
        caps=CognitiveCapabilities(route_id="test:api",provider="test-provider",model_family="test-family",
            requested_model="test-model",supported_roles=frozenset({"hypothesis_generator"}),
            structured_output=True,tool_use=False,max_input_tokens=8192,concurrency_limit=1,
            supports_cancel=False,cost_state=UsageState.MEASURED)
        schedule=TokenPriceSchedule("test-v1",0.3,1.2,"https://pricing.example/v1","2026-10-03")
        return OpenAICompatibleCognitiveAdapter(capabilities=caps,
            endpoint="https://api.example/v1/chat/completions",model="test-model",
            credential_resolver=lambda:"secret-value",price_schedule=schedule,artifacts=artifacts,
            worker_id="worker-1",worker_instance_id="instance-1",
            max_call_cost_usd=max_call_cost_usd)

    def test_success_is_strictly_validated_and_cost_uses_reported_usage(self):
        response={"model":"resolved-test-model","choices":[{"message":{"content":"{\"answer\":\"ok\"}"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":20,"completion_tokens":3}}
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            with patch("urllib.request.OpenerDirector.open",return_value=_Response(json.dumps(response).encode(),adapter.endpoint)) as opened:
                result=adapter.invoke(request())
        self.assertEqual(result.status,InvocationStatus.SUCCEEDED)
        self.assertEqual(result.resolved_route,"resolved-test-model")
        self.assertEqual(result.cost_state,UsageState.UNKNOWN)
        self.assertIsNone(result.monetary_cost)
        self.assertAlmostEqual(result.telemetry['computed_cost_estimate_usd'],(20*.3+3*1.2)/1_000_000)
        self.assertTrue(result.raw_hash and result.normalized_hash)
        headers=opened.call_args.args[0].header_items()
        self.assertIn(("Authorization","Bearer secret-value"),headers)

    def test_unknown_usage_remains_unknown_and_no_money_is_claimed(self):
        response={"model":"resolved-test-model","choices":[{"message":{"content":"{\"answer\":\"ok\"}"},"finish_reason":"stop"}]}
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            with patch("urllib.request.OpenerDirector.open",return_value=_Response(json.dumps(response).encode(),adapter.endpoint)):
                result=adapter.invoke(request())
        self.assertEqual(result.cost_state,UsageState.UNKNOWN)
        self.assertIsNone(result.monetary_cost)

    def test_tool_grant_rejected_and_output_request_clamped(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            response={"model":"resolved-test-model","choices":[{"message":{"content":"{\"answer\":\"ok\"}"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":1,"completion_tokens":1}}
            with patch("urllib.request.OpenerDirector.open",return_value=_Response(json.dumps(response).encode(),adapter.endpoint)) as opened:
                with self.assertRaises(ValueError): request(allowed_tools=("shell",))
                result=adapter.invoke(request(max_tokens=10000))
                self.assertEqual(result.status,InvocationStatus.SUCCEEDED)
                self.assertEqual(json.loads(opened.call_args.args[0].data)["max_tokens"],256)
            self.assertEqual(opened.call_count,1)

    def test_unbounded_worst_case_cost_is_rejected_before_network(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)),max_call_cost_usd=0.000001)
            with patch("urllib.request.urlopen") as opened:
                with self.assertRaisesRegex(PermissionError,"worst-case"):
                    adapter.invoke(request(instruction="x"*4000))
            opened.assert_not_called()

    def test_non_https_endpoint_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            with self.assertRaisesRegex(ValueError,"HTTPS"):
                OpenAICompatibleCognitiveAdapter(capabilities=adapter.capabilities(),
                    endpoint="http://api.example/v1",model="test-model",credential_resolver=lambda:"x",
                    price_schedule=adapter.price_schedule,artifacts=adapter.artifacts,
                    worker_id="worker-1",worker_instance_id="instance-1")

    def test_invalid_provider_json_is_malformed(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            with patch("urllib.request.OpenerDirector.open",
                       return_value=_Response(b"{not-json",adapter.endpoint)):
                result=adapter.invoke(request())
        self.assertEqual(result.status,InvocationStatus.MALFORMED)
        self.assertEqual(result.error_class,"InvalidProviderJSON")

    def test_socket_timeout_is_reported_as_timed_out(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            with patch("urllib.request.OpenerDirector.open",side_effect=socket.timeout()):
                result=adapter.invoke(request())
        self.assertEqual(result.status,InvocationStatus.TIMED_OUT)
        self.assertEqual(result.error_class,"Timeout")

    def test_reflected_credential_is_rejected_before_artifact_write(self):
        response={"model":"test-model","choices":[{"message":{"content":'{"answer":"secret-value"}'}}]}
        with tempfile.TemporaryDirectory() as root:
            store=ArtifactStore(Path(root)); adapter=self.adapter(store)
            with patch("urllib.request.OpenerDirector.open",return_value=_Response(json.dumps(response).encode(),adapter.endpoint)):
                result=adapter.invoke(request())
            self.assertEqual(result.error_class,'CredentialReflection')
            self.assertEqual(result.status,InvocationStatus.MALFORMED)
            self.assertIsNone(result.raw_artifact_id)
            self.assertFalse(list(Path(root).rglob('content')))

    def test_http_failure_layers_discard_secret_bearing_errors(self):
        import io,urllib.error
        from dataclasses import asdict
        for code,layer in [(401,'authentication'),(403,'authorization_model_access'),(429,'rate_limit'),(503,'transport')]:
            with self.subTest(code=code),tempfile.TemporaryDirectory() as root:
                adapter=self.adapter(ArtifactStore(Path(root)))
                error=urllib.error.HTTPError(adapter.endpoint,code,'secret-value',{},io.BytesIO(b'secret-value'))
                with patch('urllib.request.OpenerDirector.open',side_effect=error):
                    result=adapter.invoke(request())
                self.assertEqual(result.telemetry['failure_layer'],layer)
                self.assertNotIn('secret-value',json.dumps(asdict(result)))
                self.assertIsNone(result.raw_artifact_id)

    def test_resolver_failure_does_not_dispatch_or_expose_exception_message(self):
        with tempfile.TemporaryDirectory() as root:
            adapter=self.adapter(ArtifactStore(Path(root)))
            def failed(): raise ValueError('secret-value')
            adapter.credential_resolver=failed
            with patch('urllib.request.OpenerDirector.open') as opened:
                result=adapter.invoke(request())
            opened.assert_not_called()
            self.assertEqual(result.telemetry['failure_layer'],'resolver')
            self.assertFalse(result.telemetry['transport_dispatched'])
            self.assertEqual(result.error_class,'ValueError')


if __name__ == "__main__": unittest.main()
