from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from agentic_runtime.remote.control_server import dispatch_outbox_batch
from agentic_runtime.remote.control import RemoteControlPlane, serve_local, serve_secure, build_server_tls_context
from agentic_runtime.remote.protocol import WorkerClient, WorkerIdentity, build_worker_tls_context


class M6TransportBoundaryTests(unittest.TestCase):
    def test_https_requires_explicit_tls_context(self):
        with self.assertRaisesRegex(ValueError, "explicit CA-verified mutual-TLS"):
            WorkerClient("https://control.invalid", WorkerIdentity.new_instance("worker", "token"))

    def test_plaintext_can_be_disabled_for_worker_process(self):
        old = os.environ.get("M6_REQUIRE_TLS")
        os.environ["M6_REQUIRE_TLS"] = "1"
        try:
            with self.assertRaisesRegex(ValueError, "plaintext worker transport is disabled"):
                WorkerClient("http://127.0.0.1:8765", WorkerIdentity.new_instance("worker", "token"))
        finally:
            if old is None:
                os.environ.pop("M6_REQUIRE_TLS", None)
            else:
                os.environ["M6_REQUIRE_TLS"] = old

    def test_health_distinguishes_liveness_from_database_readiness(self):
        with tempfile.TemporaryDirectory() as root:
            service = RemoteControlPlane("postgresql://invalid", root)
            status, body = service.handle("GET", "/health/live", {}, b"")
            self.assertEqual((status, body["status"]), (200, "LIVE"))
            status, body = service.handle("GET", "/health/ready", {}, b"")
            self.assertEqual(status, 503)
            self.assertEqual(body["status"], "NOT_READY")
            self.assertEqual(body["checks"]["database"], "UNAVAILABLE")


    def test_listener_rejects_unbounded_or_unbounded_connection_configuration(self):
        with tempfile.TemporaryDirectory() as root:
            service = RemoteControlPlane("postgresql://invalid", root)
            with self.assertRaisesRegex(ValueError, "max_connections"):
                serve_local(service, max_connections=0)
            with self.assertRaisesRegex(ValueError, "request timeout"):
                from agentic_runtime.remote.control import RemoteHTTPServer
                RemoteHTTPServer(("127.0.0.1", 0), service, request_timeout_seconds=0)


    def test_outbox_batch_drains_multiple_rows_but_obeys_hard_batch_limit(self):
        class FakeDispatcher:
            def __init__(self):
                self.calls=0
            def dispatch_one(self, _worker_id):
                self.calls+=1
                return self.calls<=5
        fake=FakeDispatcher()
        with patch("agentic_runtime.remote.control_server.OutboxDispatcher",return_value=fake):
            self.assertEqual(dispatch_outbox_batch(None,None,max_items=3),3)
            self.assertEqual(fake.calls,3)
            fake.calls=0
            self.assertEqual(dispatch_outbox_batch(None,None,max_items=5),5)
            self.assertEqual(fake.calls,5)
            fake.calls=0
            self.assertEqual(dispatch_outbox_batch(None,None,max_items=8),5)
            self.assertEqual(fake.calls,6)
        with self.assertRaisesRegex(ValueError,"batch size"):
            dispatch_outbox_batch(None,None,max_items=0)

    def test_mutual_tls_handshake_accepts_trusted_client_and_rejects_missing_certificate(self):
        openssl = shutil.which("openssl")
        self.assertIsNotNone(openssl, "OpenSSL is required for the local mTLS integration test")
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            cert = root_path / "test-cert.pem"
            key = root_path / "test-key.pem"
            untrusted_cert=root_path/"other-ca.pem"
            untrusted_key=root_path/"other-ca-key.pem"
            def make_cert(cert_path,key_path,common_name,san=None):
                args=[openssl,"req","-x509","-newkey","rsa:2048","-nodes","-keyout",str(key_path),
                    "-out",str(cert_path),"-days","1","-subj",f"/CN={common_name}",
                    "-addext","basicConstraints=critical,CA:TRUE"]
                if san:
                    args += ["-addext",f"subjectAltName={san}"]
                subprocess.run(args,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            make_cert(cert,key,"localhost","IP:127.0.0.1,DNS:localhost")
            make_cert(untrusted_cert,untrusted_key,"untrusted-client")
            service = RemoteControlPlane("postgresql://invalid", str(root_path / "artifacts"))
            server = serve_secure(service, "127.0.0.1", 0,
                build_server_tls_context(str(cert), str(key), str(cert)))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            endpoint = f"https://127.0.0.1:{server.server_address[1]}/health/live"
            try:
                context = build_worker_tls_context(str(cert), str(cert), str(key))
                with urllib.request.urlopen(endpoint, context=context, timeout=3) as response:
                    self.assertEqual(json.loads(response.read())["status"], "LIVE")
                without_client_cert = ssl.create_default_context(cafile=str(cert))
                with self.assertRaises((OSError, ssl.SSLError, urllib.error.URLError)):
                    with urllib.request.urlopen(endpoint, context=without_client_cert, timeout=3):
                        pass
                wrong_client=ssl.create_default_context(cafile=str(cert))
                wrong_client.load_cert_chain(str(untrusted_cert),str(untrusted_key))
                with self.assertRaises((OSError, ssl.SSLError, urllib.error.URLError)):
                    with urllib.request.urlopen(endpoint,context=wrong_client,timeout=3):
                        pass
                with self.assertRaises((OSError, ssl.SSLError, urllib.error.URLError)):
                    with urllib.request.urlopen(endpoint.replace("https://","http://"),timeout=3):
                        pass
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)


    def test_connection_error_log_contains_class_without_exception_text(self):
        with tempfile.TemporaryDirectory() as root:
            from agentic_runtime.remote.control import RemoteHTTPServer
            service=RemoteControlPlane("postgresql://invalid",root)
            server=RemoteHTTPServer(("127.0.0.1",0),service)
            try:
                try:
                    raise ssl.SSLError("token=must-not-be-logged")
                except ssl.SSLError:
                    with self.assertLogs("agentic_runtime.remote.control",level="WARNING") as captured:
                        server.handle_error(None,("127.0.0.1",0))
                rendered=" ".join(captured.output)
                self.assertIn('"error_class": "SSLError"',rendered)
                self.assertNotIn("must-not-be-logged",rendered)
                self.assertNotIn("Traceback",rendered)
            finally:
                server.server_close()

    def test_secure_listener_requires_client_certificate_verification(self):
        with tempfile.TemporaryDirectory() as root:
            service = RemoteControlPlane("postgresql://invalid", root)
            import ssl
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.verify_mode = ssl.CERT_NONE
            with self.assertRaisesRegex(ValueError, "requires mutual TLS"):
                serve_secure(service, "127.0.0.1", 0, context)


if __name__ == "__main__":
    unittest.main()
