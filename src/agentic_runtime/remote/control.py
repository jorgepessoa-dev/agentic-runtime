from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import ssl
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.client import HTTPMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping
from urllib.parse import urlsplit

from agentic_runtime.artifacts.store import ArtifactStore
from agentic_runtime.coordinator.service import Coordinator
from agentic_runtime.coordinator.state import TaskState
from agentic_runtime.persistence.postgres import connect
from agentic_runtime.remote.config import CLIENT_CERT_HEADER, LEGACY_CLIENT_CERT_HEADER
from .protocol import (MAX_ARTIFACT_BYTES, MAX_CONTROL_BYTES, PROTOCOL_MAJOR,
    PROTOCOL_MINOR, ProtocolError, digest_bytes, digest_json, parse_protocol)


RESOURCE_CLASSES = {"native_agent_process", "remote_api_call", "deterministic_compute_job", "decision_model_call"}
WORKER_CLASSES = {"LOCAL", "REMOTE_PERSISTENT", "REMOTE_EPHEMERAL"}
RESOURCE_KEYS = {"cpu_seconds", "memory_bytes", "process_count", "wall_time_seconds", "max_sandbox_bytes"}
_SAFE_COGNITIVE_ERRORS = {
    "ProcessExit", "StrictSchemaValidationError", "OutputLimitExceeded", "TimeoutError",
    "PermissionError", "UNAVAILABLE", "FAILED", "TIMED_OUT", "CANCELLED", "MALFORMED",
    "Timeout", "InvalidProviderJSON", "InvalidStructuredOutput", "ResponseLimitExceeded",
    "MissingStructuredContent", "UnexpectedToolRequest", "ResolvedModelMismatch",
    "ResolvedModelUnavailable", "ConnectionResetError",
    "ConnectionRefusedError", "SSLError", "URLError", "OSError",
}


def _safe_cognitive_error(value: Any, succeeded: bool) -> str | None:
    """Keep stable error classes/status codes; never persist provider error bodies."""
    if not isinstance(value, str):
        return None if succeeded else "WorkerReportedFailure"
    if value in _SAFE_COGNITIVE_ERRORS:
        return value
    if (value.startswith("HTTP") and len(value) == 7 and value[4:].isdigit()
            and 100 <= int(value[4:]) <= 599):
        return value
    return None if succeeded else "WorkerReportedFailure"


def provision_worker(db: Any, *, worker_id: str, capabilities: list[str], task_types: list[str],
                     resource_classes: list[str], max_resource_policy: Mapping[str, int],
                     allowed_tools: list[str] | None = None, ttl_seconds: int = 3600,
                     created_by: str = "governance", worker_class: str = "REMOTE_PERSISTENT",
                     max_concurrency: int = 1, client_cert_sha256: str | None = None) -> str:
    """Create one capability-limited worker identity and return its token once."""
    if ttl_seconds < 60 or ttl_seconds > 86400:
        raise ValueError("worker credential lifetime must be 60..86400 seconds")
    if not worker_id or not capabilities or set(resource_classes) - RESOURCE_CLASSES:
        raise ValueError("invalid worker scope")
    if set(max_resource_policy) - RESOURCE_KEYS or any(not isinstance(v, int) or v <= 0 for v in max_resource_policy.values()):
        raise ValueError("invalid worker resource limits")
    if worker_class not in WORKER_CLASSES or not isinstance(max_concurrency, int) or max_concurrency < 1:
        raise ValueError("invalid worker class or concurrency limit")
    if client_cert_sha256 is not None and (len(client_cert_sha256) != 64 or
            any(char not in "0123456789abcdef" for char in client_cert_sha256)):
        raise ValueError("invalid worker client certificate fingerprint")
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    with db.transaction():
        db.execute("""INSERT INTO runtime.worker_identities
            (worker_id,token_sha256,granted_capabilities,allowed_task_types,resource_classes,
             max_resource_policy,allowed_tools,allowed_worker_classes,max_concurrency,client_cert_sha256,
             status,protocol_major,min_protocol_minor,token_expires_at,created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s,now()+(%s*interval '1 second'),%s)
            ON CONFLICT(worker_id) DO UPDATE SET token_sha256=EXCLUDED.token_sha256,
             granted_capabilities=EXCLUDED.granted_capabilities,allowed_task_types=EXCLUDED.allowed_task_types,
             resource_classes=EXCLUDED.resource_classes,max_resource_policy=EXCLUDED.max_resource_policy,
             allowed_tools=EXCLUDED.allowed_tools,allowed_worker_classes=EXCLUDED.allowed_worker_classes,
             max_concurrency=EXCLUDED.max_concurrency,client_cert_sha256=EXCLUDED.client_cert_sha256,
             status='ACTIVE',protocol_major=EXCLUDED.protocol_major,
             min_protocol_minor=EXCLUDED.min_protocol_minor,token_expires_at=EXCLUDED.token_expires_at,
             created_by=EXCLUDED.created_by""",
            (worker_id,digest,json.dumps(sorted(set(capabilities))),json.dumps(sorted(set(task_types))),
             json.dumps(sorted(set(resource_classes))),json.dumps(dict(max_resource_policy)),
             json.dumps(sorted(set(allowed_tools or []))),json.dumps([worker_class]),max_concurrency,
             client_cert_sha256,PROTOCOL_MAJOR,0,ttl_seconds,created_by))
    return token


class RemoteRequestError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class RemoteControlPlane:
    """Authenticated HTTP façade; all authoritative transitions still use Coordinator/PostgreSQL."""

    def __init__(self, database_url: str, artifact_root: str, *, database_role: str | None = None,
                 lease_seconds: int = 15, heartbeat_suspect_seconds: int = 3,
                 protocol_major: int = PROTOCOL_MAJOR, protocol_minor: int = PROTOCOL_MINOR,
                 admin_token: str | None = None) -> None:
        self.database_url = database_url
        self.database_role = database_role
        self.store = ArtifactStore(__import__("pathlib").Path(artifact_root))
        self.lease_seconds = lease_seconds
        self.heartbeat_suspect_seconds = heartbeat_suspect_seconds
        self.protocol_major, self.protocol_minor = protocol_major, protocol_minor
        self.admin_token_hash=hashlib.sha256(admin_token.encode()).hexdigest() if admin_token else None
        self.startup_reconciled=False
        self.require_client_certificate=False

    def db(self):
        db = connect(self.database_url)
        db.autocommit = True
        if self.database_role:
            if not self.database_role.replace("_", "").isalnum():
                db.close()
                raise ValueError("invalid database role configuration")
            db.execute(f'SET ROLE "{self.database_role}"')
        return db

    @staticmethod
    def _json(row: Mapping[str, Any]) -> dict[str, Any]:
        return {k: (v.isoformat() if isinstance(v, datetime) else v) for k,v in dict(row).items()}

    def _protocol(self, header: str, payload: Mapping[str, Any]) -> tuple[int,int]:
        body = payload.get("protocol_version")
        if not header or not body or header != body:
            raise RemoteRequestError(400,"protocol header/body mismatch")
        try:
            major,minor=parse_protocol(str(body))
        except ProtocolError as exc:
            raise RemoteRequestError(400,str(exc)) from exc
        if major != self.protocol_major or minor > self.protocol_minor:
            raise RemoteRequestError(426,"unsupported worker protocol version")
        return major,minor

    def readiness(self) -> tuple[bool,dict[str,str]]:
        checks={"database":"UNAVAILABLE","schema":"UNKNOWN","artifacts":"UNAVAILABLE",
            "reconciliation":"PENDING" if not self.startup_reconciled else "READY"}
        db=None
        try:
            db=self.db()
            row=db.execute("""SELECT NOT pg_is_in_recovery() AS writable,
                to_regclass('runtime.schema_migrations') IS NOT NULL AS migrated""").fetchone()
            checks["database"]="READY" if row["writable"] else "READ_ONLY"
            checks["schema"]="READY" if row["migrated"] else "MISSING"
        except Exception:
            pass
        finally:
            if db is not None: db.close()
        checks["artifacts"]="READY" if self.store.root.is_dir() and os.access(self.store.root,os.W_OK) else "UNAVAILABLE"
        return all(value=="READY" for value in checks.values()),checks

    def _identity(self, db: Any, headers: Mapping[str,str], *, instance: bool) -> tuple[dict[str,Any],dict[str,Any] | None]:
        worker_id=headers.get("X-Worker-ID","")
        bearer=headers.get("Authorization","")
        if not bearer.startswith("Bearer ") or len(bearer) > 256:
            raise RemoteRequestError(401,"worker authentication required")
        token=bearer[7:]
        row=db.execute("SELECT * FROM runtime.worker_identities WHERE worker_id=%s",(worker_id,)).fetchone()
        digest=hashlib.sha256(token.encode()).hexdigest()
        if not row or row["status"]!="ACTIVE" or not hmac.compare_digest(row["token_sha256"],digest):
            raise RemoteRequestError(401,"worker authentication failed")
        expected_cert=row.get("client_cert_sha256")
        presented_cert=headers.get(CLIENT_CERT_HEADER) or headers.get(LEGACY_CLIENT_CERT_HEADER)
        if (self.require_client_certificate and not expected_cert) or (expected_cert and
                (not presented_cert or not hmac.compare_digest(expected_cert,presented_cert))):
            raise RemoteRequestError(401,"worker certificate identity mismatch")
        alive=db.execute("SELECT token_expires_at>now() AS valid FROM runtime.worker_identities WHERE worker_id=%s",(worker_id,)).fetchone()["valid"]
        if not alive:
            raise RemoteRequestError(401,"worker credential expired")
        instance_row=None
        if instance:
            instance_id=headers.get("X-Worker-Instance-ID","")
            instance_row=db.execute("SELECT * FROM runtime.worker_instances WHERE worker_instance_id=%s AND worker_id=%s",
                (instance_id,worker_id)).fetchone()
            if not instance_row or instance_row["status"] in {"OFFLINE","REJECTED"}:
                raise RemoteRequestError(409,"worker instance is not active")
        return dict(row),dict(instance_row) if instance_row else None

    def _active_lease(self, db: Any, identity: dict[str,Any], instance: dict[str,Any],
                      task_id: str, attempt_id: str, epoch: int) -> dict[str,Any]:
        row=db.execute("""SELECT t.*,l.worker_id,l.attempt_id AS active_attempt,l.lease_epoch AS active_epoch,
            l.lease_until,l.status AS lease_status,a.worker_instance_id
            FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
            JOIN runtime.attempts a ON a.attempt_id=l.attempt_id
            WHERE t.task_id=%s AND l.attempt_id=%s""",(task_id,attempt_id)).fetchone()
        if not row or row["lease_status"]!="ACTIVE" or row["worker_id"]!=identity["worker_id"] \
                or row["worker_instance_id"]!=instance["worker_instance_id"] \
                or row["lease_epoch"]!=epoch or row["active_epoch"]!=epoch or row["lease_until"]<=datetime.now(timezone.utc):
            raise RemoteRequestError(409,"stale or unauthorized lease")
        return dict(row)

    def register(self, db: Any, headers: Mapping[str,str], payload: Mapping[str,Any]) -> dict[str,Any]:
        major,minor=self._protocol(headers.get("X-Agentic-Protocol",""),payload)
        identity,_=self._identity(db,headers,instance=False)
        if major!=identity["protocol_major"] or minor<int(identity["min_protocol_minor"]):
            raise RemoteRequestError(426,"worker protocol is outside identity compatibility range")
        instance_id=headers.get("X-Worker-Instance-ID","")
        caps=payload.get("capabilities"); tools=payload.get("allowed_tools",[]); tasks=payload.get("task_types")
        classes=payload.get("resource_classes"); policy=payload.get("resource_policy")
        worker_class=payload.get("worker_class","REMOTE_PERSISTENT")
        max_concurrency=payload.get("max_concurrency",1)
        if worker_class not in WORKER_CLASSES or worker_class not in set(identity.get("allowed_worker_classes") or []):
            raise RemoteRequestError(403,"worker class is outside its granted scope")
        if (not isinstance(max_concurrency,int) or isinstance(max_concurrency,bool) or max_concurrency < 1
                or max_concurrency > int(identity.get("max_concurrency",1))):
            raise RemoteRequestError(403,"worker concurrency exceeds its granted limit")
        if not isinstance(instance_id,str) or not instance_id.startswith("wi_"):
            raise RemoteRequestError(400,"invalid worker instance identity")
        for value,name in ((caps,"capabilities"),(tools,"allowed_tools"),(tasks,"task_types"),(classes,"resource_classes")):
            if not isinstance(value,list) or len(value)>64 or any(not isinstance(x,str) or not x or len(x)>80 for x in value):
                raise RemoteRequestError(400,f"invalid {name}")
        granted=set(identity["granted_capabilities"]); allowed_tasks=set(identity["allowed_task_types"])
        allowed_classes=set(identity["resource_classes"])
        allowed_tools=set(identity["allowed_tools"] or [])
        if (not set(caps).issubset(granted) or not set(tools).issubset(allowed_tools)
                or not set(tasks).issubset(allowed_tasks) or not set(classes).issubset(allowed_classes)):
            raise RemoteRequestError(403,"worker requested capabilities outside its granted scope")
        max_policy=identity["max_resource_policy"] or {}
        if not isinstance(policy,dict) or set(policy)-RESOURCE_KEYS or any(
            not isinstance(v,int) or v<=0 or v>int(max_policy.get(k,0)) for k,v in policy.items()):
            raise RemoteRequestError(403,"worker resource claim exceeds policy")
        software=payload.get("software_version")
        if not isinstance(software,str) or not software or len(software)>80:
            raise RemoteRequestError(400,"invalid software version")
        with db.transaction():
            db.execute("SELECT worker_id FROM runtime.worker_identities WHERE worker_id=%s FOR UPDATE",(identity["worker_id"],))
            existing=db.execute("SELECT * FROM runtime.worker_instances WHERE worker_id=%s AND status IN ('REGISTERING','READY','BUSY','SUSPECT','DRAINING') FOR UPDATE",
                (identity["worker_id"],)).fetchone()
            if existing and existing["worker_instance_id"]==instance_id:
                raise RemoteRequestError(409,"duplicate active worker instance identity")
            if existing:
                # A restarted process invalidates the previous process instance and
                # lets the existing lease reaper produce a new fenced attempt.
                db.execute("UPDATE runtime.worker_instances SET status='OFFLINE' WHERE worker_instance_id=%s",(existing["worker_instance_id"],))
                db.execute("UPDATE runtime.leases SET lease_until=now(),updated_at=now() WHERE worker_id=%s AND status='ACTIVE'",(identity["worker_id"],))
            db.execute("""INSERT INTO runtime.worker_instances
                (worker_instance_id,worker_id,software_version,protocol_major,protocol_minor,
                 capabilities,allowed_tools,task_types,resource_classes,resource_policy,worker_class,max_concurrency,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY')""",
                (instance_id,identity["worker_id"],software,major,minor,json.dumps(caps),json.dumps(tools),json.dumps(tasks),
                 json.dumps(classes),json.dumps(policy),worker_class,max_concurrency))
            coordinator=Coordinator(db)
            coordinator.register_worker(identity["worker_id"],list(caps))
            coordinator._event(event_type="WORKER_INSTANCE_REGISTERED",actor_id=identity["worker_id"],
                correlation_id=instance_id,payload={"worker_instance_id":instance_id,
                    "protocol_version":f"{major}.{minor}","capabilities":caps,
                    "task_types":tasks,"resource_classes":classes,"worker_class":worker_class,
                    "max_concurrency":max_concurrency})
            if existing:
                coordinator.reconcile_runtime(scan_id="worker-restart-"+instance_id,
                    retry_delay_seconds=0)
        return {"worker_id":identity["worker_id"],"worker_instance_id":instance_id,
                "status":"READY","worker_class":worker_class,"max_concurrency":max_concurrency,
                "protocol_version":f"{self.protocol_major}.{self.protocol_minor}"}

    def heartbeat(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any],
                  request_id: str, major: int, minor: int) -> dict[str,Any]:
        active=payload.get("active_attempts",[])
        if not isinstance(active,list) or len(active)>32:
            raise RemoteRequestError(400,"invalid active attempt list")
        valid=[]; stale=[]
        coordinator=Coordinator(db)
        for item in active:
            if not isinstance(item,dict):
                raise RemoteRequestError(400,"invalid active attempt")
            task=item.get("task_id"); attempt=item.get("attempt_id"); epoch=item.get("lease_epoch")
            try:
                self._active_lease(db,identity,instance,task,attempt,int(epoch))
                if coordinator.heartbeat(task,identity["worker_id"],attempt,int(epoch),lease_seconds=self.lease_seconds):
                    valid.append({"task_id":task,"attempt_id":attempt,"lease_epoch":int(epoch)})
                else: stale.append({"task_id":task,"attempt_id":attempt,"lease_epoch":int(epoch)})
            except (RemoteRequestError,TypeError,ValueError):
                stale.append({"task_id":task,"attempt_id":attempt,"lease_epoch":epoch})
        has_live=db.execute("SELECT EXISTS(SELECT 1 FROM runtime.leases WHERE worker_id=%s AND status='ACTIVE' AND lease_until>now()) AS busy",
            (identity["worker_id"],)).fetchone()["busy"]
        # DRAINING is a durable control-plane intent.  A live lease must not
        # overwrite it with BUSY, or an in-flight worker can never observe the
        # drain after completing its current attempt.
        status=("DRAINING" if instance["status"]=="DRAINING" else
                ("BUSY" if has_live else "READY"))
        with db.transaction():
            db.execute("UPDATE runtime.worker_instances SET status=%s,last_seen=now() WHERE worker_instance_id=%s",
                       (status,instance["worker_instance_id"]))
            db.execute("UPDATE runtime.workers SET status=%s,last_heartbeat=now(),updated_at=now() WHERE worker_id=%s",
                       ("DRAINING" if status=="DRAINING" else ("BUSY" if has_live else "IDLE"),identity["worker_id"]))
            db.execute("""INSERT INTO runtime.worker_heartbeats
                (heartbeat_id,worker_id,worker_instance_id,active_attempts,protocol_major,protocol_minor,request_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(request_id) DO NOTHING""",
                ("wh_"+uuid.uuid4().hex,identity["worker_id"],instance["worker_instance_id"],
                 json.dumps(active),major,minor,request_id))
            commands=db.execute("""SELECT command_id,task_id,attempt_id,lease_epoch,command FROM runtime.worker_commands
                WHERE worker_instance_id=%s AND status='PENDING' ORDER BY created_at FOR UPDATE""",
                (instance["worker_instance_id"],)).fetchall()
            if commands:
                db.execute("UPDATE runtime.worker_commands SET status='DELIVERED' WHERE worker_instance_id=%s AND status='PENDING'",
                    (instance["worker_instance_id"],))
        return {"status":status,"valid_attempts":valid,"stale_attempts":stale,
                "commands":[dict(c) for c in commands]}

    def claim(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any], request_id: str) -> dict[str,Any]:
        key=payload.get("idempotency_key")
        if not isinstance(key,str) or not key or len(key)>200:
            raise RemoteRequestError(400,"claim idempotency key required")
        request_hash=digest_json({"worker_id":identity["worker_id"],"worker_instance_id":instance["worker_instance_id"],"key":key})
        with db.transaction():
            db.execute("SELECT worker_instance_id FROM runtime.worker_instances WHERE worker_instance_id=%s FOR UPDATE",
                       (instance["worker_instance_id"],))
            prior=db.execute("SELECT request_hash,response FROM runtime.remote_claim_receipts WHERE worker_instance_id=%s AND idempotency_key=%s",
                (instance["worker_instance_id"],key)).fetchone()
            if prior:
                if prior["request_hash"]!=request_hash:
                    raise RemoteRequestError(409,"claim idempotency key reused")
                return dict(prior["response"])
            if instance["status"] in {"DRAINING","SUSPECT","OFFLINE","REJECTED"}:
                raise RemoteRequestError(409,"worker instance cannot claim in current state")
            capacity=db.execute("SELECT max_concurrency FROM runtime.worker_instances WHERE worker_instance_id=%s",
                                (instance["worker_instance_id"],)).fetchone()["max_concurrency"]
            active_count=db.execute("""SELECT count(*) AS n FROM runtime.leases l
                JOIN runtime.attempts a USING(attempt_id)
                WHERE a.worker_instance_id=%s AND l.status='ACTIVE' AND l.lease_until>now()""",
                (instance["worker_instance_id"],)).fetchone()["n"]
            coordinator=Coordinator(db)
            lease=None
            if active_count < int(capacity):
                lease=coordinator.claim(identity["worker_id"],lease_seconds=self.lease_seconds,
                    allowed_task_types=list(instance["task_types"]),
                    resource_classes=list(instance["resource_classes"]),worker_instance_id=instance["worker_instance_id"])
            if lease:
                # E1 probe tasks may request a configured executor profile;
                # ordinary tasks carrying an E1 scope follow its active
                # champion. The coordinator validates capability/health and
                # persists the complete eligible set before execution.
                e1_scope=(lease.get("metadata") or {}).get("e1_scope_id")
                evidence_executor=(lease.get("metadata") or {}).get("e1_evidence_executor_ref")
                preferred=evidence_executor
                policy_ref="e1-evaluation-probe"
                if e1_scope and not preferred:
                    active=db.execute("""SELECT v.canonical_config->'routing'->'preference'->>'default' AS executor_ref
                        FROM evolution.scope_champions c JOIN evolution.e1_genome_versions v USING(genome_id)
                        WHERE c.scope_id=%s""",(e1_scope,)).fetchone()
                    if not active:
                        raise RemoteRequestError(409,"E1 routing scope has no active champion")
                    preferred=active["executor_ref"]
                    policy_ref=f"e1-champion:{e1_scope}"
                if preferred:
                    selected=coordinator.choose_executor(task_id=lease["task_id"],attempt_id=lease["attempt_id"],
                        required_capabilities=list(lease["required_capabilities"] or []),
                        preferred_executor=preferred,policy_ref=policy_ref)
                    if selected!=preferred:
                        raise RemoteRequestError(409,"E1-selected executor is no longer eligible")
                response={"task":{"task_id":lease["task_id"],"task_type":lease["task_type"],
                    "attempt_id":lease["attempt_id"],"lease_epoch":lease["lease_epoch"],
                    "lease_seconds":self.lease_seconds,"required_capabilities":lease["required_capabilities"],
                    "input_refs":lease["input_refs"],"output_contract":lease["output_contract"],
                    "budget":lease["budget"],"metadata":{k:v for k,v in (lease["metadata"] or {}).items()
                        if k.startswith("worker_input_") or k in {"worker_sleep_seconds","worker_spawn_tree_seconds"}}}}
                db.execute("UPDATE runtime.worker_instances SET status='BUSY',last_seen=now() WHERE worker_instance_id=%s",
                           (instance["worker_instance_id"],))
                attempt=lease["attempt_id"]; task=lease["task_id"]; epoch=lease["lease_epoch"]
            else:
                response={"task":None}
                attempt=task=None; epoch=None
            db.execute("""INSERT INTO runtime.remote_claim_receipts
                (worker_instance_id,idempotency_key,request_hash,task_id,attempt_id,lease_epoch,response)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (instance["worker_instance_id"],key,request_hash,task,attempt,epoch,json.dumps(response)))
            return response

    def start_attempt(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        task_id,attempt_id,epoch=payload.get("task_id"),payload.get("attempt_id"),payload.get("lease_epoch")
        row=self._active_lease(db,identity,instance,task_id,attempt_id,int(epoch))
        coordinator=Coordinator(db)
        if row["status"]=="LEASED":
            coordinator.transition(task_id,attempt_id,int(epoch),TaskState.CONTEXT_VALIDATED,actor_id=identity["worker_id"],
                payload={"worker_instance_id":instance["worker_instance_id"]})
            coordinator.transition(task_id,attempt_id,int(epoch),TaskState.RUNNING,actor_id=identity["worker_id"],
                payload={"worker_instance_id":instance["worker_instance_id"]})
        elif row["status"]!="RUNNING":
            raise RemoteRequestError(409,"attempt is not startable")
        response={"status":"RUNNING","task_id":task_id,"attempt_id":attempt_id,"lease_epoch":int(epoch)}
        if row["task_type"]=="cognitive_invocation":
            from dataclasses import asdict
            from agentic_runtime.cognitive.contracts import CognitiveInvocationRequest
            from agentic_runtime.cognitive.ledger import CognitiveBudgetExceeded, CognitiveLedger
            request_data=(row.get("metadata") or {}).get("worker_input_cognitive_request")
            if not isinstance(request_data,dict):
                raise RemoteRequestError(422,"cognitive request envelope missing")
            allowed={"route_id","logical_role","purpose","instruction","context_refs","response_schema",
                     "max_tokens","max_wall_time_seconds","code_revision","genome_revision","provenance"}
            if set(request_data)!=allowed:
                raise RemoteRequestError(422,"cognitive request fields do not match protocol schema")
            route_id=request_data["route_id"]
            route=db.execute("""SELECT * FROM runtime.cognitive_routes cr
                WHERE route_id=%s AND worker_id=%s AND health='HEALTHY'
                  AND last_probe<=now() AND (last_probe>now()-interval '10 minutes'
                    OR EXISTS(SELECT 1 FROM runtime.cognitive_qualification_acceptances qa
                     WHERE qa.route_id=cr.route_id AND qa.campaign_id=%s
                       AND qa.qualified_at=cr.last_probe AND qa.expires_at>now()))
                FOR SHARE""",
                (route_id,identity["worker_id"],row['campaign_id'])).fetchone()
            if not route or route["tool_use"] or not route["structured_output"]:
                raise RemoteRequestError(403,"worker has no healthy tool-disabled structured route grant")
            if request_data["logical_role"] not in route["supported_roles"]:
                raise RemoteRequestError(403,"route is not registered for requested logical role")
            task_schema=row.get("output_contract") or {}
            schema=request_data["response_schema"]
            from agentic_runtime.cognitive.validation import COGNITIVE_BUNDLE_CONTRACT, canonical_json
            if (not isinstance(schema,dict) or schema.get("type")!="object"
                    or len(canonical_json(schema))>32*1024
                    or task_schema!=COGNITIVE_BUNDLE_CONTRACT):
                raise RemoteRequestError(422,"cognitive response schema or task envelope contract is invalid")
            budget=row.get("budget") or {}
            wall_cap=float(budget.get("max_wall_time_seconds",0))
            timeout=min(float(request_data["max_wall_time_seconds"]),wall_cap,180.0)
            if timeout<=0:
                raise RemoteRequestError(422,"cognitive task requires a positive persisted wall-time budget")
            invocation_id="cog_"+hashlib.sha256(f"{task_id}:{attempt_id}:{epoch}".encode()).hexdigest()[:32]
            request=CognitiveInvocationRequest(invocation_id=invocation_id,task_id=task_id,
                attempt_id=attempt_id,lease_epoch=int(epoch),campaign_id=row["campaign_id"],
                logical_role=request_data["logical_role"],purpose=request_data["purpose"],
                instruction=request_data["instruction"],context_refs=tuple(request_data["context_refs"]),
                response_schema=schema,allowed_tools=(),requested_route=route_id,
                max_tokens=request_data["max_tokens"],timeout_seconds=timeout,
                deadline_epoch=time.time()+timeout,cancellation_id="cancel_"+invocation_id,
                code_revision=request_data["code_revision"],genome_revision=request_data["genome_revision"],
                provenance={**request_data["provenance"],"attempt_id":attempt_id})
            request_hash=digest_json(asdict(request))
            try:
                invocation_id,created=CognitiveLedger(db).begin_invocation(invocation_id=invocation_id,
                    idempotency_key=f"cognitive:{task_id}:{attempt_id}:{epoch}",request=request,
                    request_hash=request_hash,worker_id=identity["worker_id"],
                    worker_instance_id=instance["worker_instance_id"])
            except CognitiveBudgetExceeded as exc:
                coordinator.transition(task_id,attempt_id,int(epoch),TaskState.BUDGET_EXCEEDED,
                    actor_id="cognitive-budget-controller",payload={"reason":"COGNITIVE_BUDGET_REJECTED"})
                raise RemoteRequestError(429,"cognitive campaign budget rejected dispatch") from exc
            if not created:
                raise RemoteRequestError(409,"cognitive invocation was already dispatched for this attempt")
            response["cognitive_request"]={**asdict(request),"request_hash":request_hash}
        return response

    def sandbox_created(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        task_id,attempt_id,epoch=payload.get("task_id"),payload.get("attempt_id"),int(payload.get("lease_epoch",-1))
        sandbox_id=payload.get("sandbox_id")
        if not isinstance(sandbox_id,str) or not sandbox_id.startswith("sbx_"):
            raise RemoteRequestError(400,"invalid sandbox identity")
        with db.transaction():
            self._active_lease(db,identity,instance,task_id,attempt_id,epoch)
            db.execute("""INSERT INTO runtime.sandboxes
                (sandbox_id,task_id,attempt_id,implementation,base_revision,workspace_ref,status,cleanup_status,
                 policy_ref,metadata,worker_id,worker_instance_id,lease_epoch)
                VALUES (%s,%s,%s,'remote-bwrap',NULL,%s,'ACTIVE','ACTIVE','worker-policy',%s,%s,%s,%s)
                ON CONFLICT(sandbox_id) DO NOTHING""",
                (sandbox_id,task_id,attempt_id,f"remote://{identity['worker_id']}/{instance['worker_instance_id']}/{sandbox_id}",
                 json.dumps({"policy":payload.get("policy",{})}),identity["worker_id"],instance["worker_instance_id"],epoch))
            db.execute("UPDATE runtime.attempts SET sandbox_id=%s WHERE attempt_id=%s AND worker_instance_id=%s",
                       (sandbox_id,attempt_id,instance["worker_instance_id"]))
            Coordinator(db)._event(event_type="REMOTE_SANDBOX_CREATED",actor_id=identity["worker_id"],
                correlation_id=task_id,task_id=task_id,attempt_id=attempt_id,
                payload={"sandbox_id":sandbox_id,"worker_instance_id":instance["worker_instance_id"],"lease_epoch":epoch})
        return {"sandbox_id":sandbox_id,"status":"ACTIVE"}

    def upload_artifact(self, db: Any, headers: Mapping[str,str], identity: dict[str,Any],
                        instance: dict[str,Any], expected_hash: str, content: bytes) -> dict[str,Any]:
        task_id=headers.get("X-Task-ID",""); attempt_id=headers.get("X-Attempt-ID","")
        try: epoch=int(headers.get("X-Lease-Epoch","-1"))
        except ValueError as exc: raise RemoteRequestError(400,"invalid lease epoch") from exc
        self._active_lease(db,identity,instance,task_id,attempt_id,epoch)
        if len(expected_hash)!=64 or any(c not in "0123456789abcdef" for c in expected_hash):
            raise RemoteRequestError(400,"invalid artifact digest")
        digest=digest_bytes(content)
        if digest!=expected_hash:
            db.execute("""INSERT INTO runtime.remote_transfer_rejections
                (rejection_id,worker_id,worker_instance_id,task_id,attempt_id,lease_epoch,expected_hash,received_size,reason)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'HASH_MISMATCH')""",
                ("reject_"+uuid.uuid4().hex,identity["worker_id"],instance["worker_instance_id"],
                 task_id,attempt_id,epoch,expected_hash,len(content)))
            raise RemoteRequestError(422,"artifact content hash mismatch")
        artifact_id="sha256:"+digest
        metadata={"retention_class":"EVIDENCE","worker_id":identity["worker_id"],
            "worker_instance_id":instance["worker_instance_id"],"lease_epoch":epoch,
            "transfer_id":headers.get("X-Transfer-ID","")}
        manifest=self.store.put(content,kind="remote_result",producer_execution_id=f"remote:{identity['worker_id']}:{instance['worker_instance_id']}",
            producer_attempt_id=attempt_id,metadata=metadata)
        input_refs=db.execute("SELECT input_refs FROM runtime.tasks WHERE task_id=%s",(task_id,)).fetchone()["input_refs"]
        input_hash=digest_json({"input_refs":input_refs})
        coordinator=Coordinator(db)
        coordinator.register_artifact(task_id=task_id,attempt_id=attempt_id,lease_epoch=epoch,
            manifest=manifest,artifact_store=self.store,input_manifest_hash=input_hash)
        with db.transaction():
            db.execute("UPDATE runtime.artifacts SET worker_id=%s,worker_instance_id=%s,lease_epoch=%s WHERE artifact_id=%s",
                (identity["worker_id"],instance["worker_instance_id"],epoch,artifact_id))
            Coordinator(db)._event(event_type="REMOTE_ARTIFACT_REGISTERED",actor_id=identity["worker_id"],
                correlation_id=task_id,task_id=task_id,attempt_id=attempt_id,
                payload={"artifact_id":artifact_id,"content_hash":digest,"worker_instance_id":instance["worker_instance_id"],
                    "lease_epoch":epoch,"transfer_id":headers.get("X-Transfer-ID","")})
        return {"artifact_id":artifact_id,"content_hash":digest,"size":len(content),"verified":True}

    def _ingest_cognitive_bundle(self, db: Any, *, task_id: str, attempt_id: str, epoch: int,
                                 identity: Mapping[str,Any], instance: Mapping[str,Any],
                                 bundle_artifact_id: str) -> tuple[dict[str,Any],dict[str,Any],Any,dict[str,Any],float|None]:
        """Verify worker envelope, independently validate output, and preserve raw lineage."""
        import base64
        from datetime import datetime, timezone
        from agentic_runtime.cognitive.contracts import CognitiveInvocationResult, InvocationStatus, UsageState
        from agentic_runtime.cognitive.ledger import CognitiveLedger
        from agentic_runtime.cognitive.validation import canonical_json, parse_strict_json
        try:
            bundle=json.loads(self.store.read(bundle_artifact_id))
        except (ValueError,UnicodeDecodeError) as exc:
            raise RemoteRequestError(422,"cognitive result envelope is invalid") from exc
        if not isinstance(bundle,dict) or bundle.get("format")!="agentic-cognitive-bundle-v1":
            raise RemoteRequestError(422,"cognitive result envelope version is unsupported")
        invocation_id=bundle.get("invocation_id")
        invocation=db.execute("""SELECT * FROM runtime.cognitive_invocations
            WHERE invocation_id=%s AND task_id=%s AND attempt_id=%s AND worker_id=%s
              AND worker_instance_id=%s AND lease_epoch=%s FOR UPDATE""",
            (invocation_id,task_id,attempt_id,identity["worker_id"],instance["worker_instance_id"],epoch)).fetchone()
        if not invocation or invocation["status"]!="PENDING" or invocation["request_hash"]!=bundle.get("request_hash"):
            raise RemoteRequestError(409,"cognitive result has no matching pending invocation")
        route=db.execute("SELECT * FROM runtime.cognitive_routes WHERE route_id=%s AND worker_id=%s",
            (invocation["route_id"],identity["worker_id"])).fetchone()
        if not route or bundle.get("requested_route")!=invocation["requested_route"]:
            raise RemoteRequestError(409,"cognitive result route identity mismatch")
        try:
            status=InvocationStatus(bundle.get("status"))
        except ValueError as exc:
            raise RemoteRequestError(422,"cognitive terminal status is invalid") from exc
        def get_bytes(field: str, hash_field: str) -> bytes | None:
            encoded=bundle.get(field); expected=bundle.get(hash_field)
            if encoded is None:
                if expected is not None:
                    raise RemoteRequestError(422,"cognitive artifact hash is missing its bytes")
                return None
            if not isinstance(encoded,str) or len(encoded)>((256*1024+2)//3)*4+8:
                raise RemoteRequestError(413,"cognitive artifact exceeds limit")
            try:
                data=base64.b64decode(encoded,validate=True)
            except (ValueError,base64.binascii.Error) as exc:
                raise RemoteRequestError(422,"cognitive artifact encoding is invalid") from exc
            if len(data)>256*1024 or not isinstance(expected,str) or digest_bytes(data)!=expected:
                raise RemoteRequestError(422,"cognitive artifact content hash mismatch")
            return data
        raw=get_bytes("raw_b64","raw_hash")
        normalized=get_bytes("normalized_b64","normalized_hash")
        task=db.execute("SELECT input_refs,output_contract FROM runtime.tasks WHERE task_id=%s",(task_id,)).fetchone()
        if not task:
            raise RemoteRequestError(404,"cognitive task disappeared")
        structured=None
        worker_resolved_model=bundle.get("resolved_route")
        resolved_model=(worker_resolved_model if isinstance(worker_resolved_model,str) and worker_resolved_model else None)
        resolution_state="UNVERIFIED" if resolved_model else "UNKNOWN"
        result_error_class=bundle.get("error_class")
        if status==InvocationStatus.SUCCEEDED:
            if raw is None or normalized is None:
                raise RemoteRequestError(422,"successful cognitive result needs raw and normalized artifacts")
            if route["adapter_version"].startswith("openai-compatible-cognitive-"):
                try:
                    provider_envelope=json.loads(raw)
                except (ValueError,UnicodeDecodeError):
                    provider_envelope=None
                raw_model=(provider_envelope.get("model")
                           if isinstance(provider_envelope,dict) else None)
                if not isinstance(raw_model,str) or not raw_model:
                    status=InvocationStatus.MALFORMED
                    result_error_class="ResolvedModelUnavailable"
                    normalized=None
                elif raw_model!=route["model_label"] or (resolved_model and resolved_model!=raw_model):
                    status=InvocationStatus.MALFORMED
                    result_error_class="ResolvedModelMismatch"
                    normalized=None
                    resolved_model=raw_model
                    resolution_state="UNVERIFIED"
                else:
                    # The raw provider response crossed an authenticated worker
                    # boundary and is hash-verified, but the provider does not
                    # sign it. Preserve the matching model claim as unverified.
                    resolved_model=raw_model
                    resolution_state="UNVERIFIED"
            if status==InvocationStatus.SUCCEEDED:
                try:
                    structured=parse_strict_json(normalized,invocation["response_schema"])
                except ValueError as exc:
                    raise RemoteRequestError(422,"control-plane schema validation rejected cognitive output") from exc
                if canonical_json(structured)!=normalized:
                    raise RemoteRequestError(422,"normalized cognitive output is not canonical")
                if bundle.get("structured_output")!=structured:
                    raise RemoteRequestError(422,"structured output differs from normalized artifact")
        elif normalized is not None:
            raise RemoteRequestError(422,"unsuccessful cognitive result cannot carry normalized success output")
        raw_id=normalized_id=None
        for content,kind in ((raw,"cognitive_raw_output"),(normalized,"cognitive_normalized_output")):
            if content is None:
                continue
            manifest=self.store.put_shared(content,kind=kind,
                producer_execution_id=f"cognitive:{invocation_id}",producer_attempt_id=attempt_id,
                metadata={"retention_class":"EVIDENCE","invocation_id":invocation_id,
                          "route_id":route["route_id"],"content_hash":digest_bytes(content)})
            artifact_kind="RAW_OUTPUT" if kind=="cognitive_raw_output" else "NORMALIZED_OUTPUT"
            db.execute("""INSERT INTO runtime.cognitive_artifacts
                (cognitive_artifact_id,invocation_id,kind,blob_artifact_id,content_hash,task_id,
                 attempt_id,location,verification_status,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'VERIFIED',%s)""",
                (f"{invocation_id}:{artifact_kind.lower()}",invocation_id,artifact_kind,
                 manifest.artifact_id,manifest.content_hash,task_id,attempt_id,manifest.location,
                 json.dumps({"worker_id":identity["worker_id"],"worker_instance_id":instance["worker_instance_id"],
                     "lease_epoch":epoch,"adapter_route_id":route["route_id"],"input_manifest_hash":
                     digest_json({"input_refs":task["input_refs"]})})))
            if artifact_kind=="RAW_OUTPUT": raw_id=manifest.artifact_id
            else: normalized_id=manifest.artifact_id
        def bounded_tokens(field: str) -> int | None:
            value=bundle.get(field)
            return value if isinstance(value,int) and not isinstance(value,bool) and 0<=value<=100_000_000 else None
        worker_token_claims={field:bounded_tokens(field) for field in
            ("input_tokens","output_tokens","cached_tokens","reasoning_tokens")}
        # Preserve a clearly labelled estimate from the worker's provider
        # response only when a trusted route configuration has an immutable,
        # public price schedule. It is not an invoice, is not independently
        # attested, and is never used to settle a hard monetary budget.
        pricing=route.get("pricing") or {}
        estimated_cost=None
        if (isinstance(pricing,dict)
                and isinstance(pricing.get("input_usd_per_million"),(int,float))
                and not isinstance(pricing.get("input_usd_per_million"),bool)
                and isinstance(pricing.get("output_usd_per_million"),(int,float))
                and not isinstance(pricing.get("output_usd_per_million"),bool)
                and worker_token_claims["input_tokens"] is not None
                and worker_token_claims["output_tokens"] is not None):
            estimated_cost=(worker_token_claims["input_tokens"]*pricing["input_usd_per_million"]
                +worker_token_claims["output_tokens"]*pricing["output_usd_per_million"])/1_000_000
        now=datetime.now(timezone.utc)
        started=invocation["started_at"]
        latency=max(0,int((now-started).total_seconds()*1000))
        configured_cost_state=UsageState(invocation["cost_state"])
        # A remote worker's telemetry is a claim, not provider-attested usage.
        # Keep it for diagnostics, but do not expose it as measured usage or
        # let it settle a campaign token/cost ceiling.
        cost_state=(configured_cost_state if configured_cost_state in {
            UsageState.SUBSCRIPTION_UNPRICED,UsageState.NOT_APPLICABLE} else UsageState.UNKNOWN)
        result=CognitiveInvocationResult(invocation_id=invocation_id,worker_id=identity["worker_id"],
            worker_instance_id=instance["worker_instance_id"],provider=route["provider_name"],
            requested_route=route["route_id"],resolved_route=resolved_model,resolution_state=resolution_state,
            model_version=resolved_model,adapter_version=route["adapter_version"],status=status,
            started_at=started.isoformat(),completed_at=now.isoformat(),latency_ms=latency,
            raw_artifact_id=raw_id,raw_hash=digest_bytes(raw) if raw is not None else None,
            normalized_artifact_id=normalized_id,normalized_hash=digest_bytes(normalized) if normalized is not None else None,
            structured_output=structured,finish_reason=None,input_tokens=None,
            output_tokens=None,cached_tokens=None,
            reasoning_tokens=None,monetary_cost=None,cost_state=cost_state,
            tool_summary=(),stderr_class="REDACTED" if bundle.get("stderr_class") else None,
            error_class=_safe_cognitive_error(result_error_class,status==InvocationStatus.SUCCEEDED),
            telemetry={"usage_state":"UNKNOWN","worker_usage_unverified":True,
                "worker_token_claims":worker_token_claims,"worker_cost_claim_state":bundle.get("cost_state"),
                "worker_latency_ms":bundle.get("latency_ms")
                    if isinstance(bundle.get("latency_ms"),int) and 0<=bundle["latency_ms"]<=3_600_000 else None,
                "resolved_model_source":"hash_verified_worker_raw_response" if resolved_model else "unavailable",
                "resolved_model_trust":"UNVERIFIED_WORKER_CLAIM" if resolved_model else "UNKNOWN",
                "registered_model_match":resolved_model==route["model_label"] if resolved_model else False,
                "output_bytes":len(raw or b""),"normalized_bytes":len(normalized or b"")})
        terminal_status=CognitiveLedger(db).finish_invocation(invocation_id=invocation_id,result=result)
        if terminal_status == "STALE":
            from dataclasses import replace
            result=replace(result,status=InvocationStatus.STALE,error_class="StaleLease")
        return dict(invocation),dict(route),result,worker_token_claims,estimated_cost

    def submit_result(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        task,attempt,epoch=payload.get("task_id"),payload.get("attempt_id"),payload.get("lease_epoch")
        artifact_id,result_hash,key=payload.get("artifact_id"),payload.get("result_hash"),payload.get("idempotency_key")
        usage=payload.get("usage",{})
        if not isinstance(key,str) or not key or len(key)>200 or not isinstance(usage,dict):
            raise RemoteRequestError(400,"invalid result submission")
        request_hash=digest_json({"task":task,"attempt":attempt,"epoch":epoch,"artifact":artifact_id,
            "result_hash":result_hash,"usage":usage,"worker_id":identity["worker_id"],
            "worker_instance_id":instance["worker_instance_id"]})
        with db.transaction():
            prior=db.execute("SELECT request_hash,result FROM runtime.remote_result_receipts WHERE idempotency_key=%s FOR UPDATE",(key,)).fetchone()
            if prior:
                if prior["request_hash"]!=request_hash:
                    raise RemoteRequestError(409,"result idempotency key reused with different content")
                return dict(prior["result"])
            self._active_lease(db,identity,instance,task,attempt,int(epoch))
            if not isinstance(artifact_id,str) or artifact_id!="sha256:"+str(result_hash):
                raise RemoteRequestError(422,"result artifact/hash reference mismatch")
            row=db.execute("SELECT * FROM runtime.artifacts WHERE artifact_id=%s AND producer_task_id=%s AND producer_attempt_id=%s AND worker_id=%s AND worker_instance_id=%s AND lease_epoch=%s AND verification_status='VERIFIED'",
                (artifact_id,task,attempt,identity["worker_id"],instance["worker_instance_id"],int(epoch))).fetchone()
            if not row:
                raise RemoteRequestError(422,"verified artifact lineage is missing")
            digest=digest_bytes(self.store.read(artifact_id))
            if digest!=result_hash:
                raise RemoteRequestError(422,"stored artifact failed content verification")
            task_kind=db.execute("SELECT task_type FROM runtime.tasks WHERE task_id=%s",(task,)).fetchone()["task_type"]
            cognitive=None
            if task_kind=="cognitive_invocation":
                cognitive=self._ingest_cognitive_bundle(db,task_id=task,attempt_id=attempt,epoch=int(epoch),
                    identity=identity,instance=instance,bundle_artifact_id=artifact_id)
                invocation_row,route_row,cognitive_result,worker_token_claims,estimated_cost=cognitive
                pricing=route_row.get("pricing") or {}
                if cognitive_result.status.value!="SUCCEEDED":
                    from agentic_runtime.coordinator.state import TaskState
                    target=(TaskState.CANCELLED if cognitive_result.status.value=="CANCELLED" else
                            TaskState.NEEDS_REVIEW if cognitive_result.status.value=="MALFORMED" else
                            TaskState.FAILED_TRANSIENT)
                    Coordinator(db).transition(task,attempt,int(epoch),target,actor_id="cognitive-result-controller",
                        payload={"invocation_id":invocation_row["invocation_id"],"cognitive_status":cognitive_result.status.value,
                                 "error_class":cognitive_result.error_class})
                    db.execute("UPDATE runtime.worker_instances SET status='READY',last_seen=now() WHERE worker_instance_id=%s AND status='BUSY'",
                        (instance["worker_instance_id"],))
                    failed={"task_id":task,"attempt_id":attempt,"lease_epoch":int(epoch),
                            "status":target.value,"invocation_id":invocation_row["invocation_id"]}
                    db.execute("""INSERT INTO runtime.remote_result_receipts
                        (idempotency_key,request_hash,task_id,attempt_id,worker_id,worker_instance_id,lease_epoch,result)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (key,request_hash,task,attempt,identity["worker_id"],instance["worker_instance_id"],int(epoch),json.dumps(failed)))
                    return failed
            route=db.execute("SELECT selected_executor_ref FROM runtime.routing_decisions WHERE task_id=%s AND attempt_id=%s",
                (task,attempt)).fetchone()
            executor_id=(cognitive[1]["route_id"] if cognitive else
                         route["selected_executor_ref"] if route else "remote-worker-fake")
            if cognitive:
                db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,configuration_ref,
                    capabilities,location,enabled,metadata) VALUES (%s,'cognitive_worker',%s,%s,%s,'[]','REMOTE',true,%s)
                    ON CONFLICT(executor_id) DO NOTHING""",
                    (executor_id,cognitive[1]["provider_name"],cognitive[1]["adapter_version"],
                     cognitive[1]["config_ref"],json.dumps({"model_family":cognitive[1]["model_family"],"cost_state":cognitive[1]["cost_state"]})))
            else:
                db.execute("""INSERT INTO runtime.executors(executor_id,adapter_type,backend,version,configuration_ref,
                    capabilities,location,enabled,metadata) VALUES (%s,'remote_worker','deterministic-fake','1',
                    'worker-protocol-v1','[]','REMOTE',true,'{}') ON CONFLICT(executor_id) DO NOTHING""",(executor_id,))
            started=db.execute("SELECT started_at FROM runtime.attempts WHERE attempt_id=%s",(attempt,)).fetchone()["started_at"]
            allowed_usage={"input_tokens","output_tokens","estimated_cost"}
            if set(usage)-allowed_usage or any(v is not None and (not isinstance(v,(int,float)) or v<0) for v in usage.values()):
                raise RemoteRequestError(400,"invalid usage telemetry")
            if cognitive:
                cr=cognitive_result
                db.execute("""INSERT INTO runtime.model_runs(model_run_id,task_id,attempt_id,executor_id,adapter_type,
                    provider,requested_model,resolved_model,role,input_tokens,output_tokens,estimated_cost,latency_ms,
                    status,schema_valid,started_at,completed_at,telemetry)
                    VALUES (%s,%s,%s,%s,'cognitive_worker',%s,%s,%s,%s,NULL,NULL,%s,%s,'SUCCEEDED',true,%s,now(),%s)""",
                    ("mr_"+uuid.uuid4().hex,task,attempt,executor_id,cognitive[1]["provider_name"],
                     cognitive[1]["model_label"],
                     cr.resolved_route,invocation_row["logical_role"],estimated_cost,
                     cr.latency_ms,started,json.dumps({"worker_id":identity["worker_id"],
                        "worker_instance_id":instance["worker_instance_id"],"lease_epoch":int(epoch),
                        "protocol_version":f"{instance['protocol_major']}.{instance['protocol_minor']}",
                        "cognitive_invocation_id":invocation_row["invocation_id"],"requested_route":cr.requested_route,
                        "resolved_model":cr.resolved_route,"resolved_route_state":cr.resolution_state,
                        "resolved_model_source":cr.telemetry.get("resolved_model_source"),
                        "raw_hash":cr.raw_hash,"normalized_hash":cr.normalized_hash,
                        "usage_state":cr.telemetry.get("usage_state","UNKNOWN"),
                        "worker_usage_unverified":True,"worker_token_claims":worker_token_claims,
                        "cost_state":cr.cost_state.value,"cost_estimate":estimated_cost,
                        "cost_estimate_status":"UNVERIFIED_WORKER_USAGE" if estimated_cost is not None else "UNKNOWN",
                        "price_schedule_id":pricing.get("schedule_id")})))
            else:
                db.execute("""INSERT INTO runtime.model_runs(model_run_id,task_id,attempt_id,executor_id,adapter_type,
                    status,started_at,completed_at,input_tokens,output_tokens,estimated_cost,telemetry)
                    VALUES (%s,%s,%s,%s,'remote_worker','SUCCEEDED',%s,now(),%s,%s,%s,%s)""",
                    ("mr_"+uuid.uuid4().hex,task,attempt,executor_id,started,None,None,None,
                     json.dumps({"worker_id":identity["worker_id"],"worker_instance_id":instance["worker_instance_id"],
                        "lease_epoch":int(epoch),"protocol_version":f"{instance['protocol_major']}.{instance['protocol_minor']}",
                        "usage_status":"UNKNOWN","unverified_worker_reported_usage":usage})))
            db.execute("UPDATE runtime.attempts SET telemetry=telemetry || %s WHERE attempt_id=%s",
                (json.dumps({"worker_id":identity["worker_id"],"worker_instance_id":instance["worker_instance_id"],"remote":True}),attempt))
            coordinator=Coordinator(db)
            coordinator.commit_result(task_id=task,attempt_id=attempt,lease_epoch=int(epoch),
                artifact_refs=[artifact_id],result_hash=result_hash,actor_id=identity["worker_id"],
                submission_metadata={"worker_instance_id":instance["worker_instance_id"],"lease_epoch":int(epoch)})
            evidence=coordinator.accept_verified_result(task_id=task,attempt_id=attempt,lease_epoch=int(epoch),artifact_store=self.store,
                actor_id="remote-result-verifier")
            db.execute("UPDATE runtime.worker_instances SET status='READY',last_seen=now() WHERE worker_instance_id=%s AND status='BUSY'",
                (instance["worker_instance_id"],))
            result={"task_id":task,"attempt_id":attempt,"lease_epoch":int(epoch),"status":"ACCEPTED",
                "artifact_id":artifact_id,"knowledge_id":evidence}
            db.execute("""INSERT INTO runtime.remote_result_receipts
                (idempotency_key,request_hash,task_id,attempt_id,worker_id,worker_instance_id,lease_epoch,result)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (key,request_hash,task,attempt,identity["worker_id"],instance["worker_instance_id"],int(epoch),json.dumps(result)))
            return result

    def create_sandbox(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        return self.sandbox_created(db,identity,instance,payload)

    def cleanup_sandbox(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        task,attempt,epoch,sandbox=payload.get("task_id"),payload.get("attempt_id"),int(payload.get("lease_epoch",-1)),payload.get("sandbox_id")
        row=db.execute("""SELECT s.* FROM runtime.sandboxes s JOIN runtime.attempts a ON a.attempt_id=s.attempt_id
            WHERE s.sandbox_id=%s AND s.task_id=%s AND s.attempt_id=%s AND a.worker_id=%s
              AND a.worker_instance_id=%s AND a.lease_epoch=%s""",
            (sandbox,task,attempt,identity["worker_id"],instance["worker_instance_id"],epoch)).fetchone()
        if not row:
            raise RemoteRequestError(409,"sandbox lineage mismatch")
        if payload.get("workspace_absent") is not True:
            db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_PENDING',cleanup_status='WORKER_REPORTED_PRESENT' WHERE sandbox_id=%s",(sandbox,))
            return {"status":"CLEANUP_PENDING"}
        # A cleanup RPC is evidence, not verification. The row remains pending
        # until the worker's later local inventory reconciliation confirms absence.
        db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_PENDING',cleanup_status='WORKER_REPORTED_ABSENT' WHERE sandbox_id=%s",(sandbox,))
        return {"status":"CLEANUP_PENDING"}

    def reconcile_local(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        entries=payload.get("sandboxes",[])
        if not isinstance(entries,list) or len(entries)>1000 or any(not isinstance(x,dict) for x in entries):
            raise RemoteRequestError(400,"invalid worker sandbox inventory")
        confirmed=[]; unresolved=[]
        with db.transaction():
            for item in entries:
                sandbox_id=item.get("sandbox_id"); state=item.get("state")
                if (not isinstance(sandbox_id,str) or not sandbox_id.startswith("sbx_")
                        or state not in {"PRESENT","ABSENT"}):
                    raise RemoteRequestError(400,"invalid sandbox reconciliation entry")
                row=db.execute("""SELECT * FROM runtime.sandboxes WHERE sandbox_id=%s AND task_id=%s AND attempt_id=%s
                    AND worker_id=%s AND worker_instance_id=%s AND lease_epoch=%s""",
                    (sandbox_id,item.get("task_id"),item.get("attempt_id"),identity["worker_id"],
                     item.get("worker_instance_id"),item.get("lease_epoch"))).fetchone()
                if not row:
                    tracked=db.execute("SELECT 1 FROM runtime.sandboxes WHERE sandbox_id=%s",(sandbox_id,)).fetchone()
                    if tracked:
                        raise RemoteRequestError(409,"sandbox reconciliation lineage mismatch")
                    # The create request may have failed before persistence. It
                    # is safe to discard local evidence only when no durable row
                    # exists for this unpredictable sandbox identifier.
                    unresolved.append(sandbox_id)
                    continue
                if int(item.get("lease_epoch",-1))<1:
                    raise RemoteRequestError(409,"sandbox reconciliation lineage mismatch")
                if state=="ABSENT":
                    if row["status"]!="DESTROYED":
                        db.execute("UPDATE runtime.sandboxes SET status='DESTROYED',cleanup_status='RECONCILED_ABSENT',destroyed_at=coalesce(destroyed_at,now()) WHERE sandbox_id=%s",
                            (sandbox_id,))
                        Coordinator(db)._event(event_type="REMOTE_SANDBOX_RECONCILED_ABSENT",actor_id=identity["worker_id"],
                            correlation_id=sandbox_id,task_id=row["task_id"],attempt_id=row["attempt_id"],
                            payload={"sandbox_id":sandbox_id,"worker_instance_id":item["worker_instance_id"],
                                "lease_epoch":int(item["lease_epoch"])})
                    confirmed.append(sandbox_id)
                else:
                    db.execute("UPDATE runtime.sandboxes SET status='CLEANUP_PENDING',cleanup_status='WORKER_REPORTED_PRESENT' WHERE sandbox_id=%s AND status<>'DESTROYED'",
                        (sandbox_id,))
                    unresolved.append(sandbox_id)
        return {"confirmed_absent":confirmed,"unresolved":unresolved,"untracked":unresolved}

    def cancel_attempt(self, db: Any, identity: dict[str,Any], instance: dict[str,Any], payload: Mapping[str,Any]) -> dict[str,Any]:
        task,attempt,epoch=payload.get("task_id"),payload.get("attempt_id"),int(payload.get("lease_epoch",-1))
        command_id=payload.get("command_id")
        invocation_id=payload.get("invocation_id")
        process_terminated=payload.get("process_termination_confirmed") is True
        if command_id is not None and (not isinstance(command_id,str) or not
                command_id.startswith("cmd_cancel_")):
            raise RemoteRequestError(400,"invalid cancellation command identity")
        with db.transaction():
            state=db.execute("""SELECT t.status AS task_status,t.campaign_id,t.metadata,t.plan_version_id,
                    i.invocation_id,i.status AS invocation_status
                FROM runtime.tasks t LEFT JOIN runtime.cognitive_invocations i
                  ON i.task_id=t.task_id AND i.attempt_id=%s AND i.lease_epoch=%s
                WHERE t.task_id=%s FOR UPDATE OF t""",(attempt,epoch,task)).fetchone()
            if not state:
                raise RemoteRequestError(404,"cancellation task not found")
            if command_id and state["plan_version_id"]:
                plan_version=state["plan_version_id"]
                accepted=db.execute("""SELECT 1 FROM runtime.coordination_plan_versions
                    WHERE plan_version_id=%s AND status='ACCEPTED'""",(plan_version,)).fetchone() if plan_version else None
                if not accepted:
                    raise RemoteRequestError(403,"cancellation command is outside an accepted coordination plan")
            if state["task_status"]=="CANCELLED" and state["invocation_status"] in {None,"CANCELLED"}:
                if command_id:
                    db.execute("""UPDATE runtime.worker_commands SET status='ACKNOWLEDGED',acknowledged_at=coalesce(acknowledged_at,now())
                        WHERE command_id=%s AND worker_instance_id=%s AND task_id=%s AND attempt_id=%s
                          AND lease_epoch=%s AND command='CANCEL_ATTEMPT' AND status IN ('DELIVERED','ACKNOWLEDGED')""",
                        (command_id,instance["worker_instance_id"],task,attempt,epoch))
                return {"status":"ALREADY_CANCELLED","task_id":task,"attempt_id":attempt,
                        "lease_epoch":epoch,"command_id":command_id}
            self._active_lease(db,identity,instance,task,attempt,epoch)
            if command_id:
                command=db.execute("""SELECT status FROM runtime.worker_commands WHERE command_id=%s
                    AND worker_instance_id=%s AND task_id=%s AND attempt_id=%s AND lease_epoch=%s
                    AND command='CANCEL_ATTEMPT' FOR UPDATE""",
                    (command_id,instance["worker_instance_id"],task,attempt,epoch)).fetchone()
                if not command or command["status"]!="DELIVERED":
                    raise RemoteRequestError(409,"cancellation command is not durably delivered")
            if state["invocation_id"]:
                if invocation_id != state["invocation_id"]:
                    raise RemoteRequestError(409,"cancellation invocation identity mismatch")
                from agentic_runtime.cognitive.ledger import CognitiveLedger
                CognitiveLedger(db).finish_cancelled_invocation(invocation_id=invocation_id,
                    worker_id=identity["worker_id"],worker_instance_id=instance["worker_instance_id"],
                    attempt_id=attempt,lease_epoch=epoch,command_id=command_id,
                    process_termination_confirmed=process_terminated)
            from agentic_runtime.coordinator.state import TaskState
            Coordinator(db).transition(task,attempt,epoch,TaskState.CANCELLED,actor_id=identity["worker_id"],
                payload={"worker_instance_id":instance["worker_instance_id"],"remote_cancel":True,
                    "command_id":command_id,"process_termination_confirmed":process_terminated})
            db.execute("UPDATE runtime.worker_instances SET status='READY',last_seen=now() WHERE worker_instance_id=%s AND status='BUSY'",
                (instance["worker_instance_id"],))
            if command_id:
                db.execute("""UPDATE runtime.worker_commands SET status='ACKNOWLEDGED',acknowledged_at=now(),
                    metadata=metadata || %s::jsonb WHERE command_id=%s AND status='DELIVERED'""",
                    (json.dumps({"process_termination_confirmed":process_terminated}),command_id))
                Coordinator(db)._event(event_type="REMOTE_WORKER_COMMAND_ACKNOWLEDGED",actor_id=identity["worker_id"],
                    correlation_id=command_id,campaign_id=state["campaign_id"],task_id=task,attempt_id=attempt,
                    payload={"command_id":command_id,"worker_instance_id":instance["worker_instance_id"],
                        "lease_epoch":epoch,"command":"CANCEL_ATTEMPT",
                        "process_termination_confirmed":process_terminated})
            return {"status":"CANCELLED","task_id":task,"attempt_id":attempt,
                    "lease_epoch":epoch,"command_id":command_id,
                    "process_termination_confirmed":process_terminated}

    def admin_command(self, db: Any, headers: Mapping[str,str], payload: Mapping[str,Any]) -> dict[str,Any]:
        bearer=headers.get("Authorization","")
        supplied=bearer[7:] if bearer.startswith("Bearer ") else ""
        if not self.admin_token_hash or not hmac.compare_digest(hashlib.sha256(supplied.encode()).hexdigest(),self.admin_token_hash):
            raise RemoteRequestError(401,"control authorization failed")
        command=payload.get("command"); instance_id=payload.get("worker_instance_id")
        with db.transaction():
            row=db.execute("SELECT * FROM runtime.worker_instances WHERE worker_instance_id=%s FOR UPDATE",(instance_id,)).fetchone()
            if not row or row["status"] in {"OFFLINE","REJECTED"}:
                raise RemoteRequestError(404,"active worker instance not found")
            if command=="DRAIN":
                db.execute("UPDATE runtime.worker_instances SET status='DRAINING' WHERE worker_instance_id=%s",(instance_id,))
                db.execute("UPDATE runtime.workers SET status='DRAINING' WHERE worker_id=%s",(row["worker_id"],))
                task=attempt=None; epoch=None
            elif command=="CANCEL_ATTEMPT":
                task,attempt,epoch=payload.get("task_id"),payload.get("attempt_id"),int(payload.get("lease_epoch",-1))
                lease=db.execute("""SELECT 1 FROM runtime.leases l JOIN runtime.attempts a USING(attempt_id)
                    WHERE l.task_id=%s AND l.attempt_id=%s AND l.lease_epoch=%s AND l.status='ACTIVE'
                      AND a.worker_id=%s AND a.worker_instance_id=%s AND l.lease_until>now()""",
                    (task,attempt,epoch,row["worker_id"],instance_id)).fetchone()
                if not lease: raise RemoteRequestError(409,"cancel request has stale lease")
            else: raise RemoteRequestError(400,"unsupported worker command")
            command_id=("cmd_cancel_" if command=="CANCEL_ATTEMPT" else "cmd_")+uuid.uuid4().hex
            db.execute("""INSERT INTO runtime.worker_commands(command_id,worker_instance_id,task_id,attempt_id,lease_epoch,command,status)
                VALUES (%s,%s,%s,%s,%s,%s,'PENDING')""",(command_id,instance_id,task,attempt,epoch,command))
            Coordinator(db)._event(event_type="REMOTE_WORKER_COMMAND_QUEUED",actor_id="governance",
                correlation_id=command_id,task_id=task,attempt_id=attempt,
                payload={"command_id":command_id,"worker_id":row["worker_id"],
                    "worker_instance_id":instance_id,"command":command,"lease_epoch":epoch})
            return {"command_id":command_id,"status":"PENDING","command":command}

    def handle(self, method: str, path: str, headers: Mapping[str,str], body: bytes) -> tuple[int,Any]:
        if path=="/health/live" and method=="GET":
            return 200,{"status":"LIVE"}
        if path=="/health/ready" and method=="GET":
            ready,checks=self.readiness()
            return (200 if ready else 503),{"status":"READY" if ready else "NOT_READY","checks":checks}
        if path=="/health" and method=="GET":
            ready,checks=self.readiness()
            return (200 if ready else 503),{"status":"READY" if ready else "NOT_READY",
                "protocol_version":f"{self.protocol_major}.{self.protocol_minor}","checks":checks}
        db=self.db()
        try:
            if method=="POST" and path=="/v1/admin/command":
                payload=json.loads(body)
                return 200,self.admin_command(db,headers,payload)
            parts=urlsplit(path).path.split("?")[0].split("/")
            if method=="POST" and path=="/v1/register":
                payload=json.loads(body); return 200,self.register(db,headers,payload)
            if method=="PUT" and len(parts)==4 and parts[:3]==["","v1","artifacts"]:
                identity,instance=self._identity(db,headers,instance=True)
                major,minor=parse_protocol(headers.get("X-Agentic-Protocol",""))
                if major!=instance["protocol_major"] or minor!=instance["protocol_minor"]:
                    raise RemoteRequestError(426,"worker artifact protocol mismatch")
                return 201,self.upload_artifact(db,headers,identity,instance,parts[3],body)
            payload=json.loads(body) if body else {}
            major,minor=self._protocol(headers.get("X-Agentic-Protocol",""),payload)
            identity,instance=self._identity(db,headers,instance=True)
            if major!=instance["protocol_major"] or minor!=instance["protocol_minor"]:
                raise RemoteRequestError(426,"worker instance protocol mismatch")
            request_id=str(payload.get("request_id") or uuid.uuid4().hex)
            routes={
                ("POST","/v1/heartbeat"):lambda:self.heartbeat(db,identity,instance,payload,request_id,major,minor),
                ("POST","/v1/claim"):lambda:self.claim(db,identity,instance,payload,request_id),
                ("POST","/v1/attempt/start"):lambda:self.start_attempt(db,identity,instance,payload),
                ("POST","/v1/sandbox/created"):lambda:self.create_sandbox(db,identity,instance,payload),
                ("POST","/v1/sandbox/cleanup"):lambda:self.cleanup_sandbox(db,identity,instance,payload),
                ("POST","/v1/reconcile-local"):lambda:self.reconcile_local(db,identity,instance,payload),
                ("POST","/v1/cancel"):lambda:self.cancel_attempt(db,identity,instance,payload),
                ("POST","/v1/result"):lambda:self.submit_result(db,identity,instance,payload),
            }
            action=routes.get((method,path))
            if action is None: raise RemoteRequestError(404,"unknown worker operation")
            return 200,action()
        except RemoteRequestError as exc:
            return exc.status,{"error":str(exc)}
        except (ValueError,KeyError,TypeError,json.JSONDecodeError) as exc:
            return 400,{"error":str(exc)}
        except Exception as exc:
            logging.getLogger("agentic_runtime.remote.control").warning(json.dumps({
                "event":"operation_failed","operation":f"{method} {path}",
                "error_class":type(exc).__name__},sort_keys=True))
            return 500,{"error":"control-plane operation failed"}
        finally:
            db.close()


class RemoteHTTPServer(ThreadingHTTPServer):
    daemon_threads=True
    allow_reuse_address=True

    def __init__(self, address: tuple[str,int], service: RemoteControlPlane, *,
                 tls_context: ssl.SSLContext | None=None, max_connections: int=8,
                 request_timeout_seconds: float=30.0) -> None:
        if not isinstance(max_connections,int) or not 1 <= max_connections <= 256:
            raise ValueError("max_connections must be between 1 and 256")
        if not 0 < request_timeout_seconds <= 300:
            raise ValueError("request timeout must be between 0 and 300 seconds")
        self.service=service
        self.tls_context=tls_context
        self.request_timeout_seconds=request_timeout_seconds
        self._connection_slots=threading.BoundedSemaphore(max_connections)
        super().__init__(address,RemoteRequestHandler)

    def get_request(self):
        sock,address=super().get_request()
        sock.settimeout(self.request_timeout_seconds)
        if self.tls_context:
            sock=self.tls_context.wrap_socket(sock,server_side=True,do_handshake_on_connect=False)
        return sock,address

    def process_request(self, request, client_address):
        if not self._connection_slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            except OSError: pass
            self.shutdown_request(request)
            return
        try: super().process_request(request,client_address)
        except Exception:
            self._connection_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try: super().process_request_thread(request,client_address)
        finally: self._connection_slots.release()

    def handle_error(self, request, client_address):
        exc_type,_,_=sys.exc_info()
        logging.getLogger("agentic_runtime.remote.control").warning(json.dumps({
            "event":"connection_failed","error_class":getattr(exc_type,"__name__","UnknownError")},sort_keys=True))


class RemoteRequestHandler(BaseHTTPRequestHandler):
    server: RemoteHTTPServer
    protocol_version="HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # The stdlib logger never receives Authorization header/token values.
        return

    def _handle(self) -> None:
        try:
            length_header=self.headers.get("Content-Length")
            length=0 if self.command=="GET" and length_header is None else int(length_header or "-1")
            limit=MAX_ARTIFACT_BYTES if self.command=="PUT" else MAX_CONTROL_BYTES
            if length<0 or length>limit:
                raise RemoteRequestError(413,"request body outside allowed size")
            body=self.rfile.read(length)
            if len(body)!=length:
                raise RemoteRequestError(400,"incomplete request body")
            request_headers=HTTPMessage()
            for name,value in self.headers.items():
                if name.lower() not in {CLIENT_CERT_HEADER.lower(), LEGACY_CLIENT_CERT_HEADER.lower()}:
                    request_headers.add_header(name,value)
            if isinstance(self.connection,ssl.SSLSocket):
                peer_certificate=self.connection.getpeercert(binary_form=True)
                if peer_certificate:
                    request_headers[CLIENT_CERT_HEADER]=hashlib.sha256(peer_certificate).hexdigest()
            status,value=self.server.service.handle(self.command,self.path,request_headers,body)
        except RemoteRequestError as exc:
            status,value=exc.status,{"error":str(exc)}
        except (ValueError,UnicodeDecodeError):
            status,value=400,{"error":"malformed request"}
        except Exception as exc:
            logging.getLogger("agentic_runtime.remote.control").warning(json.dumps({
                "event":"request_failed","error_class":type(exc).__name__},sort_keys=True))
            status,value=500,{"error":"request failed"}
        raw=json.dumps(value,sort_keys=True,separators=(",",":")).encode()
        self.send_response(status)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(raw)))
        self.send_header("Connection","close")
        self.end_headers()
        try: self.wfile.write(raw)
        except (BrokenPipeError,ConnectionResetError): pass
        self.close_connection=True

    do_GET=_handle
    do_POST=_handle
    do_PUT=_handle


def serve_local(service: RemoteControlPlane, host: str="127.0.0.1", port: int=0,
                *, max_connections: int=8) -> RemoteHTTPServer:
    if host not in {"127.0.0.1","::1","localhost"}:
        raise ValueError("validation server must bind to loopback only")
    server=RemoteHTTPServer((host,port),service,max_connections=max_connections)
    return server


def serve_secure(service: RemoteControlPlane, host: str, port: int, tls_context: ssl.SSLContext,
                 *, max_connections: int=8) -> RemoteHTTPServer:
    if tls_context.verify_mode != ssl.CERT_REQUIRED:
        raise ValueError("cross-host worker service requires mutual TLS client certificates")
    service.require_client_certificate=True
    return RemoteHTTPServer((host,port),service,tls_context=tls_context,max_connections=max_connections)


def build_server_tls_context(cert_file: str, key_file: str, client_ca_file: str) -> ssl.SSLContext:
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version=ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=cert_file,keyfile=key_file)
    context.load_verify_locations(cafile=client_ca_file)
    context.verify_mode=ssl.CERT_REQUIRED
    return context
