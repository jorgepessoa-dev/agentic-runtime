from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

from psycopg.types.json import Jsonb

from .contracts import (CognitiveCapabilities, CognitiveInvocationRequest,
                        CognitiveInvocationResult)
from .validation import canonical_json, sha256


class InvocationConflict(RuntimeError):
    pass


class StaleInvocation(RuntimeError):
    pass


class CognitiveBudgetExceeded(ValueError):
    pass


def _event(db: Any, *, task_id: str, campaign_id: str | None, attempt_id: str,
           invocation_id: str, event_type: str, payload: dict[str, Any]) -> None:
    payload_json = Jsonb(payload)
    digest = db.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS hash",
                        (payload_json,)).fetchone()["hash"]
    db.execute("""INSERT INTO runtime.events
        (event_id,event_type,campaign_id,task_id,attempt_id,actor_type,actor_id,
         correlation_id,schema_version,payload,payload_hash)
        VALUES (%s,%s,%s,%s,%s,'COGNITIVE_WORKER',%s,%s,'cognitive.v1',%s,%s)""",
        ("evt_" + uuid.uuid4().hex, event_type, campaign_id, task_id, attempt_id,
         invocation_id, invocation_id, payload_json, digest))


class CognitiveLedger:
    """Durable capability and invocation ledger; never stores credentials or prompts."""
    def __init__(self, db: Any) -> None:
        self.db = db

    def check_campaign_call_budget(self, *, campaign_id: str, route_id: str,
                                   route_concurrency_limit: int, lock_campaign: bool = True) -> None:
        """Validate hard call/concurrency caps while holding the campaign lock.

        The caller inserts the PENDING invocation in this same transaction,
        making that row the durable pre-dispatch call reservation. A crashed
        invocation remains charged to the call cap; reconciliation only frees
        concurrency by making it terminal.
        """
        if lock_campaign:
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"cognitive-campaign:{campaign_id}",))
        campaign = self.db.execute("SELECT status,budget FROM runtime.campaigns WHERE campaign_id=%s",
                                   (campaign_id,)).fetchone()
        if not campaign or campaign["status"] != "ACTIVE":
            raise StaleInvocation("cognitive work requires an active campaign")
        limits = campaign["budget"] or {}
        call_limit = limits.get("max_cognitive_calls")
        route_limits = limits.get("max_cognitive_calls_per_route")
        if not isinstance(call_limit, int) or isinstance(call_limit, bool) or call_limit < 1:
            raise CognitiveBudgetExceeded("cognitive campaign requires a positive max_cognitive_calls")
        if isinstance(route_limits, int) and not isinstance(route_limits, bool):
            route_limit = route_limits
        elif isinstance(route_limits, dict):
            route_limit = route_limits.get(route_id)
        else:
            route_limit = None
        if not isinstance(route_limit, int) or isinstance(route_limit, bool) or route_limit < 1:
            raise CognitiveBudgetExceeded("cognitive campaign requires an explicit positive per-route call cap")
        if "max_cost" in limits and limits["max_cost"] is not None:
            route_cost = self.db.execute("SELECT cost_state FROM runtime.cognitive_routes WHERE route_id=%s",
                                         (route_id,)).fetchone()
            if not route_cost or route_cost["cost_state"] not in {"MEASURED", "PROVIDER_REPORTED"}:
                raise CognitiveBudgetExceeded("hard monetary campaign cap cannot use a route with unknown or unpriced cost")
        counts = self.db.execute("""SELECT count(*) AS calls,
            count(*) FILTER (WHERE route_id=%s) AS route_calls,
            count(*) FILTER (WHERE status='PENDING') AS active_calls,
            count(*) FILTER (WHERE route_id=%s AND status='PENDING') AS route_active
            FROM runtime.cognitive_invocations WHERE campaign_id=%s""",
            (route_id,route_id,campaign_id)).fetchone()
        campaign_concurrency = limits.get("max_concurrent_cognitive_calls", 1)
        if not isinstance(campaign_concurrency, int) or isinstance(campaign_concurrency, bool) or campaign_concurrency < 1:
            raise CognitiveBudgetExceeded("cognitive campaign concurrency cap must be a positive integer")
        effective_route_concurrency = min(campaign_concurrency, route_concurrency_limit)
        if counts["calls"] >= call_limit or counts["route_calls"] >= route_limit:
            raise CognitiveBudgetExceeded("cognitive campaign call budget exhausted")
        if counts["active_calls"] >= campaign_concurrency or counts["route_active"] >= effective_route_concurrency:
            raise CognitiveBudgetExceeded("cognitive campaign or route concurrency cap reached")

    def observe_route(self, *, capabilities: CognitiveCapabilities, worker_id: str,
                      adapter_version: str, resource_class: str, config_ref: str,
                      pricing: Mapping[str, Any] | None = None,
                      observed_at: datetime | None = None,
                      event_type: str = "COGNITIVE_ROUTE_PROBED",
                      evidence_ref: str | None = None) -> None:
        if event_type not in {"COGNITIVE_ROUTE_PROBED", "COGNITIVE_ROUTE_REGISTERED_FROM_QUALIFICATION"}:
            raise ValueError("unsupported cognitive route event type")
        if any(not value or len(value) > 200 for value in
               (capabilities.route_id, capabilities.provider, capabilities.model_family, worker_id)):
            raise ValueError("route identity fields are required and bounded")
        if capabilities.tool_use:
            raise ValueError("cognitive proposal routes must be tool-disabled")
        pricing_data = dict(pricing or {})
        if pricing_data:
            expected = {"schedule_id", "input_usd_per_million", "output_usd_per_million", "source_ref", "observed_at"}
            if set(pricing_data) != expected:
                raise ValueError("route pricing schedule fields must match the versioned public-price contract")
            for key in ("input_usd_per_million", "output_usd_per_million"):
                value = pricing_data[key]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                    raise ValueError("route pricing rates must be finite non-negative numbers")
            # A configured rate schedule supports a labelled estimate only;
            # it does not establish an actual billed/verified cost state.
        now = observed_at or datetime.now(timezone.utc)
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"cognitive-route:{capabilities.route_id}",))
            self.db.execute("""INSERT INTO runtime.cognitive_routes
                (route_id,worker_id,provider_name,model_family,model_label,adapter_version,
                 supported_roles,declared_capabilities,observed_capabilities,structured_output,
                 tool_use,max_input_tokens,concurrency_limit,supports_cancel,cost_state,health,
                 last_probe,resource_class,config_ref,pricing)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(route_id) DO UPDATE SET worker_id=EXCLUDED.worker_id,
                 provider_name=EXCLUDED.provider_name,model_family=EXCLUDED.model_family,
                 model_label=EXCLUDED.model_label,adapter_version=EXCLUDED.adapter_version,
                 supported_roles=EXCLUDED.supported_roles,declared_capabilities=EXCLUDED.declared_capabilities,
                 observed_capabilities=EXCLUDED.observed_capabilities,structured_output=EXCLUDED.structured_output,
                 tool_use=false,max_input_tokens=EXCLUDED.max_input_tokens,
                 concurrency_limit=EXCLUDED.concurrency_limit,supports_cancel=EXCLUDED.supports_cancel,
                 cost_state=EXCLUDED.cost_state,health=EXCLUDED.health,last_probe=EXCLUDED.last_probe,
                 resource_class=EXCLUDED.resource_class,config_ref=EXCLUDED.config_ref,
                 pricing=EXCLUDED.pricing,updated_at=now()""",
                (capabilities.route_id, worker_id, capabilities.provider, capabilities.model_family,
                 capabilities.requested_model, adapter_version, Jsonb(sorted(capabilities.supported_roles)),
                 Jsonb(sorted(capabilities.declared)), Jsonb(sorted(capabilities.observed)),
                 capabilities.structured_output, capabilities.max_input_tokens,
                 capabilities.concurrency_limit, capabilities.supports_cancel,
                 capabilities.cost_state.value, capabilities.health, now, resource_class, config_ref,
                 Jsonb(pricing_data)))
            payload = {"route_id": capabilities.route_id, "provider": capabilities.provider,
                       "model_family": capabilities.model_family, "health": capabilities.health,
                       "observed_capabilities": sorted(capabilities.observed),
                       "cost_state": capabilities.cost_state.value,
                       "last_probe": now.isoformat(), "evidence_ref": evidence_ref}
            digest = self.db.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS hash",
                                     (Jsonb(payload),)).fetchone()["hash"]
            self.db.execute("""INSERT INTO runtime.events
                (event_id,event_type,actor_type,actor_id,correlation_id,schema_version,payload,payload_hash)
                VALUES (%s,%s,'COGNITIVE_WORKER',%s,%s,'cognitive.v1',%s,%s)""",
                ("evt_" + uuid.uuid4().hex, event_type, worker_id, capabilities.route_id, Jsonb(payload), digest))

    def begin_invocation(self, *, invocation_id: str, idempotency_key: str,
                         request: CognitiveInvocationRequest, request_hash: str,
                         worker_id: str, worker_instance_id: str) -> tuple[str, bool]:
        if len(request_hash) != 64 or any(c not in "0123456789abcdef" for c in request_hash):
            raise ValueError("canonical request hash required")
        with self.db.transaction():
            prior = self.db.execute("SELECT invocation_id,request_hash,status FROM runtime.cognitive_invocations WHERE idempotency_key=%s FOR UPDATE",
                                    (idempotency_key,)).fetchone()
            if prior:
                if prior["request_hash"] != request_hash:
                    raise InvocationConflict("invocation idempotency key reused with a different request")
                # The same attempt must never invoke a paid/stochastic route a
                # second time just because its start response was retried.
                return prior["invocation_id"], False
            lease = self.db.execute("""SELECT l.lease_until,t.campaign_id,t.status AS task_status,
                       wi.status AS worker_status,cr.worker_id AS route_worker
                FROM runtime.leases l JOIN runtime.tasks t USING(task_id)
                JOIN runtime.attempts a USING(attempt_id)
                JOIN runtime.worker_instances wi ON wi.worker_instance_id=a.worker_instance_id
                JOIN runtime.cognitive_routes cr ON cr.route_id=%s
                WHERE l.task_id=%s AND l.attempt_id=%s AND l.lease_epoch=%s
                  AND l.worker_id=%s AND l.status='ACTIVE' AND l.lease_until>now()
                  AND cr.health='HEALTHY' AND (cr.last_probe>now()-interval '10 minutes'
                    OR EXISTS(SELECT 1 FROM runtime.cognitive_qualification_acceptances qa
                     WHERE qa.route_id=cr.route_id AND qa.campaign_id=t.campaign_id
                       AND qa.qualified_at=cr.last_probe AND qa.expires_at>now()))
                  AND cr.last_probe<=now()
                  AND a.worker_instance_id=%s AND wi.worker_id=%s""",
                (request.requested_route, request.task_id, request.attempt_id,
                 request.lease_epoch, worker_id, worker_instance_id, worker_id)).fetchone()
            attempt_id = request.attempt_id
            if (not lease or lease["route_worker"] != worker_id
                    or lease["task_status"] != "RUNNING" or lease["worker_status"] not in {"BUSY", "READY"}
                    or (request.campaign_id and request.campaign_id != lease["campaign_id"])):
                raise StaleInvocation("no current authorized worker lease for cognitive invocation")
            campaign_id = lease["campaign_id"]
            if campaign_id:
                # A campaign row lock serializes call and concurrency caps with
                # other cognitive starts. The invocation row is the durable
                # reservation: it is committed before the worker receives the
                # request, and it remains charged if the worker later vanishes.
                route = self.db.execute("SELECT concurrency_limit FROM runtime.cognitive_routes WHERE route_id=%s",
                                         (request.requested_route,)).fetchone()
                self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                                (f"cognitive-campaign:{campaign_id}",))
                # Another identical start may have committed while this
                # transaction waited for the campaign reservation lock.
                prior = self.db.execute("SELECT invocation_id,request_hash FROM runtime.cognitive_invocations WHERE idempotency_key=%s FOR UPDATE",
                                        (idempotency_key,)).fetchone()
                if prior:
                    if prior["request_hash"] != request_hash:
                        raise InvocationConflict("invocation idempotency key reused with a different request")
                    return prior["invocation_id"], False
                self.check_campaign_call_budget(campaign_id=campaign_id,route_id=request.requested_route,
                    route_concurrency_limit=int(route["concurrency_limit"]),lock_campaign=False)
            self.db.execute("""INSERT INTO runtime.cognitive_invocations
                (invocation_id,idempotency_key,request_hash,task_id,campaign_id,attempt_id,worker_id,
                 worker_instance_id,lease_epoch,route_id,logical_role,response_schema,response_schema_hash,
                 provider_name,requested_route,
                 resolution_state,adapter_version,status,started_at,usage_state,cost_state)
                SELECT %s,%s,%s,%s,t.campaign_id,%s,%s,%s,%s,%s,%s,%s,%s,cr.provider_name,%s,
                       'UNKNOWN',cr.adapter_version,'PENDING',now(),'UNKNOWN',cr.cost_state
                FROM runtime.tasks t JOIN runtime.cognitive_routes cr ON cr.route_id=%s
                WHERE t.task_id=%s""",
                (invocation_id,idempotency_key,request_hash,request.task_id,attempt_id,worker_id,
                 worker_instance_id,request.lease_epoch,request.requested_route,request.logical_role,
                 Jsonb(dict(request.response_schema)),sha256(canonical_json(request.response_schema)),
                 request.requested_route,request.requested_route,request.task_id))
            _event(self.db, task_id=request.task_id, campaign_id=lease["campaign_id"],
                   attempt_id=attempt_id, invocation_id=invocation_id,
                   event_type="COGNITIVE_INVOCATION_STARTED",
                   payload={"route_id":request.requested_route,"role":request.logical_role,
                            "request_hash":request_hash,"worker_instance_id":worker_instance_id,
                            "lease_epoch":request.lease_epoch})
            return invocation_id, True

    def finish_invocation(self, *, invocation_id: str, result: CognitiveInvocationResult) -> str:
        if result.invocation_id != invocation_id:
            raise InvocationConflict("result invocation identity mismatch")
        with self.db.transaction():
            current = self.db.execute("SELECT * FROM runtime.cognitive_invocations WHERE invocation_id=%s FOR UPDATE",
                                      (invocation_id,)).fetchone()
            if not current:
                raise KeyError("unknown cognitive invocation")
            if current["status"] != "PENDING":
                same = (current["status"] == result.status.value
                    and current["raw_hash"] == result.raw_hash
                    and current["normalized_hash"] == result.normalized_hash
                    and current["worker_id"] == result.worker_id
                    and current["worker_instance_id"] == result.worker_instance_id
                    and current["provider_name"] == result.provider
                    and current["requested_route"] == result.requested_route
                    and current["input_tokens"] == result.input_tokens
                    and current["output_tokens"] == result.output_tokens
                    and current["cached_tokens"] == result.cached_tokens
                    and current["reasoning_tokens"] == result.reasoning_tokens
                    and current["cost_state"] == result.cost_state.value
                    and current["provider_cost"] == (result.monetary_cost if result.cost_state.value in
                        {"MEASURED", "PROVIDER_REPORTED"} else None))
                if same:
                    return "DUPLICATE"
                raise InvocationConflict("late or conflicting terminal cognitive result")
            if (current["worker_id"] != result.worker_id
                    or current["worker_instance_id"] != result.worker_instance_id
                    or current["provider_name"] != result.provider
                    or current["requested_route"] != result.requested_route):
                raise InvocationConflict("result provenance does not match the authorized route/worker")
            status = result.status.value
            if status == "SUCCEEDED":
                valid_lease = self.db.execute("""SELECT 1 FROM runtime.leases l
                    JOIN runtime.attempts a USING(attempt_id)
                    WHERE l.task_id=%s AND l.attempt_id=%s AND l.lease_epoch=%s AND l.worker_id=%s
                      AND a.worker_instance_id=%s AND l.status='ACTIVE' AND l.lease_until>now()""",
                    (current["task_id"],current["attempt_id"],current["lease_epoch"],
                     current["worker_id"],current["worker_instance_id"])).fetchone()
                if not valid_lease:
                    status = "STALE"
            cost = result.monetary_cost if result.cost_state.value in {"MEASURED", "PROVIDER_REPORTED"} else None
            self.db.execute("""UPDATE runtime.cognitive_invocations SET
                status=%s,completed_at=%s,latency_ms=%s,resolved_route=%s,resolution_state=%s,
                model_version=%s,raw_artifact_id=%s,raw_hash=%s,normalized_artifact_id=%s,
                normalized_hash=%s,input_tokens=%s,output_tokens=%s,cached_tokens=%s,
                reasoning_tokens=%s,provider_cost=%s,usage_state=%s,cost_state=%s,
                finish_reason=%s,error_class=%s,stderr_class=%s,tool_summary=%s,telemetry=%s
                WHERE invocation_id=%s""",
                (status,result.completed_at,result.latency_ms,result.resolved_route,result.resolution_state,
                 result.model_version,result.raw_artifact_id,result.raw_hash,result.normalized_artifact_id,
                 result.normalized_hash,result.input_tokens,result.output_tokens,result.cached_tokens,
                 result.reasoning_tokens,cost,result.telemetry.get("usage_state","UNKNOWN"),
                 result.cost_state.value,result.finish_reason,result.error_class,result.stderr_class,
                 Jsonb(list(result.tool_summary)),Jsonb(dict(result.telemetry)),invocation_id))
            _event(self.db, task_id=current["task_id"], campaign_id=current["campaign_id"],
                   attempt_id=current["attempt_id"], invocation_id=invocation_id,
                   event_type="COGNITIVE_INVOCATION_FINISHED",
                   payload={"route_id":current["route_id"],"status":status,
                            "raw_hash":result.raw_hash,"normalized_hash":result.normalized_hash,
                            "latency_ms":result.latency_ms,"cost_state":result.cost_state.value,
                            "input_tokens":result.input_tokens,"output_tokens":result.output_tokens})
            return status

    def finish_cancelled_invocation(self, *, invocation_id: str, worker_id: str,
                                    worker_instance_id: str, attempt_id: str, lease_epoch: int,
                                    command_id: str | None, process_termination_confirmed: bool) -> str:
        """Terminalize an in-flight invocation after authenticated process cancellation.

        Cancellation carries no provider output or artifact references. The worker
        identity and fencing epoch must match the original durable invocation.
        """
        with self.db.transaction():
            row = self.db.execute("""SELECT * FROM runtime.cognitive_invocations
                WHERE invocation_id=%s FOR UPDATE""", (invocation_id,)).fetchone()
            if not row:
                raise KeyError("unknown cognitive invocation")
            identity_matches = (row["worker_id"] == worker_id
                and row["worker_instance_id"] == worker_instance_id
                and row["attempt_id"] == attempt_id and row["lease_epoch"] == lease_epoch)
            if not identity_matches:
                raise InvocationConflict("cancellation identity does not match invocation fence")
            if row["status"] == "CANCELLED":
                return "DUPLICATE"
            if row["status"] != "PENDING":
                raise StaleInvocation("only an in-flight cognitive invocation can be cancelled")
            self.db.execute("""UPDATE runtime.cognitive_invocations SET status='CANCELLED',
                completed_at=now(),latency_ms=greatest(0,(extract(epoch FROM (now()-started_at))*1000)::bigint),
                error_class='CANCELLED',telemetry=telemetry || %s::jsonb
                WHERE invocation_id=%s""",
                (json.dumps({"control_plane_cancel":True,"cancel_command_id":command_id,
                    "process_termination_confirmed":bool(process_termination_confirmed)}),invocation_id))
            _event(self.db, task_id=row["task_id"], campaign_id=row["campaign_id"],
                attempt_id=attempt_id, invocation_id=invocation_id,
                event_type="COGNITIVE_INVOCATION_FINISHED",
                payload={"route_id":row["route_id"],"status":"CANCELLED",
                    "raw_hash":None,"normalized_hash":None,"cancel_command_id":command_id,
                    "process_termination_confirmed":bool(process_termination_confirmed)})
            return "CANCELLED"
