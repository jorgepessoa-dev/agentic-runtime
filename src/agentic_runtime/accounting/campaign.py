"""Durable campaign resource reservations with conservative unknown-usage handling."""
from __future__ import annotations

import uuid
import math
from typing import Any, Mapping

from agentic_runtime.persistence.events import evolution_event as _evolution_event, jsonb as _jsonb


DIMENSIONS = frozenset({"experiment_units", "child_tasks", "hypotheses", "challengers",
                        "evaluations", "attempts", "wall_time", "tokens_input",
                        "tokens_output", "monetary_cost"})
LIMIT_KEYS = {"experiment_units":"max_experiment_units","monetary_cost": "max_cost", "tokens_input": "max_tokens_input",
              "tokens_output": "max_tokens_output", "wall_time": "max_wall_time"}


class CampaignAccounting:
    """Reservations serialize on the campaign row; settlement is idempotent.

    Unknown consumption settles against the full reservation (a conservative
    cap) and is marked UNKNOWN. A hard dimension cannot be reserved without an
    explicit upper bound. Overruns are persisted then stop the campaign.
    """

    def __init__(self, db: Any) -> None:
        self.db = db

    def reserve(self, *, campaign_id: str, stage: str, idempotency_key: str,
                amounts: Mapping[str, int | float | None], budget_class: str = "EXPLOIT",
                task_id: str | None = None, attempt_id: str | None = None,
                usage_bounded: bool = False) -> str:
        if not amounts or set(amounts) - DIMENSIONS or budget_class not in {"EXPLOIT", "EXPLORE"}:
            raise ValueError("invalid campaign resource reservation")
        if any(value is not None and (not math.isfinite(float(value)) or float(value) < 0) for value in amounts.values()):
            raise ValueError("resource reservation cannot be negative")
        with self.db.transaction():
            campaign = self.db.execute("SELECT * FROM runtime.improvement_campaigns WHERE campaign_id=%s FOR UPDATE",(campaign_id,)).fetchone()
            if not campaign or campaign["status"] in {"STOPPED","COMPLETED"}:
                raise ValueError("campaign is stopped or absent")
            prior = self.db.execute("SELECT reservation_id FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",(campaign_id,idempotency_key)).fetchone()
            if prior:
                existing={r["dimension"]:r["reserved"] for r in self.db.execute("SELECT dimension,reserved FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",(prior["reservation_id"],)).fetchall()}
                if existing != dict(amounts):
                    raise ValueError("reservation idempotency key reused with different amounts")
                return prior["reservation_id"]
            budget = campaign["budget"] or {}
            for dimension, limit_key in LIMIT_KEYS.items():
                if limit_key in budget and dimension not in amounts:
                    raise ValueError(f"hard campaign limit {limit_key} requires an explicit bounded reservation for {dimension}")
            if "max_tokens" in budget and not {"tokens_input","tokens_output"}.issubset(amounts):
                raise ValueError("hard campaign limit max_tokens requires input and output token bounds")
            rows = self.db.execute("""SELECT d.dimension,
                coalesce(sum(d.consumed) FILTER (WHERE r.status IN ('SETTLED','UNKNOWN')),0) AS consumed,
                coalesce(sum(d.reserved) FILTER (WHERE r.status='RESERVED'),0) AS reserved
                FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
                WHERE r.campaign_id=%s GROUP BY d.dimension""",(campaign_id,)).fetchall()
            totals = {row["dimension"]:(float(row["consumed"] or 0),float(row["reserved"] or 0)) for row in rows}
            policy_limits=campaign["limits"] or {}
            policy_keys={"child_tasks":"max_campaign_children","hypotheses":"max_hypotheses",
                "challengers":"max_challengers","evaluations":"max_evaluations","attempts":"max_attempts",
                "wall_time":"max_wall_time_seconds"}
            if "max_tokens" in budget:
                already=sum(sum(totals.get(d,(0,0))) for d in ("tokens_input","tokens_output"))
                requested=sum(float(amounts[d] or 0) for d in ("tokens_input","tokens_output"))
                if already+requested>float(budget["max_tokens"]):
                    self.db.execute("UPDATE runtime.improvement_campaigns SET status='STOPPED',stop_reason='BUDGET_EXHAUSTED',ended_at=now(),stopped_at=now() WHERE campaign_id=%s",(campaign_id,))
                    raise ValueError("campaign hard limit exceeded for combined tokens")
            if "experiment_units" in amounts:
                requested=float(amounts["experiment_units"] or 0)
                old_rows=self.db.execute("SELECT budget_class,action,dimensions FROM runtime.improvement_budget_ledger WHERE campaign_id=%s",(campaign_id,)).fetchall()
                old_total=old_class=0.0
                for item in old_rows:
                    sign=1.0 if item["action"]=="RESERVE" else -1.0 if item["action"]=="RELEASE" else 0.0
                    value=sign*float((item["dimensions"] or {}).get("experiment_units",0))
                    old_total+=value
                    if item["budget_class"]==budget_class: old_class+=value
                new_total=sum(sum(totals.get(d,(0,0))) for d in ("experiment_units",))
                new_class=self.db.execute("""SELECT coalesce(sum(CASE WHEN r.status='RESERVED' THEN d.reserved ELSE d.consumed END),0) AS n
                    FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
                    WHERE r.campaign_id=%s AND r.budget_class=%s AND d.dimension='experiment_units' AND r.status IN ('RESERVED','SETTLED','UNKNOWN')""",
                    (campaign_id,budget_class)).fetchone()["n"]
                global_limit=budget.get("max_experiment_units")
                allocation=(campaign["exploration_allocation"] if budget_class=="EXPLORE" else campaign["exploitation_allocation"]) or {}
                class_limit=allocation.get("experiment_units")
                if global_limit is not None and old_total+new_total+requested>float(global_limit):
                    raise ValueError("campaign experiment unit budget exhausted")
                if class_limit is not None and old_class+float(new_class or 0)+requested>float(class_limit):
                    raise ValueError(f"campaign {budget_class.lower()} experiment allocation exhausted")
            for dimension, request in amounts.items():
                key = LIMIT_KEYS.get(dimension)
                limit = budget.get(key) if key else budget.get(dimension)
                policy_key=policy_keys.get(dimension)
                policy_limit=policy_limits.get(policy_key) if policy_key else None
                if limit is None: limit=policy_limit
                elif policy_limit is not None: limit=min(float(limit),float(policy_limit))
                if request is None and limit is not None:
                    raise ValueError(f"unknown {dimension} usage cannot be reserved against a hard limit")
                if request is None and not usage_bounded:
                    continue
                if limit is not None:
                    if request is None or totals.get(dimension,(0,0))[0] + totals.get(dimension,(0,0))[1] + float(request) > float(limit):
                        self.db.execute("UPDATE runtime.improvement_campaigns SET status='STOPPED',stop_reason='BUDGET_EXHAUSTED',ended_at=now(),stopped_at=now() WHERE campaign_id=%s",(campaign_id,))
                        raise ValueError(f"campaign hard limit exceeded for {dimension}")
            reservation_id="cres_"+uuid.uuid4().hex
            self.db.execute("""INSERT INTO runtime.improvement_reservations
                (reservation_id,campaign_id,idempotency_key,budget_class,stage,status,dispatch_state,usage_status,task_id,attempt_id)
                VALUES (%s,%s,%s,%s,%s,'RESERVED','NOT_DISPATCHED',%s,%s,%s)""",
                (reservation_id,campaign_id,idempotency_key,budget_class,stage,"UNKNOWN" if any(v is None for v in amounts.values()) else "KNOWN",task_id,attempt_id))
            for dimension, amount in amounts.items():
                self.db.execute("""INSERT INTO runtime.improvement_reservation_dimensions
                    (reservation_id,dimension,reserved) VALUES (%s,%s,%s)""",(reservation_id,dimension,amount))
            _evolution_event(self.db,"CAMPAIGN_RESOURCES_RESERVED",scope_id=campaign["scope_id"],actor_id="campaign-accounting",
                payload={"campaign_id":campaign_id,"reservation_id":reservation_id,"stage":stage,"dimensions":dict(amounts)})
            return reservation_id

    def mark_dispatched(self, reservation_id: str) -> None:
        with self.db.transaction():
            row=self.db.execute("SELECT campaign_id,status,dispatch_state FROM runtime.improvement_reservations WHERE reservation_id=%s FOR UPDATE",(reservation_id,)).fetchone()
            if not row or row["status"] != "RESERVED":
                raise ValueError("reservation is not dispatchable")
            if row["dispatch_state"] == "DISPATCHED":
                return
            campaign=self.db.execute("SELECT status FROM runtime.improvement_campaigns WHERE campaign_id=%s FOR UPDATE",(row["campaign_id"],)).fetchone()
            if not campaign or campaign["status"] in {"STOPPED","COMPLETED"}:
                raise ValueError("stopped campaign cannot dispatch reserved work")
            self.db.execute("UPDATE runtime.improvement_reservations SET dispatch_state='DISPATCHED' WHERE reservation_id=%s",(reservation_id,))
            _evolution_event(self.db,"CAMPAIGN_WORK_DISPATCHED",scope_id=None,actor_id="campaign-accounting",
                payload={"campaign_id":row["campaign_id"],"reservation_id":reservation_id})

    def reserve_task_attempt(self, *, goal_id: str | None, task_id: str,
                             attempt_id: str, task_budget: Mapping[str, Any]) -> str | None:
        """Reserve campaign usage before a campaign-linked attempt is leased."""
        if not goal_id:
            return None
        campaign=self.db.execute("""SELECT c.campaign_id,c.budget,c.limits,c.scope_id,
            EXISTS(SELECT 1 FROM evolution.e1_scope_policies p WHERE p.scope_id=c.scope_id) AS is_e1_campaign
            FROM runtime.improvement_campaigns c
            WHERE goal_id=%s AND status NOT IN ('STOPPED','COMPLETED') FOR UPDATE""",(goal_id,)).fetchone()
        if not campaign:
            return None
        caps={"attempts":1,"tokens_input":task_budget.get("max_tokens_input"),
              "tokens_output":task_budget.get("max_tokens_output"),
              "monetary_cost":task_budget.get("max_cost"),
              "wall_time":task_budget.get("max_wall_time_seconds",
                  (campaign["limits"] or {}).get("max_wall_time_seconds"))}
        # For governed E1 campaigns, accepted runtime attempts are part of the
        # experiment budget. Existing campaign accounting remains unchanged.
        if campaign["is_e1_campaign"]:
            caps["experiment_units"]=1
        reservation=self.reserve(campaign_id=campaign["campaign_id"],stage="task_attempt",
            idempotency_key=f"attempt:{attempt_id}",amounts=caps,task_id=task_id)
        return reservation

    def attach_and_dispatch(self, reservation_id: str, *, task_id: str,
                             attempt_id: str | None = None) -> None:
        self.db.execute("UPDATE runtime.improvement_reservations SET task_id=%s,attempt_id=%s WHERE reservation_id=%s",
                        (task_id,attempt_id,reservation_id))
        self.mark_dispatched(reservation_id)

    def reserve_child_task(self, *, goal_id: str | None, task_id: str,
                           idempotency_key: str) -> str | None:
        """Reserve a campaign child slot before its dispatch outbox is committed."""
        if not goal_id:
            return None
        campaign=self.db.execute("""SELECT campaign_id FROM runtime.improvement_campaigns
            WHERE goal_id=%s AND status NOT IN ('STOPPED','COMPLETED') FOR UPDATE""",(goal_id,)).fetchone()
        if not campaign:
            return None
        reservation=self.reserve(campaign_id=campaign["campaign_id"],stage="child_task",
            idempotency_key=f"child:{idempotency_key}",amounts={"child_tasks":1},task_id=None)
        # The task FK is attached after the durable child row has been inserted,
        # while still inside the caller's transaction.
        return reservation

    def finish_task_reservation(self, *, task_id: str, attempt_id: str | None,
                                terminal_status: str) -> None:
        """Settle all dispatched reservations for a terminal task/attempt."""
        rows=self.db.execute("""SELECT reservation_id,attempt_id FROM runtime.improvement_reservations
            WHERE task_id=%s AND status='RESERVED'
              AND (%s::text IS NULL OR attempt_id=%s OR attempt_id IS NULL)
            ORDER BY created_at FOR UPDATE""",(task_id,attempt_id,attempt_id)).fetchall()
        if not rows:
            return
        for row in rows:
            usage=self.db.execute("""SELECT sum(input_tokens) AS input_tokens,
                    sum(output_tokens) AS output_tokens,sum(estimated_cost) AS monetary_cost,
                    count(*) AS run_count,
                    bool_or(input_tokens IS NULL) AS input_unknown,
                    bool_or(output_tokens IS NULL) AS output_unknown,
                    bool_or(estimated_cost IS NULL) AS cost_unknown
                FROM runtime.model_runs WHERE task_id=%s AND (%s::text IS NULL OR attempt_id=%s)""",
                (task_id,row["attempt_id"],row["attempt_id"])).fetchone()
            elapsed=self.db.execute("""SELECT extract(epoch FROM coalesce(completed_at,now())-started_at) AS wall_time
                FROM runtime.attempts WHERE attempt_id=%s""",(row["attempt_id"],)).fetchone() if row["attempt_id"] else None
            dimensions=self.db.execute("SELECT dimension FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",
                                       (row["reservation_id"],)).fetchall()
            actual={d["dimension"]:1 for d in dimensions if d["dimension"] in {"attempts","child_tasks"}}
            for key in ("tokens_input","tokens_output","monetary_cost","wall_time"):
                if any(d["dimension"]==key for d in dimensions):
                    if key=="wall_time":
                        actual[key]=elapsed["wall_time"] if elapsed else None
                    elif key=="tokens_input":
                        actual[key]=None if not usage["run_count"] or usage["input_unknown"] else usage["input_tokens"]
                    elif key=="tokens_output":
                        actual[key]=None if not usage["run_count"] or usage["output_unknown"] else usage["output_tokens"]
                    else:
                        actual[key]=None if not usage["run_count"] or usage["cost_unknown"] else usage[key]
            self.settle(row["reservation_id"],actual=actual,terminal_status=terminal_status)

    def settle(self, reservation_id: str, *, actual: Mapping[str, int | float | None], terminal_status: str) -> str:
        if terminal_status not in {"SUCCEEDED","FAILED","TIMEOUT","CANCELLED","STALE","QUARANTINED"}:
            raise ValueError("unsupported terminal accounting status")
        with self.db.transaction():
            row=self.db.execute("SELECT * FROM runtime.improvement_reservations WHERE reservation_id=%s FOR UPDATE",(reservation_id,)).fetchone()
            if not row:
                raise ValueError("reservation not found")
            if row["status"] in {"SETTLED","RELEASED","UNKNOWN"}:
                return row["status"]
            if row["dispatch_state"] != "DISPATCHED":
                raise ValueError("cannot settle work that was never dispatched")
            dims=self.db.execute("SELECT * FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s FOR UPDATE",(reservation_id,)).fetchall()
            actual=dict(actual)
            if set(actual)-{d["dimension"] for d in dims}:
                raise ValueError("actual usage contains an unreserved dimension")
            if any(value is not None and (not math.isfinite(float(value)) or float(value) < 0)
                   for value in actual.values()):
                raise ValueError("actual resource usage must be finite and nonnegative")
            overrun=False; has_unknown=False
            for d in dims:
                key=d["dimension"]; reserved=d["reserved"]; amount=actual.get(key)
                if amount is None:
                    has_unknown=True
                    consumed=reserved
                    released=0
                else:
                    consumed=float(amount)
                    if reserved is not None and consumed > float(reserved): overrun=True
                    released=max(0.0,float(reserved)-consumed) if reserved is not None else 0.0
                self.db.execute("UPDATE runtime.improvement_reservation_dimensions SET consumed=%s,released=%s,unknown=%s WHERE reservation_id=%s AND dimension=%s",
                    (consumed,released,amount is None,reservation_id,key))
            status="UNKNOWN" if has_unknown else "SETTLED"
            self.db.execute("UPDATE runtime.improvement_reservations SET status=%s,dispatch_state='TERMINAL',usage_status=%s,settled_at=now(),metadata=metadata||%s WHERE reservation_id=%s",
                (status,"UNKNOWN" if has_unknown else "KNOWN",_jsonb({"terminal_status":terminal_status,"overrun_policy":"STOP_CAMPAIGN" if overrun else None}),reservation_id))
            if overrun:
                self.db.execute("UPDATE runtime.improvement_campaigns SET status='STOPPED',stop_reason='BUDGET_EXHAUSTED',ended_at=now(),stopped_at=now() WHERE campaign_id=%s AND status NOT IN ('STOPPED','COMPLETED')",(row["campaign_id"],))
                _evolution_event(self.db,"CAMPAIGN_STOPPED",scope_id=None,actor_id="campaign-accounting",
                    payload={"campaign_id":row["campaign_id"],"reason":"ACTUAL_USAGE_EXCEEDED_RESERVATION"})
            _evolution_event(self.db,"CAMPAIGN_RESOURCES_SETTLED",scope_id=None,actor_id="campaign-accounting",
                payload={"campaign_id":row["campaign_id"],"reservation_id":reservation_id,"status":status,
                    "terminal_status":terminal_status,"overrun":overrun})
            return status

    def reconcile(self) -> dict[str,int]:
        released=unknown=0
        rows=self.db.execute("SELECT reservation_id,status,dispatch_state,attempt_id,task_id,stage FROM runtime.improvement_reservations WHERE status='RESERVED' ORDER BY created_at").fetchall()
        for row in rows:
            # E2 evaluation reservations are reconciled by the E2 run/mission
            # ledger. They have no task_id until an immutable coordination mission is
            # materialized, so generic campaign recovery must not mark them
            # UNKNOWN during that intentional binding window.
            if row["stage"] == "e2_evaluation":
                continue
            if row["dispatch_state"] == "NOT_DISPATCHED":
                with self.db.transaction():
                    self.db.execute("UPDATE runtime.improvement_reservation_dimensions SET released=reserved,consumed=0 WHERE reservation_id=%s",(row["reservation_id"],))
                    self.db.execute("UPDATE runtime.improvement_reservations SET status='RELEASED',settled_at=now() WHERE reservation_id=%s AND status='RESERVED'",(row["reservation_id"],))
                    _evolution_event(self.db,"CAMPAIGN_RESERVATION_RECOVERED",scope_id=None,actor_id="campaign-accounting",
                        payload={"reservation_id":row["reservation_id"],"recovery":"RELEASE_UNDISPATCHED"})
                released+=1; continue
            if not row["attempt_id"] and not row["task_id"]:
                # Dispatch was persisted but no durable execution identity
                # survived; charge the full reservation as UNKNOWN.
                dims=self.db.execute("SELECT dimension,reserved FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",(row["reservation_id"],)).fetchall()
                self.settle(row["reservation_id"],actual={d["dimension"]:None for d in dims},terminal_status="STALE")
                unknown+=1; continue
            terminal=None; expired=False
            if row["attempt_id"]:
                terminal=self.db.execute("SELECT status FROM runtime.attempts WHERE attempt_id=%s",(row["attempt_id"],)).fetchone()
                lease=self.db.execute("SELECT status,lease_until FROM runtime.leases WHERE attempt_id=%s",(row["attempt_id"],)).fetchone()
                expired=bool(lease and (lease["status"]!="ACTIVE" or lease["lease_until"]<=self.db.execute("SELECT now() AS n").fetchone()["n"]))
            elif row["task_id"]:
                terminal=self.db.execute("SELECT status FROM runtime.tasks WHERE task_id=%s",(row["task_id"],)).fetchone()
            if terminal and (terminal["status"] not in {"RUNNING","LEASED","QUEUED"} or expired):
                status=terminal["status"]
                mapped=("STALE" if expired or status in {"ABANDONED","RETRY_PENDING"} else
                    "CANCELLED" if status=="CANCELLED" else "QUARANTINED" if status=="QUARANTINED" else
                    "FAILED" if status.startswith("FAILED") or status in {"REJECTED","BUDGET_EXCEEDED","NEEDS_REVIEW"} else "SUCCEEDED")
                if row["task_id"]:
                    self.finish_task_reservation(task_id=row["task_id"],attempt_id=row["attempt_id"],terminal_status=mapped)
                else:
                    dims=self.db.execute("SELECT dimension FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s",(row["reservation_id"],)).fetchall()
                    self.settle(row["reservation_id"],actual={d["dimension"]:None for d in dims},terminal_status=mapped)
                unknown+=1
        return {"released_undispatched":released,"settled_unknown":unknown}

    def snapshot(self, campaign_id: str) -> dict[str, Any]:
        campaign=self.db.execute("SELECT budget,limits,exploration_allocation,exploitation_allocation FROM runtime.improvement_campaigns WHERE campaign_id=%s",(campaign_id,)).fetchone()
        if not campaign: raise ValueError("campaign not found")
        rows=self.db.execute("""SELECT d.dimension,
            coalesce(sum(d.reserved) FILTER (WHERE r.status='RESERVED'),0) AS reserved,
            coalesce(sum(d.consumed) FILTER (WHERE r.status IN ('SETTLED','UNKNOWN')),0) AS consumed,
            coalesce(sum(d.released),0) AS released,
            coalesce(bool_or(d.unknown),false) AS unknown
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.campaign_id=%s GROUP BY d.dimension""",(campaign_id,)).fetchall()
        limits=campaign["budget"] or {}; policy=campaign["limits"] or {}; result={}
        policy_keys={"child_tasks":"max_campaign_children","hypotheses":"max_hypotheses",
            "challengers":"max_challengers","evaluations":"max_evaluations","attempts":"max_attempts",
            "wall_time":"max_wall_time_seconds"}
        for row in rows:
            dimension=row["dimension"]; limit=limits.get(LIMIT_KEYS.get(dimension,""),limits.get(dimension))
            if limit is None and dimension in policy_keys: limit=policy.get(policy_keys[dimension])
            consumed=float(row["consumed"] or 0); reserved=float(row["reserved"] or 0)
            result[dimension]={"limit":limit,"reserved":reserved,"consumed":consumed,
                "released":float(row["released"] or 0),"remaining":None if limit is None else float(limit)-consumed-reserved,
                "usage_status":"UNKNOWN" if row["unknown"] else "KNOWN"}
        if "max_tokens" in limits:
            token_keys=("tokens_input","tokens_output")
            consumed=sum(result.get(k,{}).get("consumed",0.0) for k in token_keys)
            reserved=sum(result.get(k,{}).get("reserved",0.0) for k in token_keys)
            released=sum(result.get(k,{}).get("released",0.0) for k in token_keys)
            unknown=any(result.get(k,{}).get("usage_status")=="UNKNOWN" for k in token_keys)
            cap=float(limits["max_tokens"])
            result["combined_tokens"]={"limit":cap,"reserved":reserved,"consumed":consumed,
                "released":released,"remaining":cap-reserved-consumed,
                "usage_status":"UNKNOWN" if unknown else "KNOWN"}
        return result
