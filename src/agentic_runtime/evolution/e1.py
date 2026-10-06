"""Governed, deterministic E1 challenger campaigns.

This module consumes persisted verified executor measurements. It can create
E1-only candidates and evaluate them deterministically; PostgreSQL alone owns
the atomic champion transition and automatic evidence-triggered rollback.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from agentic_runtime.persistence.events import jsonb as _jsonb
from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.contracts.serialization import canonical_bytes as _canonical_bytes
from agentic_runtime.contracts.serialization import canonical_hash


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def canonical_bytes(value: Any) -> bytes:
    return _canonical_bytes(value)


def sha256(value: Any) -> str:
    return canonical_hash(value)


def postgres_json_hash(db: Any, value: Any) -> str:
    """Hash PostgreSQL's canonical JSONB text used by the durable genome row."""
    return db.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS digest",
                      (_jsonb(value),)).fetchone()["digest"]


class E1PolicyError(ValueError):
    pass


def monetary_comparison_eligible(*, mode: str, value: float | None,
                                 provenance: str | None) -> bool:
    """Monetary comparison accepts only policy-authoritative billing evidence."""
    if mode == "CAMPAIGN_SAFETY_ONLY":
        return True
    if mode == "LEGACY_RECORDED":
        return value is not None
    if mode != "AUTHORITATIVE_ONLY":
        raise E1PolicyError("unknown monetary comparison policy")
    return value is not None and provenance == "AUTHORITATIVE_BILLING"


def compare_e1_metrics(*, baseline: Mapping[str, Any], candidate: Mapping[str, Any],
                       policy: Mapping[str, Any]) -> dict[str, bool]:
    """Evaluate immutable quality/latency policy without inventing cost data."""
    metric=str(policy.get("improvement_metric", ""))
    quality_key=str(policy.get("protected_quality_metric", ""))
    direction=policy.get("improvement_direction")
    required=float(policy.get("required_improvement", -1))
    if (metric not in baseline or metric not in candidate or quality_key not in baseline
            or quality_key not in candidate or direction not in {"MIN", "MAX"} or required < 0):
        raise E1PolicyError("comparison policy or required metrics are incomplete")
    base_metric=float(baseline[metric]); candidate_metric=float(candidate[metric])
    if direction == "MIN": improved=candidate_metric <= base_metric*(1-required)
    else: improved=candidate_metric >= base_metric*(1+required)
    quality_floor=float(policy.get("quality_floor", baseline[quality_key]))
    quality=(float(candidate[quality_key]) >= quality_floor
             and float(candidate[quality_key]) >= float(baseline[quality_key]))
    monetary_mode=policy.get("monetary_evidence_mode", "LEGACY_RECORDED")
    money=monetary_comparison_eligible(mode=str(monetary_mode),
        value=candidate.get("cost_units"),provenance=candidate.get("cost_provenance"))
    maximum=policy.get("maximum_cost_units")
    if monetary_mode != "CAMPAIGN_SAFETY_ONLY" and maximum is not None:
        money=money and float(candidate["cost_units"]) <= float(maximum)
    return {"improved":improved,"quality_pass":quality,"monetary_pass":money,
            "eligible":improved and quality and money}


def validate_frozen_candidate_outcomes(*, expected_routes: Sequence[str],
        valid_routes: Sequence[str], failed_routes: Sequence[str], minimum_valid: int) -> None:
    """Validate a frozen portfolio without retries or hidden candidate loss."""
    expected=set(expected_routes); valid=set(valid_routes); failed=set(failed_routes)
    if minimum_valid < 1 or valid & failed or valid | failed != expected:
        raise E1PolicyError("candidate outcomes do not reconcile to the frozen portfolio")
    if len(valid) < minimum_valid:
        raise E1PolicyError("candidate-local failures leave fewer than the frozen minimum valid challengers")


class E1EvolutionService:
    """Bounded campaign service; no model output is authoritative."""

    EVALUATOR = "deterministic-e1-evaluator-v1"
    PROPOSER = "deterministic-e1-mutation-engine-v1"
    PROMOTER = "deterministic-e1-promotion-controller-v1"

    def __init__(self, db: Any, *, governance_db: Any | None = None,
                 evaluator_db: Any | None = None,
                 promotion_db: Any | None = None) -> None:
        self.db = db
        self.governance_db = governance_db
        self.evaluator_db = evaluator_db
        # Active champion movement is invoked through a separately connected
        # login that inherits only the E1 promotion capability. It is never
        # performed with the ordinary runtime/evaluator connection.
        self.promotion_db = promotion_db
        self._reservation_started: dict[str, float] = {}

    def register_scope(self, *, scope_id: str, description: str, champion_id: str,
                       champion_config: Mapping[str, Any], suite_id: str,
                       suite_version: str, suite_definition: Mapping[str, Any],
                       mode: str = "SHADOW", promotion_policy: Mapping[str, Any] | None = None,
                       allowed_paths: Sequence[str] = ("routing.preference.*",),
                       code_revision: str = "working-tree") -> str:
        """Register a deliberately small E1 scope and immutable baseline."""
        if mode not in {"SHADOW", "ACTIVE_E1"}:
            raise E1PolicyError("new campaign mode must be SHADOW or explicitly governed ACTIVE_E1")
        if not allowed_paths or any(not p.startswith(("routing.", "context.", "retry.", "exploration_allocation.")) for p in allowed_paths):
            raise E1PolicyError("E1 mutation path is outside the registered soft-genome allowlist")
        cfg = dict(champion_config)
        authority_db=self.governance_db or self.db
        digest = postgres_json_hash(authority_db,cfg)
        policy = dict(promotion_policy or {
            "improvement_metric": "latency_ms", "improvement_direction": "MIN",
            "minimum_improvement_fraction": 0.05, "required_improvement": 0.05, "protected_quality_metric": "quality",
            "maximum_cost_units": 1e12,
        })
        if policy.get("improvement_direction") not in {"MIN", "MAX"}:
            raise E1PolicyError("promotion policy direction must be explicit")
        policy_hash = sha256({"scope": scope_id, "mode": mode, "allowed_paths": list(allowed_paths),
                              "promotion_policy": policy, "version": "1"})
        definition = dict(suite_definition)
        suite_hash = sha256(definition)
        config_ref = f"sha256:{digest}"
        with authority_db.transaction():
            authority_db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,%s)", (scope_id, description))
            authority_db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
                 mutation_rationale,created_by,metadata)
                VALUES (%s,%s,'1','CHAMPION',%s,%s,'initial E1 champion','governed E1 baseline',%s,%s)""",
                (champion_id, scope_id, config_ref, digest, "e1-scope-governance", _jsonb({"tier":"E1"})))
            authority_db.execute("INSERT INTO evolution.scope_champions(scope_id,genome_id) VALUES (%s,%s)", (scope_id, champion_id))
            authority_db.execute("""INSERT INTO evolution.e1_scope_policies
                (scope_id,policy_version,allowed_paths,bounds,mode,promotion_policy,policy_hash,created_by)
                VALUES (%s,'1',%s,%s,%s,%s,%s,'human-governed-e1-scope-registration')""",
                (scope_id,_jsonb(list(allowed_paths)),_jsonb({}),mode,_jsonb(policy),policy_hash))
            authority_db.execute("""INSERT INTO evolution.e1_genome_versions
                (genome_id,scope_id,tier,parent_genome_id,canonical_config,config_hash,changed_paths,provenance)
                VALUES (%s,%s,'E1',NULL,%s,%s,'[]'::jsonb,%s)""",
                (champion_id,scope_id,_jsonb(cfg),digest,_jsonb({"created_by":"human-governed-e1-scope-registration","tier":"E1"})))
            existing = authority_db.execute("SELECT status,integrity_hash FROM evolution.eval_suite_versions WHERE eval_suite_id=%s AND version=%s",(suite_id,suite_version)).fetchone()
            if existing:
                if existing["status"] != "AUTHORITATIVE" or existing["integrity_hash"] != suite_hash:
                    raise E1PolicyError("existing evaluation suite differs; immutable version cannot be replaced")
            else:
                authority_db.execute("""INSERT INTO evolution.eval_suite_versions
                    (eval_suite_id,version,scope_id,status,definition_ref,integrity_hash,created_by,metadata)
                    VALUES (%s,%s,%s,'AUTHORITATIVE',%s,%s,'independent-evaluation-governance',%s)""",
                    (suite_id,suite_version,scope_id,f"e1-suite://{suite_hash}",suite_hash,_jsonb({"metric_directions":{"latency_ms":"MIN","quality":"MAX"},"code_revision":code_revision})))
            self._event(scope_id,None,"E1_SCOPE_REGISTERED",self.PROMOTER,
                        {"champion":champion_id,"mode":mode,"policy_hash":policy_hash,"config_hash":digest},db=authority_db)
        authority_db.commit()
        return champion_id

    def create_campaign(self, *, campaign_id: str, goal_id: str, scope_id: str,
                        budget: Mapping[str, Any] | None = None,
                        mode: str | None = None, created_by: str = "e1-campaign-controller") -> None:
        policy = self.db.execute("SELECT mode FROM evolution.e1_scope_policies WHERE scope_id=%s", (scope_id,)).fetchone()
        if not policy:
            raise E1PolicyError("unknown E1 scope")
        if mode is not None and mode != policy["mode"]:
            raise E1PolicyError("campaign cannot expand or override its governed scope mode")
        budget_value = dict(budget or {"max_experiment_units": 40})
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.campaigns
                (campaign_id,idempotency_key,request_hash,description,status,budget,max_children,created_by)
                VALUES (%s,%s,%s,'bounded E1 improvement campaign','ACTIVE',%s,0,%s)""",
                (campaign_id,f"m7:{campaign_id}",sha256({"scope":scope_id,"budget":budget_value}),_jsonb(budget_value),created_by))
            self.db.execute("""INSERT INTO runtime.goals(goal_id,campaign_id,status,description,priority,
                mission_ref,created_by,metadata) VALUES (%s,%s,'ACTIVE','Improve a generic execution capability',1,
                'mission-charter-v1',%s,%s)""",
                (goal_id,campaign_id,created_by,_jsonb({"scope_id":scope_id,"tier":"E1"})))
            self.db.execute("""INSERT INTO runtime.improvement_campaigns
                (campaign_id,scope_id,goal_id,status,budget,exploration_allocation,
                 exploitation_allocation,limits,created_by)
                VALUES (%s,%s,%s,'CREATED',%s,%s,%s,%s,%s)""",
                (campaign_id,scope_id,goal_id,_jsonb(budget_value),
                _jsonb({"experiment_units":8}),_jsonb({"experiment_units":32}),
                 _jsonb({"max_opportunities":5,"max_selected_opportunities":2,
                         "max_hypotheses":3,"max_challengers":4,"max_evaluations":5,
                         "max_campaign_children":0,"max_attempts":12,"max_wall_time_seconds":3600}),created_by))

    def record_measurement(self, *, measurement_id: str, scope_id: str, campaign_id: str,
                           executor_id: str, capability: str, task_id: str,
                           attempt_id: str, model_run_id: str, artifact_ref: str,
                           metadata: Mapping[str, Any] | None = None) -> None:
        """Snapshot metrics from an already accepted and hash-verified runtime result.

        Metric values are deliberately absent from this API. A PostgreSQL
        trigger derives them from the accepted task/attempt, verified artifact
        and matching executor run, so a scout cannot invent benchmark evidence.
        """
        with self.db.transaction():
            self.db.execute("""INSERT INTO evolution.e1_executor_measurements
                (measurement_id,scope_id,campaign_id,task_class,executor_id,capability,verified,
                 quality,latency_ms,cost_units,task_id,attempt_id,model_run_id,artifact_ref,metadata)
                VALUES (%s,%s,%s,'pending',%s,%s,true,1,0,0,%s,%s,%s,%s,%s)""",
                (measurement_id,scope_id,campaign_id,executor_id,capability,task_id,attempt_id,
                 model_run_id,artifact_ref,_jsonb(dict(metadata or {}))))

    def _event(self, scope_id: str, campaign_id: str | None, kind: str, actor: str,
               payload: Mapping[str, Any], db: Any | None = None) -> None:
        target=db or self.db
        event_id = _id("e1evt")
        payload_hash=target.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS digest",
                                    (_jsonb(dict(payload)),)).fetchone()["digest"]
        target.execute("""INSERT INTO evolution.e1_events
            (event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (event_id,scope_id,campaign_id,kind,actor,_jsonb(dict(payload)),payload_hash))

    def _reserve(self, campaign_id: str, stage: str, amounts: Mapping[str, int]) -> str:
        acc = CampaignAccounting(self.db)
        reserved_amounts=dict(amounts)
        campaign=self.db.execute("SELECT budget,limits FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                                 (campaign_id,)).fetchone()
        budget=campaign["budget"] or {}
        limits=campaign["limits"] or {}
        wall_limits=[int(value) for value in (budget.get("max_wall_time"),
            limits.get("max_wall_time_seconds")) if value is not None]
        if wall_limits:
            reserved_amounts["wall_time"]=min(300,*wall_limits)
        reservation = acc.reserve(campaign_id=campaign_id,stage=stage,
            idempotency_key=f"m7:{campaign_id}:{stage}",amounts=reserved_amounts,budget_class="EXPLOIT")
        acc.mark_dispatched(reservation)
        self._reservation_started[reservation]=time.monotonic()
        return reservation

    def _settle(self, reservation: str, amounts: Mapping[str, int]) -> None:
        actual=dict(amounts)
        if reservation in self._reservation_started:
            elapsed=max(0.001,time.monotonic()-self._reservation_started.pop(reservation))
            dimension=self.db.execute("SELECT 1 FROM runtime.improvement_reservation_dimensions WHERE reservation_id=%s AND dimension='wall_time'",
                                      (reservation,)).fetchone()
            if dimension:
                actual["wall_time"]=elapsed
        CampaignAccounting(self.db).settle(reservation,actual=actual,terminal_status="SUCCEEDED")

    def _persist_cognitive_proposal_lineage(self, *, campaign_id: str, scope_id: str,
            mutation_id: str, genome_id: str, model_proposal: Mapping[str, Any]) -> None:
        """Durably join the validated proposal payload to its immutable worker artifacts."""
        proposal=model_proposal.get("proposal")
        proposal_hash=model_proposal.get("proposal_hash")
        if not isinstance(proposal,Mapping) or sha256(proposal)!=proposal_hash:
            raise E1PolicyError("proposal lineage payload hash is invalid")
        if model_proposal.get("proposal_artifact_ref")!="sha256:"+str(proposal_hash):
            raise E1PolicyError("proposal lineage artifact reference does not match the canonical proposal")
        self.db.execute("""INSERT INTO evolution.e1_cognitive_proposal_lineage
            (lineage_id,campaign_id,scope_id,mutation_id,genome_id,invocation_id,
             raw_cognitive_artifact_id,normalized_cognitive_artifact_id,
             raw_artifact_ref,normalized_artifact_ref,proposal_artifact_ref,
             raw_hash,normalized_hash,proposal_hash,proposal)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (_id("e1coglineage"),campaign_id,scope_id,mutation_id,genome_id,
             model_proposal["invocation_id"],model_proposal["raw_cognitive_artifact_id"],
             model_proposal["normalized_cognitive_artifact_id"],model_proposal["raw_artifact_ref"],
             model_proposal["normalized_artifact_ref"],model_proposal["proposal_artifact_ref"],
             model_proposal["raw_hash"],model_proposal["normalized_hash"],proposal_hash,_jsonb(dict(proposal))))

    def reconcile_incomplete_campaigns(self) -> dict[str, int]:
        """Run campaign recovery as one durable unit when called from idle state.

        The accounting reconciliation begins with reads before it enters its
        own per-reservation transactions. Without this outer boundary those
        reads can leave the connection in an implicit transaction, turning
        later savepoints into uncommitted work that is lost when a recovery
        process exits.
        """
        with self.db.transaction():
            return self._reconcile_incomplete_campaigns()

    def _reconcile_incomplete_campaigns(self) -> dict[str, int]:
        """Close interrupted E1 campaigns without inferring success.

        A durable promotion/shadow decision proves the terminal evolution
        decision committed. Any other nonterminal campaign is stopped and
        retained for audit; runtime accounting reconciliation classifies
        reservations first so a restart cannot leak reserved budget.
        """
        accounting = CampaignAccounting(self.db).reconcile()
        rows = self.db.execute("""SELECT c.campaign_id,c.scope_id,c.status,
            EXISTS(SELECT 1 FROM evolution.e1_promotion_decisions d
                   WHERE d.campaign_id=c.campaign_id) AS has_decision
            FROM runtime.improvement_campaigns c
            JOIN evolution.e1_scope_policies p USING(scope_id)
            WHERE c.status IN ('CREATED','OBSERVING','OPPORTUNITIES_IDENTIFIED',
                'SELECTED','HYPOTHESES_CREATED','CHALLENGERS_CREATED','EVALUATING')
            ORDER BY c.created_at FOR UPDATE OF c""").fetchall()
        completed = stopped = 0
        for row in rows:
            target = "COMPLETED" if row["has_decision"] else "STOPPED"
            reason = "RECOVERED_DURABLE_DECISION" if row["has_decision"] else "PROCESS_RESTART_INCOMPLETE_CAMPAIGN"
            with self.db.transaction():
                changed = self.db.execute("""UPDATE runtime.improvement_campaigns
                    SET status=%s,ended_at=now(),stop_reason=%s,
                        stopped_at=CASE WHEN %s='STOPPED' THEN now() ELSE stopped_at END
                    WHERE campaign_id=%s AND status=%s RETURNING campaign_id""",
                    (target,reason,target,row["campaign_id"],row["status"])).fetchone()
                if changed:
                    # A stopped/completed improvement campaign must also stop
                    # ordinary task dispatch. Coordinator.claim filters on the
                    # authoritative runtime campaign state.
                    runtime_status = "COMPLETED" if target == "COMPLETED" else "CANCELLED"
                    self.db.execute("""UPDATE runtime.campaigns SET status=%s
                        WHERE campaign_id=%s AND status IN ('ACTIVE','PAUSED')""",
                        (runtime_status,row["campaign_id"]))
                    self._event(row["scope_id"],row["campaign_id"],"E1_CAMPAIGN_RECONCILED",
                        "e1-recovery-controller",{"from":row["status"],"to":target,"reason":reason})
            if changed:
                if target == "COMPLETED": completed += 1
                else: stopped += 1
        # Older deployment attempts may already have a terminal evolution
        # row while the linked execution campaign remained ACTIVE. Repair that
        # split state before workers can claim more tasks from it.
        stale_dispatch = self.db.execute("""SELECT i.campaign_id,i.scope_id,i.status
            FROM runtime.improvement_campaigns i JOIN runtime.campaigns c USING(campaign_id)
            WHERE i.status IN ('COMPLETED','STOPPED') AND c.status IN ('ACTIVE','PAUSED')
            ORDER BY i.campaign_id""").fetchall()
        repaired = 0
        for row in stale_dispatch:
            target = "COMPLETED" if row["status"] == "COMPLETED" else "CANCELLED"
            with self.db.transaction():
                changed = self.db.execute("""UPDATE runtime.campaigns SET status=%s
                    WHERE campaign_id=%s AND status IN ('ACTIVE','PAUSED') RETURNING campaign_id""",
                    (target,row["campaign_id"])).fetchone()
                if changed:
                    self._event(row["scope_id"],row["campaign_id"],"E1_CAMPAIGN_DISPATCH_STATE_REPAIRED",
                        "e1-recovery-controller",{"evolution_status":row["status"],"runtime_status":target})
                    repaired += 1
        # A terminal campaign cannot dispatch any more work. Retire queued or
        # retry-pending tasks through Coordinator so state and TASK_CANCELLED
        # events commit together; preserve attempts already in progress for
        # lease/fencing reconciliation instead of guessing their outcome.
        terminal_campaigns = self.db.execute("""SELECT i.campaign_id,i.scope_id,i.status
            FROM runtime.improvement_campaigns i JOIN runtime.campaigns c USING(campaign_id)
            WHERE i.status IN ('COMPLETED','STOPPED') AND c.status IN ('COMPLETED','CANCELLED')
              AND EXISTS (SELECT 1 FROM runtime.tasks t WHERE t.campaign_id=i.campaign_id
                          AND t.status IN ('DRAFT','QUEUED','RETRY_PENDING'))
            ORDER BY i.campaign_id""").fetchall()
        from agentic_runtime.coordinator.service import Coordinator
        coordinator = Coordinator(self.db)
        dispatchable_tasks_cancelled = 0
        for row in terminal_campaigns:
            with self.db.transaction():
                count = coordinator.cancel_dispatchable_campaign_tasks(row["campaign_id"],
                    actor_id="e1-recovery-controller", reason="E1_CAMPAIGN_TERMINAL")
                if count:
                    self._event(row["scope_id"],row["campaign_id"],"E1_TERMINAL_TASKS_CANCELLED",
                        "e1-recovery-controller",{"cancelled_task_count":count})
                    dispatchable_tasks_cancelled += count
        accounting_after = CampaignAccounting(self.db).reconcile()
        return {"released_reservations":accounting["released_undispatched"],
                "unknown_reservations":accounting["settled_unknown"],
                "campaigns_completed":completed,"campaigns_stopped":stopped,
                "campaign_dispatch_states_repaired":repaired,
                "dispatchable_tasks_cancelled":dispatchable_tasks_cancelled,
                "post_cancel_released_reservations":accounting_after["released_undispatched"],
                "post_cancel_unknown_reservations":accounting_after["settled_unknown"]}

    def _measurement_summary(self, scope_id: str, task_class: str,
                             campaign_id: str | None = None) -> list[dict[str, Any]]:
        rows = self.db.execute("""SELECT m.executor_id,m.capability,count(m.measurement_id) AS sample_count,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY m.latency_ms) AS latency_ms,
            min(m.quality) AS quality,
            avg(CASE
                -- Remote/provider cost estimates are useful for reservation and
                -- planning, but they are not measured cost evidence. In
                -- particular, worker-reported token claims must never satisfy
                -- a hard E1 promotion cost gate. The only currently admissible
                -- zero-cost observation is deterministic execution with no
                -- provider/model and no cost claim. A future verified billing
                -- source needs an explicit trusted contract before admission.
                WHEN mr.adapter_type='DETERMINISTIC'
                 AND mr.provider IS NULL
                 AND mr.requested_model IS NULL
                 AND mr.resolved_model IS NULL
                 AND mr.estimated_cost=0
                THEN 0
                ELSE NULL
            END) AS cost_units,
            array_agg(m.measurement_id ORDER BY m.measured_at,m.measurement_id) AS refs
            FROM evolution.e1_executor_measurements m
            JOIN runtime.model_runs mr ON mr.model_run_id=m.model_run_id
            WHERE m.scope_id=%s AND m.task_class=%s AND m.verified
              AND (%s::text IS NULL OR m.campaign_id=%s::text)
            GROUP BY m.executor_id,m.capability HAVING count(*)>=2 ORDER BY m.executor_id""",
            (scope_id,task_class,campaign_id,campaign_id)).fetchall()
        normalized=[]
        for row in rows:
            item=dict(row)
            item["sample_count"]=int(item["sample_count"])
            for key in ("latency_ms","quality","cost_units"):
                item[key]=float(item[key]) if item[key] is not None else None
            item["refs"]=list(item["refs"] or [])
            normalized.append(item)
        return normalized

    def run(self, *, campaign_id: str, scope_id: str, task_class: str,
            code_revision: str = "working-tree", challenger_limit: int = 4,
            model_provenance_by_executor: Mapping[str, Mapping[str, Any]] | None = None,
            frozen_pack_ref: Mapping[str, str] | None = None,
            minimum_valid_challengers: int = 3,
            candidate_failures: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
        try:
            return self._run_transaction(campaign_id=campaign_id,scope_id=scope_id,task_class=task_class,
                                         code_revision=code_revision,challenger_limit=challenger_limit,
                                         model_provenance_by_executor=model_provenance_by_executor,
                                         frozen_pack_ref=frozen_pack_ref,
                                         minimum_valid_challengers=minimum_valid_challengers,
                                         candidate_failures=candidate_failures)
        except Exception:
            self.db.rollback()
            raise

    def _run_transaction(self, *, campaign_id: str, scope_id: str, task_class: str,
            code_revision: str = "working-tree", challenger_limit: int = 4,
            model_provenance_by_executor: Mapping[str, Mapping[str, Any]] | None = None,
            frozen_pack_ref: Mapping[str, str] | None = None,
            minimum_valid_challengers: int = 3,
            candidate_failures: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
        if challenger_limit < 3 or challenger_limit > 4:
            raise E1PolicyError("campaign challenger limit must be within the bounded 3..4 candidate set")
        if minimum_valid_challengers < 1 or minimum_valid_challengers > challenger_limit:
            raise E1PolicyError("minimum valid challenger count is outside the frozen campaign bound")
        if self.promotion_db is None or self.evaluator_db is None:
            raise E1PolicyError("E1 evaluation and promotion require separately authenticated capability connections")
        campaign = self.db.execute("SELECT status,scope_id,goal_id,budget FROM runtime.improvement_campaigns WHERE campaign_id=%s",(campaign_id,)).fetchone()
        if not campaign or campaign["scope_id"] != scope_id or campaign["status"] in {"STOPPED","COMPLETED"}:
            raise E1PolicyError("campaign missing, stopped, or outside requested scope")
        self.db.execute("UPDATE runtime.improvement_campaigns SET status='OBSERVING' WHERE campaign_id=%s",(campaign_id,))
        reserve = self._reserve(campaign_id,"observe",{"experiment_units":1})
        summary = self._measurement_summary(scope_id,task_class,campaign_id)
        if len(summary) < 1 + minimum_valid_challengers:
            self._settle(reserve,{"experiment_units":0})
            raise E1PolicyError("autonomous E1 discovery lacks the frozen minimum repeated challenger measurements")
        champion_row = self.db.execute("""SELECT c.genome_id,v.canonical_config,v.config_hash
            FROM evolution.scope_champions c JOIN evolution.e1_genome_versions v ON v.genome_id=c.genome_id
            JOIN evolution.system_genomes g ON g.genome_id=c.genome_id WHERE c.scope_id=%s""",(scope_id,)).fetchone()
        if not champion_row:
            self._settle(reserve,{"experiment_units":0})
            raise E1PolicyError("E1 champion is unavailable")
        champion_config = champion_row["canonical_config"]
        pref = champion_config.get("routing",{}).get("preference",{}).get("default")
        baselines = [row for row in summary if row["executor_id"] == pref]
        if not baselines:
            self._settle(reserve,{"experiment_units":0})
            raise E1PolicyError("current champion route lacks verified measurement coverage")
        baseline = baselines[0]
        viable_alternatives = [r for r in summary if r["executor_id"] != pref and r["quality"] >= baseline["quality"]]
        if not viable_alternatives:
            self._settle(reserve,{"experiment_units":1})
            self._event(scope_id,campaign_id,"CAMPAIGN_STOPPED",self.PROPOSER,{"reason":"NO_ELIGIBLE_OPPORTUNITIES"})
            raise E1PolicyError("no quality-preserving alternative was discovered")
        # The observation is derived from measured, verified runtime outcomes.
        observation_id = _id("obs")
        measurements = [x for row in summary for x in row["refs"]]
        # Cost can be UNKNOWN or SUBSCRIPTION_UNPRICED. It is not a tie-breaker
        # for this latency/quality campaign; deterministic route ID breaks ties.
        fastest = min(viable_alternatives,key=lambda r:(r["latency_ms"],r["executor_id"]))
        summary_payload = {"task_class":task_class,"baseline_executor":pref,
            "baseline_latency_ms":float(baseline["latency_ms"]),"baseline_quality":float(baseline["quality"]),
            "best_observed_executor":fastest["executor_id"],"best_observed_latency_ms":float(fastest["latency_ms"]),
            "quality_preserved":True,"measurement_refs":measurements,"sample_counts":{r["executor_id"]:r["sample_count"] for r in summary}}
        fingerprint = sha256({"scope":scope_id,"task_class":task_class,"kind":"LATENCY_QUALITY_GAP"})
        opportunity_id = _id("opp")
        hypothesis_id = _id("hyp")
        mutation_ids: list[str] = []
        challenger_ids: list[str] = []
        hyp_res = self._reserve(campaign_id,"hypothesis",{"experiment_units":1,"hypotheses":1})
        with self.db.transaction():
            self.db.execute("""INSERT INTO evolution.observations(observation_id,scope_id,source_refs,summary,created_by,metadata)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (observation_id,scope_id,_jsonb(measurements),"Verified executors have differing latency at preserved quality",self.PROPOSER,_jsonb(summary_payload)))
            previous = self.db.execute("SELECT opportunity_id FROM runtime.opportunities WHERE scope_id=%s AND fingerprint=%s AND status IN ('OPEN','INVESTIGATING','ACTIONABLE','DETECTED','TRIAGED','SELECTED','UNDER_INVESTIGATION') ORDER BY created_at LIMIT 1 FOR UPDATE",(scope_id,fingerprint)).fetchone()
            if previous:
                opportunity_id = previous["opportunity_id"]
                self.db.execute("UPDATE runtime.opportunities SET observation_refs=observation_refs||%s::jsonb WHERE opportunity_id=%s",(_jsonb([observation_id]),opportunity_id))
            else:
                self.db.execute("""INSERT INTO runtime.opportunities
                    (opportunity_id,goal_id,kind,observation_refs,description,status,estimated_value,
                     scope_id,fingerprint,estimated_cost,uncertainty,novelty,risk,triage_rationale,metadata)
                    VALUES (%s,%s,'OPPORTUNITY',%s,%s,'SELECTED',%s,%s,%s,%s,%s,'KNOWN','LOW',%s,%s)""",
                    (opportunity_id,campaign["goal_id"],_jsonb([observation_id]),
                     f"Measured latency differs across eligible routes for {task_class}",_jsonb({"latency_improvement_possible":True}),
                     scope_id,fingerprint,_jsonb({"class":"LOW"}),"LOW",
                     "Observed verified route results; quality-preserving alternatives exist",_jsonb(summary_payload)))
            self._event(scope_id,campaign_id,"OPPORTUNITY_DETECTED",self.PROPOSER,{"opportunity_id":opportunity_id,"observation_id":observation_id,"fingerprint":fingerprint})
            self.db.execute("""INSERT INTO runtime.opportunity_decisions
                (decision_id,opportunity_id,selected,priority_class,rationale,campaign_id,budget_class)
                VALUES (%s,%s,true,'P1',%s,%s,'EXPLOIT')""",
                (_id("opdec"),opportunity_id,_jsonb({"evidence_strength":"repeated verified measurements","reversibility":"HIGH","cost":"LOW"}),campaign_id))
            self.db.execute("""INSERT INTO runtime.hypotheses(hypothesis_id,opportunity_id,statement,falsification_ref,status,created_by,metadata)
                VALUES (%s,%s,%s,%s,'PROPOSED',%s,%s)""",
                (hypothesis_id,opportunity_id,
                 f"For task class {task_class}, routing to a measured quality-preserving alternative can reduce median latency by the governed threshold.",
                 f"m7:falsification:{sha256({'hypothesis':hypothesis_id,'metric':'latency_ms','quality':'no_regression'})}",
                 self.PROPOSER,_jsonb({"falsification_criteria":"candidate latency improvement below policy threshold or quality regression",
                    "supporting_refs":measurements,"contradicting_refs":[r["refs"][0] for r in summary if r["quality"] < baseline["quality"]],
                    "search_scope":"all verified executor measurements for this scope/task class","contradiction_search":"complete"})))
            for ref in measurements:
                self.db.execute("INSERT INTO runtime.hypothesis_evidence(hypothesis_id,evidence_ref,relation,search_scope) VALUES (%s,%s,'SUPPORTS',%s) ON CONFLICT DO NOTHING",
                    (hypothesis_id,ref,f"all verified measurements for {scope_id}/{task_class}"))
            for row in summary:
                if row["quality"] < baseline["quality"]:
                    for ref in row["refs"]:
                        self.db.execute("INSERT INTO runtime.hypothesis_evidence(hypothesis_id,evidence_ref,relation,search_scope) VALUES (%s,%s,'CONTRADICTS',%s) ON CONFLICT DO NOTHING",
                            (hypothesis_id,ref,f"all verified measurements for {scope_id}/{task_class}"))
            self.db.execute("UPDATE runtime.improvement_campaigns SET status='HYPOTHESES_CREATED' WHERE campaign_id=%s",(campaign_id,))
            self._event(scope_id,campaign_id,"HYPOTHESIS_CREATED",self.PROPOSER,{"hypothesis_id":hypothesis_id,"opportunity_id":opportunity_id})
        self._settle(reserve,{"experiment_units":1})
        self._settle(hyp_res,{"experiment_units":1,"hypotheses":1})
        policy = dict(self.db.execute("SELECT * FROM evolution.e1_scope_policies WHERE scope_id=%s",(scope_id,)).fetchone())
        campaign_policy = self.db.execute("""SELECT policy_version,comparison_policy,policy_hash
            FROM evolution.e1_campaign_comparison_policies WHERE campaign_id=%s AND scope_id=%s""",
            (campaign_id,scope_id)).fetchone()
        if campaign_policy:
            policy["promotion_policy"] = dict(campaign_policy["comparison_policy"])
            policy["policy_version"] = campaign_policy["policy_version"]
            policy["policy_hash"] = campaign_policy["policy_hash"]
        champion_id = champion_row["genome_id"]

        # Freeze one content-addressed definition before any candidate is evaluated.
        if model_provenance_by_executor is None:
            candidates_src = [r for r in summary if r["executor_id"] != pref][:challenger_limit]
        else:
            # Real model proposals can narrow which measured alternatives are
            # materialized. They cannot invent an executor or bypass verified
            # runtime measurements. Each entry must retain immutable invocation
            # and artifact hashes for later audit.
            if len(model_provenance_by_executor) < minimum_valid_challengers or len(model_provenance_by_executor) > challenger_limit:
                raise E1PolicyError("model-originated E1 candidates do not meet the frozen minimum")
            measured = {r["executor_id"] for r in summary if r["executor_id"] != pref}
            if not set(model_provenance_by_executor).issubset(measured):
                raise E1PolicyError("model proposal referenced an executor without verified measurements")
            validated_provenance: dict[str, dict[str, Any]] = {}
            for executor_id, provenance in model_provenance_by_executor.items():
                provenance=dict(provenance)
                required = {"invocation_id", "raw_hash", "normalized_hash", "raw_artifact_ref",
                    "normalized_artifact_ref", "proposal_artifact_ref", "proposal_hash", "proposal",
                    "source_observation_id", "evaluation_pack_hash", "rollback_target"}
                if not required.issubset(provenance) or any(
                    not isinstance(provenance[key], str) or not provenance[key]
                    for key in required - {"proposal"}
                ):
                    raise E1PolicyError("model proposal provenance must bind invocation and output hashes")
                if frozen_pack_ref is None:
                    raise E1PolicyError("model-originated challengers require a pre-frozen evaluation pack")
                proposal=provenance["proposal"]
                if not isinstance(proposal, Mapping) or sha256(proposal)!=provenance["proposal_hash"]:
                    raise E1PolicyError("normalized proposal payload does not match its canonical artifact hash")
                source_ref_id=provenance.get("source_candidate_ref_id")
                if (provenance["proposal_artifact_ref"]!="sha256:"+provenance["proposal_hash"]
                        or proposal.get("proposal_id") is None or proposal.get("scope_id")!=scope_id
                        or proposal.get("tier")!="E1" or proposal.get("parent_genome_hash")!=champion_row["config_hash"]
                        or (not source_ref_id and proposal.get("evaluation_pack_hash")!=frozen_pack_ref.get("definition_hash"))
                        or proposal.get("rollback_target")!=champion_id):
                    raise E1PolicyError("model proposal is not bound to the E1 scope, parent, eligible pack and rollback target")
                proposal_mutation=proposal.get("mutation")
                if (not isinstance(proposal_mutation,Mapping)
                        or proposal_mutation.get("path")!="routing.preference.default"
                        or proposal_mutation.get("value")!=executor_id):
                    raise E1PolicyError("model proposal mutation does not match its measured challenger route")
                if provenance["source_observation_id"] not in proposal.get("source_refs",[]):
                    raise E1PolicyError("model proposal omits its durable observation reference")
                if source_ref_id:
                    source_ref=self.db.execute("""SELECT r.*,l.invocation_id AS lineage_invocation_id,
                        l.raw_cognitive_artifact_id,l.normalized_cognitive_artifact_id,
                        l.proposal AS source_proposal,l.proposal_hash AS lineage_proposal_hash
                        FROM evolution.e1_campaign_candidate_refs r
                        JOIN evolution.e1_cognitive_proposal_lineage l ON l.lineage_id=r.source_lineage_id
                        WHERE r.candidate_ref_id=%s AND r.campaign_id=%s AND r.scope_id=%s
                          AND r.route_id=%s AND r.evaluation_pack_id=%s AND r.evaluation_pack_hash=%s
                          AND r.policy_hash=%s""",
                        (source_ref_id,campaign_id,scope_id,executor_id,frozen_pack_ref.get("pack_id"),
                         frozen_pack_ref.get("definition_hash"),policy.get("policy_hash"))).fetchone()
                    if (not source_ref or source_ref["source_genome_id"]!=provenance.get("registered_genome_id")
                            or source_ref["lineage_invocation_id"]!=provenance["invocation_id"]
                            or source_ref["proposal_hash"]!=provenance["proposal_hash"]
                            or source_ref["lineage_proposal_hash"]!=provenance["proposal_hash"]
                            or dict(source_ref["source_proposal"])!=dict(proposal)):
                        raise E1PolicyError("reused candidate does not match its frozen campaign source reference")
                    provenance["source_campaign_id"]=source_ref["source_campaign_id"]
                    provenance["source_lineage_id"]=source_ref["source_lineage_id"]
                    provenance["source_config_hash"]=source_ref["source_config_hash"]
                    provenance["raw_cognitive_artifact_id"]=source_ref["raw_cognitive_artifact_id"]
                    provenance["normalized_cognitive_artifact_id"]=source_ref["normalized_cognitive_artifact_id"]
                    validated_provenance[executor_id]=provenance
                    continue
                refs=self.db.execute("""SELECT ci.status,ci.campaign_id,ci.raw_artifact_id,ci.raw_hash,
                    ci.normalized_artifact_id,ci.normalized_hash,t.status AS task_status,
                    r.cognitive_artifact_id AS raw_cognitive_artifact_id,
                    n.cognitive_artifact_id AS normalized_cognitive_artifact_id,
                    r.kind AS raw_kind,n.kind AS normalized_kind
                    FROM runtime.cognitive_invocations ci
                    JOIN runtime.tasks t ON t.task_id=ci.task_id
                    JOIN runtime.cognitive_artifacts r ON r.invocation_id=ci.invocation_id AND r.kind='RAW_OUTPUT'
                    JOIN runtime.cognitive_artifacts n ON n.invocation_id=ci.invocation_id AND n.kind='NORMALIZED_OUTPUT'
                    WHERE ci.invocation_id=%s""",(provenance["invocation_id"],)).fetchone()
                if (not refs or refs["status"]!="SUCCEEDED" or refs["task_status"]!="ACCEPTED"
                        or refs["campaign_id"]!=campaign_id
                        or refs["raw_artifact_id"]!=provenance["raw_artifact_ref"]
                        or refs["normalized_artifact_id"]!=provenance["normalized_artifact_ref"]
                        or refs["raw_hash"]!=provenance["raw_hash"]
                        or refs["normalized_hash"]!=provenance["normalized_hash"]):
                    raise E1PolicyError("model proposal raw/normalized artifacts are not durably verified in this campaign")
                provenance["raw_cognitive_artifact_id"]=refs["raw_cognitive_artifact_id"]
                provenance["normalized_cognitive_artifact_id"]=refs["normalized_cognitive_artifact_id"]
                validated_provenance[executor_id]=provenance
            model_provenance_by_executor=validated_provenance
            candidates_src = [r for r in summary if r["executor_id"] in model_provenance_by_executor]
            if len(candidates_src) < minimum_valid_challengers:
                raise E1PolicyError("fewer than the frozen minimum candidates have complete repeated measurements")
        cases = [{"case_id":f"{task_class}-{i+1}","task_class":task_class,
                  "measurements":row["refs"],"route":row["executor_id"]} for i,row in enumerate(summary)]
        generated_pack_definition = {"scope_id":scope_id,"task_class":task_class,"cases":cases,
            "metric_semantics":{"latency_ms":"median lower is better","quality":"minimum must not regress","cost_units":"mean must remain under policy cap"},
            "promotion_policy":policy["promotion_policy"],"code_revision":code_revision,
            "reproducibility":{"source":"immutable verified executor measurements","seed":"fixed","executor_runs":summary}}
        with self.evaluator_db.transaction():
            suite = self.evaluator_db.execute("SELECT eval_suite_id,version,integrity_hash FROM evolution.eval_suite_versions WHERE scope_id=%s AND status='AUTHORITATIVE'",(scope_id,)).fetchone()
            if not suite:
                raise E1PolicyError("no authoritative frozen suite")
            if frozen_pack_ref is not None:
                pack_id=frozen_pack_ref.get("pack_id")
                expected_hash=frozen_pack_ref.get("definition_hash")
                frozen=self.evaluator_db.execute("""SELECT pack_id,scope_id,definition,definition_hash,
                    suite_id,suite_version,evaluator_id,code_revision FROM evolution.e1_evaluation_pack_versions
                    WHERE pack_id=%s""",(pack_id,)).fetchone()
                if (not frozen or frozen["scope_id"]!=scope_id or frozen["definition_hash"]!=expected_hash
                        or frozen["evaluator_id"]!=self.EVALUATOR or frozen["code_revision"]!=code_revision):
                    raise E1PolicyError("pre-frozen evaluation pack identity or authority does not match")
                pack_definition=dict(frozen["definition"])
                if sha256(pack_definition)!=expected_hash:
                    raise E1PolicyError("pre-frozen evaluation pack content hash mismatch")
                frozen_minimum=int(pack_definition.get("minimum_valid_challengers",3))
                if frozen_minimum!=minimum_valid_challengers:
                    raise E1PolicyError("runtime minimum differs from the pre-frozen evaluation pack")
                if candidate_failures and pack_definition.get("failure_policy",{}).get("version")!="candidate-local-invalid-response-v2":
                    raise E1PolicyError("candidate-local rejection was not enabled by the frozen versioned policy")
                binding=pack_definition.get("suite_binding",{})
                if (binding.get("suite_id")!=suite["eval_suite_id"]
                        or binding.get("version")!=suite["version"]
                        or binding.get("integrity_hash")!=suite["integrity_hash"]
                        or pack_definition.get("task_class")!=task_class
                        or pack_definition.get("code_revision")!=code_revision):
                    raise E1PolicyError("pre-frozen evaluation pack does not bind the current suite/task/revision")
                frozen_routes=set(pack_definition.get("eligible_routes",[]))
                actual_routes={row["executor_id"] for row in summary}
                expected_candidates=frozen_routes-{pref} if frozen_routes else set(model_provenance_by_executor)
                failed_candidates=set((candidate_failures or {}).keys())
                if frozen_routes:
                    validate_frozen_candidate_outcomes(expected_routes=expected_candidates,
                        valid_routes=set(model_provenance_by_executor),failed_routes=failed_candidates,
                        minimum_valid=minimum_valid_challengers)
                    if pref not in frozen_routes or actual_routes!={pref}|set(model_provenance_by_executor):
                        raise E1PolicyError("verified outcomes do not match the frozen evaluation pack")
                elif failed_candidates:
                    raise E1PolicyError("candidate failures require a frozen evaluation pack")
                if not failed_candidates.issubset(expected_candidates):
                    raise E1PolicyError("candidate failure is outside the frozen evaluation portfolio")
                pack_hash=expected_hash
            else:
                pack_definition=generated_pack_definition
                pack_definition["suite_binding"]={"suite_id":suite["eval_suite_id"],
                    "version":suite["version"],"integrity_hash":suite["integrity_hash"]}
                pack_hash=sha256(pack_definition)
                pack_id=_id("e1pack")
                self.evaluator_db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",(f"e1-pack:{scope_id}",))
                pack_version=self.evaluator_db.execute("SELECT coalesce(max(version),0)+1 AS version FROM evolution.e1_evaluation_pack_versions WHERE scope_id=%s",(scope_id,)).fetchone()["version"]
                self.evaluator_db.execute("""INSERT INTO evolution.e1_evaluation_pack_versions
                    (pack_id,scope_id,version,suite_id,suite_version,definition,definition_hash,evaluator_id,evaluator_version,code_revision,created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'1',%s,%s)""",
                    (pack_id,scope_id,pack_version,suite["eval_suite_id"],suite["version"],_jsonb(pack_definition),pack_hash,
                     self.EVALUATOR,code_revision,self.EVALUATOR))
        self._event(scope_id,campaign_id,"EVALUATION_PACK_FROZEN",self.EVALUATOR,{"pack_id":pack_id,"pack_hash":pack_hash,"suite":suite["version"]})
        self.db.execute("UPDATE runtime.improvement_campaigns SET status='EVALUATING' WHERE campaign_id=%s",(campaign_id,))
        current_config = dict(champion_config)
        now = datetime.now(timezone.utc)
        champion_run_id = _id("e1run")
        base_metrics = {"aggregate":{"latency_ms":float(baseline["latency_ms"]),"quality":float(baseline["quality"]),"cost_units":float(baseline["cost_units"]) if baseline["cost_units"] is not None else None}}
        res = self._reserve(campaign_id,"evaluate:champion",{"experiment_units":1,"evaluations":1})
        self.db.commit()
        with self.evaluator_db.transaction():
            self.evaluator_db.execute("""INSERT INTO evolution.e1_evaluation_runs
                (run_id,scope_id,genome_id,baseline_genome_id,pack_id,campaign_id,evaluator_id,evaluator_version,status,metrics,result_refs,reproducible,started_at,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'1','COMPLETED',%s,%s,true,%s,%s)""",
                (champion_run_id,scope_id,champion_id,champion_id,pack_id,campaign_id,self.EVALUATOR,_jsonb(base_metrics),_jsonb(measurements),now,now))
        self.evaluator_db.commit()
        self._settle(res,{"experiment_units":1,"evaluations":1})

        eval_rows: list[dict[str,Any]] = []
        for source in candidates_src:
            route = source["executor_id"]
            model_proposal = dict((model_provenance_by_executor or {}).get(route, {}))
            path = "routing.preference.default"
            if not self._path_allowed(path,policy["allowed_paths"]):
                raise E1PolicyError("mutation path is not in the fixed E1 allowlist")
            new_config = json.loads(canonical_bytes(current_config))
            new_config.setdefault("routing",{}).setdefault("preference",{})["default"] = route
            config_hash = postgres_json_hash(self.db,new_config)
            registered = model_proposal.get("registered_genome_id")
            if registered:
                prior = self.db.execute("""SELECT g.genome_id,g.status,v.parent_genome_id,
                    v.config_hash,l.mutation_id,l.invocation_id,l.proposal_hash,l.campaign_id,l.scope_id
                    FROM evolution.system_genomes g
                    JOIN evolution.e1_genome_versions v USING(genome_id)
                    JOIN evolution.e1_cognitive_proposal_lineage l USING(genome_id)
                    WHERE g.genome_id=%s""",(registered,)).fetchone()
                source_ref_id=model_proposal.get("source_candidate_ref_id")
                expected_source_campaign=model_proposal.get("source_campaign_id",campaign_id)
                source_statuses={"CHALLENGER","RETAINED_FOR_DIVERSITY","SUPERSEDED","REJECTED","CHAMPION"}
                if (not prior or prior["status"] not in (source_statuses if source_ref_id else {"CHALLENGER"})
                        or prior["parent_genome_id"]!=champion_id or prior["config_hash"]!=config_hash
                        or prior["campaign_id"]!=expected_source_campaign or prior["scope_id"]!=scope_id
                        or prior["invocation_id"]!=model_proposal["invocation_id"]
                        or prior["proposal_hash"]!=model_proposal["proposal_hash"]):
                    raise E1PolicyError("pre-registered challenger differs from the frozen proposal lineage")
                if source_ref_id and not self.db.execute("""SELECT 1 FROM evolution.e1_campaign_candidate_refs
                    WHERE candidate_ref_id=%s AND campaign_id=%s AND source_genome_id=%s""",
                    (source_ref_id,campaign_id,registered)).fetchone():
                    raise E1PolicyError("reused challenger has no durable reference in this campaign")
                genome_id=registered; mutation_id=prior["mutation_id"]
            else:
                mutation_id = _id("mut")
                genome_id = _id("genome")
                mutres = self._reserve(campaign_id,f"challenger:{genome_id}",{"experiment_units":1,"challengers":1})
                with self.db.transaction():
                    self.db.execute("""INSERT INTO evolution.mutation_proposals
                        (mutation_id,scope_id,observation_refs,parent_genome_id,candidate_config_ref,candidate_config_hash,
                         hypothesis,expected_effect,created_by,status,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'REGISTERED',%s)""",
                        (mutation_id,scope_id,_jsonb([observation_id]),champion_id,f"sha256:{config_hash}",config_hash,
                         f"Test route {route} using opportunity {opportunity_id}",
                         _jsonb({"latency_ms":"decrease","quality":"no regression"}),self.PROPOSER,
                         _jsonb({"hypothesis_id":hypothesis_id,"changed_paths":[path],"unchanged":"all other E1 fields",
                                 "model_proposal":model_proposal})))
                    self.db.execute("""INSERT INTO evolution.system_genomes
                        (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,mutation_rationale,created_by,metadata)
                        VALUES (%s,%s,%s,'CHALLENGER',%s,%s,%s,%s,%s,%s)""",
                        (genome_id,scope_id,f"e1-{genome_id[-8:]}",f"sha256:{config_hash}",config_hash,
                         f"routing preference to {route}",f"minimum mutation to test {hypothesis_id}",self.PROPOSER,
                         _jsonb({"tier":"E1","mutation_id":mutation_id,"route":route,
                                 "model_proposal":model_proposal})))
                    self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",(genome_id,champion_id))
                    self.db.execute("""INSERT INTO evolution.e1_genome_versions
                        (genome_id,scope_id,tier,parent_genome_id,canonical_config,config_hash,changed_paths,provenance)
                        VALUES (%s,%s,'E1',%s,%s,%s,%s,%s)""",
                        (genome_id,scope_id,champion_id,_jsonb(new_config),config_hash,_jsonb([path]),
                         _jsonb({"created_by":self.PROPOSER,"observation_id":observation_id,"opportunity_id":opportunity_id,
                                 "hypothesis_id":hypothesis_id,"mutation_id":mutation_id,"rollback_target":champion_id,
                                 "model_proposal":model_proposal})))
                    if model_provenance_by_executor is not None:
                        self._persist_cognitive_proposal_lineage(campaign_id=campaign_id,scope_id=scope_id,
                            mutation_id=mutation_id,genome_id=genome_id,model_proposal=model_proposal)
                self._settle(mutres,{"experiment_units":1,"challengers":1})
            mutation_ids.append(mutation_id); challenger_ids.append(genome_id)
            # The challenger identity/lineage is committed before the
            # independent evaluator can reference it through PostgreSQL FKs.
            self.db.commit()
            run_id = _id("e1run")
            started = datetime.now(timezone.utc)
            metrics = {"aggregate":{"latency_ms":float(source["latency_ms"]),"quality":float(source["quality"]),"cost_units":float(source["cost_units"]) if source["cost_units"] is not None else None}}
            evaluation_res = self._reserve(campaign_id,f"evaluate:{genome_id}",{"experiment_units":1,"evaluations":1})
            self.db.commit()
            with self.evaluator_db.transaction():
                self.evaluator_db.execute("""INSERT INTO evolution.e1_evaluation_runs
                    (run_id,scope_id,genome_id,baseline_genome_id,pack_id,campaign_id,evaluator_id,evaluator_version,status,
                     metrics,result_refs,reproducible,started_at,completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'1','COMPLETED',%s,%s,true,%s,%s)""",
                    (run_id,scope_id,genome_id,champion_id,pack_id,campaign_id,self.EVALUATOR,_jsonb(metrics),
                     _jsonb(source["refs"]),started,datetime.now(timezone.utc)))
            self.evaluator_db.commit()
            self._settle(evaluation_res,{"experiment_units":1,"evaluations":1})
            eval_rows.append({"genome_id":genome_id,"run_id":run_id,"metrics":metrics,"route":route,"mutation_id":mutation_id})

        metric = policy["promotion_policy"]["improvement_metric"]
        candidates_gates=[]
        for item in eval_rows:
            m=item["metrics"]["aggregate"]
            gates=compare_e1_metrics(baseline=base_metrics["aggregate"],candidate=m,
                                     policy=policy["promotion_policy"])
            improved,protected,cost_ok=gates["improved"],gates["quality_pass"],gates["monetary_pass"]
            candidates_gates.append((item,improved,protected,cost_ok))
        winners=[x[0] for x in candidates_gates if x[1] and x[2] and x[3]]
        winner = min(winners,key=lambda x:(x["metrics"]["aggregate"][metric],x["route"])) if winners else None
        comparison_by_genome={}
        for item,improved,protected,cost_ok in candidates_gates:
            # Prior comparison updates participate in the artifact-reference
            # advisory protocol. Release them before another independently
            # connected evaluator updates a genome sharing proposal blobs.
            self.db.commit()
            eligible = item is winner
            if eligible:
                disposition="ELIGIBLE"; reason="meets deterministic improvement and protected-quality gates; campaign spend is governed separately"
            elif protected and cost_ok and item["metrics"]["aggregate"][metric] < base_metrics["aggregate"][metric]:
                disposition="RETAINED"; reason="measurable alternative, but not selected under the frozen threshold/candidate ordering"
            else:
                disposition="REJECTED"; reason="fails a protected-quality, cost or improvement gate"
            comparison_id=_id("e1cmp")
            with self.db.transaction():
                self.db.execute("""INSERT INTO evolution.e1_comparisons
                    (comparison_id,scope_id,campaign_id,candidate_genome_id,champion_genome_id,candidate_run_id,champion_run_id,
                     eligible,disposition,rationale) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (comparison_id,scope_id,campaign_id,item["genome_id"],champion_id,item["run_id"],champion_run_id,eligible,
                     disposition,_jsonb({"reason":reason,"candidate":item["metrics"],"champion":base_metrics,"pack_hash":pack_hash,
                         "policy_version":policy.get("policy_version"),"policy_hash":policy.get("policy_hash"),
                         "monetary_comparison":policy["promotion_policy"].get("monetary_evidence_mode","LEGACY_RECORDED"),
                         "campaign_spend_safety_separate":True})))
                status={"ELIGIBLE":"CHALLENGER","RETAINED":"RETAINED_FOR_DIVERSITY","REJECTED":"REJECTED"}[disposition]
                source_provenance=(model_provenance_by_executor or {}).get(item["route"],{})
                if not source_provenance.get("source_candidate_ref_id"):
                    with self.evaluator_db.transaction():
                        self.evaluator_db.execute("UPDATE evolution.system_genomes SET status=%s WHERE genome_id=%s",(status,item["genome_id"]))
                    self.db.execute("UPDATE evolution.mutation_proposals SET status='DECIDED' WHERE mutation_id=%s",(item["mutation_id"],))
                self._event(scope_id,campaign_id,"E1_CHALLENGER_COMPARED",self.EVALUATOR,
                    {"genome_id":item["genome_id"],"comparison_id":comparison_id,"disposition":disposition,"reason":reason})
            comparison_by_genome[item["genome_id"]]=comparison_id

        # A malformed/invalid response rejects only its already-registered
        # candidate. Persist an independent FAILED evaluation and REJECTED
        # comparison so the failure remains first-class and cannot disappear.
        for route, failure in (candidate_failures or {}).items():
            genome_id=failure.get("genome_id")
            mutation_id=failure.get("mutation_id")
            invocation_id=failure.get("invocation_id")
            invocation=self.db.execute("""SELECT ci.status,ci.campaign_id,ci.raw_artifact_id,
                ci.normalized_artifact_id,ci.raw_hash,ci.normalized_hash,ci.started_at,ci.completed_at,
                ci.error_class,t.status AS task_status
                FROM runtime.cognitive_invocations ci JOIN runtime.tasks t USING(task_id)
                WHERE ci.invocation_id=%s AND ci.route_id=%s""",(invocation_id,route)).fetchone()
            if (not invocation or invocation["campaign_id"]!=campaign_id
                    or invocation["status"] not in {"MALFORMED","FAILED","TIMED_OUT","UNAVAILABLE","QUARANTINED"}
                    or invocation["task_status"]=="ACCEPTED"
                    or invocation["raw_artifact_id"]!=failure.get("raw_artifact_ref")
                    or invocation["normalized_artifact_id"]!=failure.get("normalized_artifact_ref")
                    or invocation["raw_hash"]!=failure.get("raw_hash")
                    or invocation["normalized_hash"]!=failure.get("normalized_hash")):
                raise E1PolicyError("candidate-local failure lacks matching durable invocation artifacts")
            registered=self.db.execute("""SELECT g.status,v.parent_genome_id,l.campaign_id,l.scope_id,
                l.invocation_id,l.mutation_id,l.proposal->>'evaluation_pack_hash' AS evaluation_pack_hash,
                cr.candidate_ref_id,cr.source_campaign_id
                FROM evolution.system_genomes g JOIN evolution.e1_genome_versions v USING(genome_id)
                JOIN evolution.e1_cognitive_proposal_lineage l USING(genome_id)
                LEFT JOIN evolution.e1_campaign_candidate_refs cr
                  ON cr.source_genome_id=g.genome_id AND cr.campaign_id=%s
                WHERE g.genome_id=%s""",(campaign_id,genome_id)).fetchone()
            source_ref_id=(model_provenance_by_executor or {}).get(route,{}).get("source_candidate_ref_id")
            expected_source_campaign=(model_provenance_by_executor or {}).get(route,{}).get("source_campaign_id",campaign_id)
            if (not registered or registered["status"] not in ({"CHALLENGER","RETAINED_FOR_DIVERSITY","SUPERSEDED","REJECTED","CHAMPION"} if source_ref_id else {"CHALLENGER"})
                    or registered["parent_genome_id"]!=champion_id
                    or registered["campaign_id"]!=expected_source_campaign or registered["scope_id"]!=scope_id
                    or registered["mutation_id"]!=mutation_id
                    or (not source_ref_id and registered["evaluation_pack_hash"]!=pack_hash)
                    or (source_ref_id and registered["candidate_ref_id"]!=source_ref_id)):
                raise E1PolicyError("failed candidate does not match its frozen durable challenger lineage")
            run_id=_id("e1run"); comparison_id=_id("e1cmp")
            result_refs=[invocation["raw_artifact_id"],invocation["normalized_artifact_id"]]
            failed_metrics={"aggregate":{"latency_ms":None,"quality":0.0,"cost_units":None},
                "candidate_failure":{"error_class":invocation["error_class"],"invocation_id":invocation_id,
                    "policy":"candidate-local-invalid-response-no-retry","pack_hash":pack_hash}}
            started=invocation["started_at"]; completed=invocation["completed_at"] or started
            with self.evaluator_db.transaction():
                self.evaluator_db.execute("""INSERT INTO evolution.e1_evaluation_runs
                    (run_id,scope_id,genome_id,baseline_genome_id,pack_id,campaign_id,evaluator_id,
                     evaluator_version,status,metrics,result_refs,reproducible,started_at,completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'1','FAILED',%s,%s,true,%s,%s)""",
                    (run_id,scope_id,genome_id,champion_id,pack_id,campaign_id,self.EVALUATOR,
                     _jsonb(failed_metrics),_jsonb(result_refs),started,completed))
                if not source_ref_id:
                    self.evaluator_db.execute("UPDATE evolution.system_genomes SET status='REJECTED' WHERE genome_id=%s",
                        (genome_id,))
            self.evaluator_db.commit()
            with self.db.transaction():
                self.db.execute("""INSERT INTO evolution.e1_comparisons
                    (comparison_id,scope_id,campaign_id,candidate_genome_id,champion_genome_id,
                     candidate_run_id,champion_run_id,eligible,disposition,rationale)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,false,'REJECTED',%s)""",
                    (comparison_id,scope_id,campaign_id,genome_id,champion_id,run_id,champion_run_id,
                     _jsonb({"reason":"candidate-local invalid response; no retry",
                         "error_class":invocation["error_class"],"pack_hash":pack_hash,
                         "invocation_id":invocation_id,"raw_artifact_ref":invocation["raw_artifact_id"],
                         "normalized_artifact_ref":invocation["normalized_artifact_id"]})))
                if not source_ref_id:
                    self.db.execute("UPDATE evolution.mutation_proposals SET status='DECIDED' WHERE mutation_id=%s",
                        (mutation_id,))
                self._event(scope_id,campaign_id,"E1_CANDIDATE_REJECTED_INVALID_RESPONSE",self.EVALUATOR,
                    {"genome_id":genome_id,"evaluation_id":run_id,"comparison_id":comparison_id,
                     "invocation_id":invocation_id,"pack_hash":pack_hash,"retry":False})
            comparison_by_genome[genome_id]=comparison_id
            challenger_ids.append(genome_id);mutation_ids.append(mutation_id)

        decision_result=None
        if winner:
            # Evaluation lineage must be committed and visible to the
            # independent promotion identity before its CAS transaction.
            self.db.commit()
            auth_id=_id("e1auth")
            with self.promotion_db.transaction():
                actor=self.promotion_db.execute("SELECT session_user AS actor").fetchone()["actor"]
                promoted=self.promotion_db.execute("SELECT evolution.authorize_and_execute_e1(%s,%s,%s) AS decision",
                    (auth_id,comparison_by_genome[winner["genome_id"]],actor)).fetchone()["decision"]
            decision_result={"authorization_id":auth_id,"decision_id":promoted,"winner":winner["genome_id"],"mode":policy["mode"]}
        with self.db.transaction():
            self.db.execute("UPDATE runtime.improvement_campaigns SET status='COMPLETED',ended_at=now(),stop_reason='COMPLETED' WHERE campaign_id=%s",(campaign_id,))
            self.db.execute("UPDATE runtime.campaigns SET status='COMPLETED' WHERE campaign_id=%s AND status='ACTIVE'",(campaign_id,))
            self._event(scope_id,campaign_id,"CAMPAIGN_COMPLETED",self.PROPOSER,{"opportunity_id":opportunity_id,"challengers":challenger_ids,"decision":decision_result})
        result={"campaign_id":campaign_id,"scope_id":scope_id,"observation_id":observation_id,
            "opportunity_id":opportunity_id,"hypothesis_id":hypothesis_id,"mutation_ids":mutation_ids,
            "challengers":challenger_ids,"pack_id":pack_id,"pack_hash":pack_hash,
            "champion_before":champion_id,"champion_after":self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(scope_id,)).fetchone()["genome_id"],
            "decision":decision_result,"comparisons":comparison_by_genome,"measurement_summary":summary_payload}
        self.db.commit()
        return result

    @staticmethod
    def _path_allowed(path: str, patterns: Sequence[str]) -> bool:
        import fnmatch
        return any(fnmatch.fnmatchcase(path,p) for p in patterns)

    def postpromotion_check(self, *, check_id: str, decision_id: str, scope_id: str,
                            passed: bool, failure_class: str | None,
                            metrics: Mapping[str, Any], evidence_hash: str) -> str | None:
        if not passed and not failure_class:
            raise ValueError("a failed check requires an explicit failure class")
        if self.evaluator_db is None:
            raise E1PolicyError("post-promotion verification requires the evaluator capability")
        with self.evaluator_db.transaction():
            decision=self.evaluator_db.execute("SELECT campaign_id FROM evolution.e1_promotion_decisions WHERE decision_id=%s AND scope_id=%s",
                (decision_id,scope_id)).fetchone()
            if not decision:
                raise ValueError("post-promotion check must reference a known decision")
            self.evaluator_db.execute("""INSERT INTO evolution.e1_postpromotion_checks
                (check_id,decision_id,scope_id,passed,failure_class,metrics,evidence_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (check_id,decision_id,scope_id,passed,failure_class,_jsonb(dict(metrics)),evidence_hash))
        with self.db.transaction():
            self._event(scope_id,decision["campaign_id"],"E1_POSTPROMOTION_CHECK",self.EVALUATOR,
                        {"decision_id":decision_id,"check_id":check_id,"passed":passed,"failure_class":failure_class})
        if passed:
            return None
        if self.promotion_db is None:
            raise E1PolicyError("automatic E1 rollback requires the separate promotion capability")
        with self.promotion_db.transaction():
            actor=self.promotion_db.execute("SELECT session_user AS actor").fetchone()["actor"]
            return self.promotion_db.execute("SELECT evolution.rollback_e1_on_failed_check(%s,%s) AS genome",
                (check_id,actor)).fetchone()["genome"]
