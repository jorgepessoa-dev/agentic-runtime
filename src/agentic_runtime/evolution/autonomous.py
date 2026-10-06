"""Deterministic, bounded opportunity and challenger services.

LLM assisted interpretation can be added behind the same value objects; durable
decisions and policy checks in this module remain authoritative.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.evolution.evaluator import canonical_json, evaluate_routing_genome
from agentic_runtime.evolution.controller import pareto_dominates
from agentic_runtime.persistence.events import evolution_event as _evolution_event, jsonb as _jsonb
from agentic_runtime.contracts.serialization import canonical_bytes as _canonical_json_bytes
from agentic_runtime.contracts.serialization import canonical_hash
from agentic_runtime.artifacts.store import ArtifactIntegrityError, ArtifactStore



def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _canonical(value: Any) -> bytes:
    return _canonical_json_bytes(value)


def _hash(value: Any) -> str:
    return canonical_hash(value)


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    scope_id: str
    goal_id: str
    kind: str
    fingerprint: str
    description: str
    evidence_refs: tuple[str, ...]
    severity: str
    estimated_cost: str = "LOW"
    uncertainty: str = "MODERATE"
    novelty: str = "KNOWN"
    risk: str = "LOW"
    evidence_summary: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class Hypothesis:
    hypothesis_id: str
    opportunity_id: str
    scope_id: str
    statement: str
    mechanism: str
    supporting_evidence_refs: tuple[str, ...]
    contradicting_evidence_refs: tuple[str, ...]
    falsification_criteria: str
    expected_effect: Mapping[str, Any]
    estimated_cost: str
    risk_class: str
    created_by: str = "deterministic-hypothesis-generator"
    search_scope: str = "prior task outcomes for matching task type"
    contradiction_search_completed: bool = False


class OpportunityDiscoveryService:
    """Reads bounded aggregate evidence and persists explainable opportunities."""

    def __init__(self, connection: Any) -> None:
        self.db = connection

    def scan_task_outcomes(self, *, scope_id: str, goal_id: str,
                           minimum_attempts: int = 3) -> list[Opportunity]:
        rows = self.db.execute("""SELECT capability AS task_type, count(*) AS attempts,
            count(*) FILTER (WHERE t.status='ACCEPTED') AS accepted,
            count(*) FILTER (WHERE t.status IN ('FAILED_TRANSIENT','FAILED_PERMANENT','QUARANTINED')) AS failed,
            coalesce(avg(extract(epoch FROM (a.completed_at-a.started_at))*1000)
                     FILTER (WHERE a.completed_at IS NOT NULL),0)::bigint AS latency_ms
          FROM runtime.tasks t
          CROSS JOIN LATERAL jsonb_array_elements_text(t.required_capabilities) AS capability
          LEFT JOIN runtime.attempts a USING(task_id)
          WHERE t.goal_id=%s GROUP BY capability ORDER BY capability""", (goal_id,)).fetchall()
        found: list[Opportunity] = []
        for row in rows:
            if row["attempts"] < minimum_attempts:
                continue
            if not row["failed"]:
                continue
            evidence = {"goal_id": goal_id, "task_type": row["task_type"],
                        "attempts": row["attempts"], "failed": row["failed"],
                        "accepted": row["accepted"], "latency_ms": row["latency_ms"]}
            fingerprint = _hash({"scope": scope_id, "kind": "RELIABILITY", "task_type": row["task_type"]})
            opportunity = Opportunity(_id("opp"), scope_id, goal_id, "WEAKNESS", fingerprint,
                f"Repeated failures for task capability {row['task_type']}",
                (f"metric:{_hash(evidence)}",), "HIGH", evidence_summary=evidence)
            found.append(self.persist(opportunity, evidence=evidence))
        return found

    def persist(self, opportunity: Opportunity, *, evidence: Mapping[str, Any]) -> Opportunity:
        from agentic_runtime.coordinator.service import Coordinator
        coordinator = Coordinator(self.db)
        with self.db.transaction():
            self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,%s) ON CONFLICT(scope_id) DO NOTHING",
                            (opportunity.scope_id,"Autonomous improvement scope"))
            prior = self.db.execute("""SELECT opportunity_id FROM runtime.opportunities
                WHERE scope_id=%s AND fingerprint=%s AND status IN
                ('OPEN','INVESTIGATING','ACTIONABLE','DETECTED','TRIAGED','SELECTED','UNDER_INVESTIGATION')
                FOR UPDATE""", (opportunity.scope_id, opportunity.fingerprint)).fetchone()
            if prior:
                self.db.execute("UPDATE runtime.opportunities SET observation_refs=observation_refs||%s::jsonb WHERE opportunity_id=%s",
                    (_jsonb(list(opportunity.evidence_refs)), prior["opportunity_id"]))
                return Opportunity(prior["opportunity_id"], opportunity.scope_id, opportunity.goal_id,
                    opportunity.kind, opportunity.fingerprint, opportunity.description,
                    opportunity.evidence_refs, opportunity.severity,
                    evidence_summary=opportunity.evidence_summary)
            self.db.execute("""INSERT INTO runtime.opportunities
                (opportunity_id,goal_id,kind,observation_refs,description,status,estimated_value,
                 scope_id,fingerprint,estimated_cost,uncertainty,novelty,risk,triage_rationale,metadata)
                VALUES (%s,%s,%s,%s,%s,'DETECTED',%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (opportunity.opportunity_id, opportunity.goal_id, opportunity.kind,
                 _jsonb(list(opportunity.evidence_refs)), opportunity.description,
                 _jsonb({"severity": opportunity.severity}), opportunity.scope_id,
                 opportunity.fingerprint, _jsonb({"class": opportunity.estimated_cost}),
                 opportunity.uncertainty, opportunity.novelty, opportunity.risk,
                 "Deterministic durable task outcome aggregation", _jsonb(dict(evidence))))
            coordinator._event(event_type="OPPORTUNITY_DETECTED", actor_id="opportunity-discovery",
                correlation_id=opportunity.opportunity_id, goal_id=opportunity.goal_id,
                payload={"scope_id": opportunity.scope_id, "kind": opportunity.kind,
                         "fingerprint": opportunity.fingerprint, "evidence": dict(evidence)})
        return opportunity

    def prioritize(self, opportunity: Opportunity, *, campaign_id: str | None = None,
                   selected: bool = True) -> str:
        # Hard filters and categorical severity avoid invented universal weights.
        rationale = {"severity": opportunity.severity, "evidence_count": len(opportunity.evidence_refs),
                     "cost_class": opportunity.estimated_cost, "risk_class": opportunity.risk,
                     "novelty": opportunity.novelty, "policy": "severity/evidence first; low cost and reversible preferred"}
        budget_class = "EXPLORE" if opportunity.kind == "STAGNATION" or opportunity.novelty == "NOVEL" else "EXPLOIT"
        priority = "P0" if opportunity.severity == "HIGH" or opportunity.kind == "STAGNATION" else "P1" if selected else "DEFER"
        decision_id = _id("opdec")
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.opportunity_decisions
                (decision_id,opportunity_id,selected,priority_class,rationale,campaign_id,budget_class)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (decision_id, opportunity.opportunity_id, selected, priority, _jsonb(rationale), campaign_id,budget_class))
            self.db.execute("UPDATE runtime.opportunities SET status=%s,triage_rationale=%s WHERE opportunity_id=%s",
                ("SELECTED" if selected else "DEFERRED", json.dumps(rationale, sort_keys=True), opportunity.opportunity_id))
            _evolution_event(self.db,"OPPORTUNITY_SELECTED" if selected else "OPPORTUNITY_DEFERRED",
                scope_id=opportunity.scope_id,actor_id="opportunity-prioritizer",
                payload={"opportunity_id":opportunity.opportunity_id,"priority":priority,"budget_class":budget_class,"rationale":rationale})
        return decision_id


class HypothesisGenerator:
    def generate(self, opportunity: Opportunity, *, evidence_summary: Mapping[str, Any],
                 contradicting_refs: Sequence[str] = (),
                 search_scope: str = "prior task outcomes for matching task capability",
                 contradiction_search_completed: bool = False) -> Hypothesis:
        task_type = str(evidence_summary.get("task_type", "affected capability"))
        statement = f"A bounded routing or workflow change for {task_type} will reduce verified failures without reducing acceptance quality."
        return Hypothesis(_id("hyp"), opportunity.opportunity_id, opportunity.scope_id,
            statement, "Repeated failures indicate a potentially avoidable capability/executor mismatch.",
            opportunity.evidence_refs, tuple(contradicting_refs),
            "Falsify if matched evaluation has no lower failure count or lowers verified acceptance.",
            {"failure_rate": "decrease", "verified_quality": "no_regression"}, "LOW", "LOW",
            search_scope=search_scope,contradiction_search_completed=contradiction_search_completed)

    def persist(self, db: Any, hypothesis: Hypothesis) -> str:
        with db.transaction():
            db.execute("""INSERT INTO runtime.hypotheses
                (hypothesis_id,opportunity_id,statement,falsification_ref,status,created_by,metadata)
                VALUES (%s,%s,%s,%s,'PROPOSED',%s,%s)""",
                (hypothesis.hypothesis_id, hypothesis.opportunity_id, hypothesis.statement,
                 hypothesis.falsification_criteria, hypothesis.created_by,
                 _jsonb({"mechanism": hypothesis.mechanism,"expected_effect": dict(hypothesis.expected_effect),
                         "estimated_cost": hypothesis.estimated_cost,"risk_class": hypothesis.risk_class,
                         "scope_id": hypothesis.scope_id})))
            for ref in hypothesis.supporting_evidence_refs:
                db.execute("INSERT INTO runtime.hypothesis_evidence(hypothesis_id,evidence_ref,relation) VALUES (%s,%s,'SUPPORTS')",
                           (hypothesis.hypothesis_id, ref))
            for ref in hypothesis.contradicting_evidence_refs:
                db.execute("INSERT INTO runtime.hypothesis_evidence(hypothesis_id,evidence_ref,relation) VALUES (%s,%s,'CONTRADICTS')",
                           (hypothesis.hypothesis_id, ref))
            if not hypothesis.contradicting_evidence_refs and hypothesis.contradiction_search_completed:
                db.execute("INSERT INTO runtime.hypothesis_evidence(hypothesis_id,evidence_ref,relation,search_scope) VALUES (%s,%s,'SEARCHED_NO_RESULT',%s)",
                (hypothesis.hypothesis_id, "none-found:" + _hash(hypothesis.search_scope), hypothesis.search_scope))
            _evolution_event(db,"HYPOTHESIS_CREATED",scope_id=hypothesis.scope_id,
                actor_id=hypothesis.created_by,payload={"hypothesis_id":hypothesis.hypothesis_id,
                "opportunity_id":hypothesis.opportunity_id,"supporting":list(hypothesis.supporting_evidence_refs),
                "contradicting":list(hypothesis.contradicting_evidence_refs),
                "falsification_criteria":hypothesis.falsification_criteria})
        return hypothesis.hypothesis_id


class AutonomousMutationPolicy:
    def __init__(self, db: Any) -> None:
        self.db = db

    def authorize(self, *, path: str, tier: str, evaluation: bool = True) -> bool:
        rows = self.db.execute("SELECT path_pattern,tier,autonomous_proposal,autonomous_evaluation FROM runtime.mutation_policy").fetchall()
        for row in rows:
            if fnmatch.fnmatchcase(path, row["path_pattern"]):
                if row["tier"] != tier or not row["autonomous_proposal"] or (evaluation and not row["autonomous_evaluation"]):
                    return False
                return True
        return False

    def validate_patch(self, *, tier: str, changes: Mapping[str, Any]) -> None:
        if not changes:
            raise ValueError("mutation must change at least one allowlisted field")
        for path in changes:
            if path.startswith(("mission.", "governance.", "evaluation.", "security.", "artifact.", "lease.", "runtime_code.", "credential.")):
                raise ValueError(f"forbidden mutation path: {path}")
            if not self.authorize(path=path, tier=tier):
                raise ValueError(f"mutation is outside autonomous allowlist: {path}")


@dataclass(frozen=True)
class CampaignLimits:
    max_opportunities: int = 5
    max_selected_opportunities: int = 2
    max_hypotheses_per_opportunity: int = 3
    max_challengers_per_hypothesis: int = 2
    max_parallel_evaluations: int = 2
    max_campaign_children: int = 12
    max_hypotheses: int = 6
    max_challengers: int = 4
    max_evaluations: int = 4
    max_attempts: int = 12
    max_wall_time_seconds: int = 900


class ImprovementCampaignService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def create(self, *, campaign_id: str, scope_id: str, goal_id: str | None,
               budget: Mapping[str, Any], created_by: str,
               limits: CampaignLimits = CampaignLimits(),
               exploration: Mapping[str, Any] | None = None,
               exploitation: Mapping[str, Any] | None = None) -> str:
        if any(int(v) < 0 for v in budget.values() if isinstance(v, (int, float))):
            raise ValueError("campaign budget dimensions cannot be negative")
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.improvement_campaigns
                (campaign_id,scope_id,goal_id,status,budget,exploration_allocation,exploitation_allocation,limits,created_by)
                VALUES (%s,%s,%s,'CREATED',%s,%s,%s,%s,%s)""",
                (campaign_id,scope_id,goal_id,_jsonb(dict(budget)),_jsonb(dict(exploration or {})),
                 _jsonb(dict(exploitation or {})),_jsonb(limits.__dict__),created_by))
            _evolution_event(self.db,"IMPROVEMENT_CAMPAIGN_CREATED",scope_id=scope_id,actor_id=created_by,
                payload={"campaign_id":campaign_id,"budget":dict(budget),"limits":limits.__dict__})
        return campaign_id

    def stop(self, campaign_id: str, reason: str) -> None:
        if reason not in {"BUDGET_EXHAUSTED","DEADLINE_REACHED","MAX_OPPORTUNITIES_REACHED",
                          "MAX_CHALLENGERS_REACHED","NO_ELIGIBLE_OPPORTUNITIES",
                          "DUPLICATE_LOOP_DETECTED","EVALUATION_UNAVAILABLE",
                          "GOVERNANCE_VIOLATION","COMPLETED"}:
            raise ValueError("campaign stop reason is not recognized")
        with self.db.transaction():
            state = "COMPLETED" if reason == "COMPLETED" else "STOPPED"
            self.db.execute("UPDATE runtime.improvement_campaigns SET status=%s,ended_at=now(),stopped_at=CASE WHEN %s='STOPPED' THEN now() ELSE stopped_at END,stop_reason=%s WHERE campaign_id=%s AND status NOT IN ('COMPLETED','STOPPED')",
                            (state,state,reason,campaign_id))
            _evolution_event(self.db,"CAMPAIGN_STOPPED",scope_id=None,actor_id="campaign-controller",
                payload={"campaign_id":campaign_id,"reason":reason})

    def reserve(self, *, campaign_id: str, budget_class: str, dimensions: Mapping[str, int | float],
                idempotency_key: str) -> dict[str, Any]:
        """Reserve campaign/class quotas before creating hypotheses or evaluation work."""
        if budget_class not in {"EXPLOIT","EXPLORE"} or any(float(v) < 0 for v in dimensions.values()):
            raise ValueError("invalid campaign reservation")
        terminal_reason: str | None = None
        result: dict[str, Any] | None = None
        with self.db.transaction():
            campaign = self.db.execute("SELECT * FROM runtime.improvement_campaigns WHERE campaign_id=%s FOR UPDATE",
                                       (campaign_id,)).fetchone()
            if not campaign or campaign["status"] in {"STOPPED","COMPLETED"}:
                raise ValueError("campaign is stopped or absent")
            prior = self.db.execute("SELECT budget_class,dimensions FROM runtime.improvement_budget_ledger WHERE campaign_id=%s AND idempotency_key=%s",
                                    (campaign_id,idempotency_key+":reserve")).fetchone()
            if prior:
                if prior["budget_class"] != budget_class or prior["dimensions"] != dict(dimensions):
                    raise ValueError("reservation idempotency key reused with different budget")
                return {"reserved":True,"duplicate":True,"dimensions":dict(dimensions)}
            limits = campaign["limits"] or {}
            elapsed = self.db.execute("SELECT extract(epoch FROM now()-%s)::float8 AS seconds",(campaign["created_at"],)).fetchone()["seconds"]
            wall_limit = limits.get("max_wall_time_seconds")
            counts = self.db.execute("SELECT budget_class,action,dimensions FROM runtime.improvement_budget_ledger WHERE campaign_id=%s",
                                     (campaign_id,)).fetchall()
            prior_count: dict[str,float] = {}
            total_experiments = 0.0
            class_experiments = 0.0
            for row in counts:
                # Limits count authorized work once, when reserved; consumption is
                # accounting evidence and must not double the same commitment.
                sign = 1.0 if row["action"] == "RESERVE" else -1.0 if row["action"] == "RELEASE" else 0.0
                for key,value in (row["dimensions"] or {}).items():
                    if key in {"opportunities","selected_opportunities","hypotheses","challengers","evaluations","children","attempts"}:
                        prior_count[key] = prior_count.get(key,0.0) + sign*float(value)
                    if key == "experiment_units" and row["action"] == "RESERVE":
                        total_experiments += float(value)
                        if row["budget_class"] == budget_class: class_experiments += float(value)
            accounted=self.db.execute("""SELECT r.budget_class,d.reserved,d.consumed,r.status
                FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
                WHERE r.campaign_id=%s AND d.dimension='experiment_units' AND r.status IN ('RESERVED','SETTLED','UNKNOWN')""",
                (campaign_id,)).fetchall()
            for row in accounted:
                amount=float((row["reserved"] if row["status"]=="RESERVED" else row["consumed"]) or 0)
                total_experiments+=amount
                if row["budget_class"]==budget_class: class_experiments+=amount
            requested_exp = float(dimensions.get("experiment_units",0))
            global_exp_limit = float((campaign["budget"] or {}).get("max_experiment_units",0))
            allocation = campaign["exploration_allocation" if budget_class == "EXPLORE" else "exploitation_allocation"] or {}
            class_exp_limit = float(allocation.get("experiment_units",0))
            if wall_limit is not None and elapsed >= float(wall_limit):
                terminal_reason = "DEADLINE_REACHED"
            elif any(prior_count.get(key,0)+float(dimensions.get(key,0)) > float(limits.get(limit_key,float("inf")))
                     for key,limit_key in (("opportunities","max_opportunities"),("selected_opportunities","max_selected_opportunities"),
                       ("hypotheses","max_hypotheses"),("challengers","max_challengers"),("evaluations","max_evaluations"),
                       ("children","max_campaign_children"),("attempts","max_attempts"))):
                terminal_reason = "MAX_OPPORTUNITIES_REACHED" if prior_count.get("opportunities",0)+dimensions.get("opportunities",0) > limits.get("max_opportunities",float("inf")) else "MAX_CHALLENGERS_REACHED"
            elif requested_exp and (total_experiments+requested_exp > global_exp_limit or class_experiments+requested_exp > class_exp_limit):
                terminal_reason = "BUDGET_EXHAUSTED"
            else:
                budget = campaign["budget"] or {}
                reserved_budget = campaign["reserved_budget"] or {}
                consumed_budget = campaign["consumed_budget"] or {}
                for key in ("max_cost","max_tokens","max_wall_time"):
                    if key in budget:
                        if key not in dimensions or float(reserved_budget.get(key,0))+float(consumed_budget.get(key,0))+float(dimensions[key]) > float(budget[key]):
                            terminal_reason = "BUDGET_EXHAUSTED"
                            break
            if terminal_reason:
                self.db.execute("UPDATE runtime.improvement_campaigns SET status='STOPPED',stop_reason=%s,ended_at=now(),stopped_at=now() WHERE campaign_id=%s",
                                (terminal_reason,campaign_id))
                _evolution_event(self.db,"CAMPAIGN_STOPPED",scope_id=campaign["scope_id"],actor_id="campaign-controller",
                    payload={"campaign_id":campaign_id,"reason":terminal_reason,"requested":dict(dimensions)})
            else:
                self.db.execute("INSERT INTO runtime.improvement_budget_ledger(entry_id,campaign_id,idempotency_key,budget_class,action,dimensions) VALUES (%s,%s,%s,%s,'RESERVE',%s)",
                    (_id("bledger"),campaign_id,idempotency_key+":reserve",budget_class,_jsonb(dict(dimensions))))
                reserved = dict(campaign["reserved_budget"] or {})
                for key,value in dimensions.items():
                    reserved[key] = float(reserved.get(key,0))+float(value)
                self.db.execute("UPDATE runtime.improvement_campaigns SET reserved_budget=%s WHERE campaign_id=%s",
                                (_jsonb(reserved),campaign_id))
                result={"reserved":True,"duplicate":False,"dimensions":dict(dimensions),"budget_class":budget_class}
        if terminal_reason:
            raise ValueError(f"campaign stopped: {terminal_reason}")
        assert result is not None
        return result

    def settle(self, *, campaign_id: str, budget_class: str, idempotency_key: str,
               usage: Mapping[str, int | float]) -> None:
        with self.db.transaction():
            row = self.db.execute("SELECT dimensions FROM runtime.improvement_budget_ledger WHERE campaign_id=%s AND idempotency_key=%s FOR UPDATE",
                                   (campaign_id,idempotency_key+":reserve")).fetchone()
            if not row:
                raise ValueError("cannot consume unreserved campaign work")
            prior = self.db.execute("SELECT 1 FROM runtime.improvement_budget_ledger WHERE campaign_id=%s AND idempotency_key=%s",
                                    (campaign_id,idempotency_key+":consume")).fetchone()
            if prior:
                return
            reserved = row["dimensions"] or {}
            if any(float(usage.get(key,0)) > float(reserved.get(key,0)) for key in usage):
                raise ValueError("actual campaign usage exceeded its reservation")
            campaign = self.db.execute("SELECT reserved_budget,consumed_budget FROM runtime.improvement_campaigns WHERE campaign_id=%s FOR UPDATE",
                                       (campaign_id,)).fetchone()
            rbudget=dict(campaign["reserved_budget"] or {}); consumed=dict(campaign["consumed_budget"] or {})
            for key,value in reserved.items(): rbudget[key]=max(0.0,float(rbudget.get(key,0))-float(value))
            for key,value in usage.items(): consumed[key]=float(consumed.get(key,0))+float(value)
            self.db.execute("UPDATE runtime.improvement_campaigns SET reserved_budget=%s,consumed_budget=%s WHERE campaign_id=%s",
                            (_jsonb(rbudget),_jsonb(consumed),campaign_id))
            self.db.execute("INSERT INTO runtime.improvement_budget_ledger(entry_id,campaign_id,idempotency_key,budget_class,action,dimensions) VALUES (%s,%s,%s,%s,'CONSUME',%s)",
                (_id("bledger"),campaign_id,idempotency_key+":consume",budget_class,_jsonb(dict(usage))))


class StagnationDetector:
    def __init__(self, db: Any) -> None:
        self.db = db

    def detect(self, *, scope_id: str, evaluation_count: int = 3) -> dict[str, Any] | None:
        rows = self.db.execute("""SELECT evaluation_id,metrics,created_at FROM evolution.evaluation_records
            WHERE scope_id=%s AND status='COMPLETED' ORDER BY created_at DESC LIMIT %s""",
            (scope_id,evaluation_count)).fetchall()
        if len(rows) < evaluation_count:
            return None
        # A plateau is a sequence with no candidate Pareto win over its champion.
        no_win = all((row["metrics"] or {}).get("candidate") == (row["metrics"] or {}).get("champion")
                     or not (row["metrics"] or {}).get("candidate") for row in rows)
        if not no_win:
            return None
        summary = {"scope_id":scope_id,"evaluation_refs":[row["evaluation_id"] for row in rows],
                   "signal":"no measurable gain across recent completed evaluations",
                   "response":"raise priority of heterogeneous and materially different exploration"}
        return summary

    def detect_and_persist(self, *, scope_id: str, goal_id: str,
                           evaluation_count: int = 3) -> dict[str, Any] | None:
        signal = self.detect(scope_id=scope_id, evaluation_count=evaluation_count)
        if signal is None:
            return None
        evidence_refs = tuple(f"evaluation:{ref}" for ref in signal["evaluation_refs"])
        fingerprint = _hash({"scope": scope_id, "kind": "STAGNATION",
                             "evaluations": sorted(signal["evaluation_refs"])})
        opportunity = Opportunity(_id("opp"), scope_id, goal_id, "STAGNATION", fingerprint,
            signal["signal"], evidence_refs, "HIGH", novelty="NOVEL",
            evidence_summary=signal)
        discovery = OpportunityDiscoveryService(self.db)
        with self.db.transaction():
            observation_id = _id("obs")
            self.db.execute("""INSERT INTO evolution.observations
                (observation_id,scope_id,source_refs,summary,created_by,metadata)
                VALUES (%s,%s,%s,%s,'stagnation-detector',%s)""",
                (observation_id,scope_id,_jsonb(list(evidence_refs)),signal["signal"],_jsonb(signal)))
            persisted = discovery.persist(opportunity, evidence=signal)
            _evolution_event(self.db,"STAGNATION_DETECTED",scope_id=scope_id,
                actor_id="stagnation-detector",payload={"observation_id":observation_id,
                "opportunity_id":persisted.opportunity_id,"evaluation_refs":signal["evaluation_refs"]})
        return {**signal,"observation_id":observation_id,"opportunity":persisted}


class EvaluatorWorker:
    """Deterministic evaluator identity and result recorder; no promotion authority."""

    evaluator_id = "deterministic-evaluator-v1"
    implementation_version = "1"

    def record_run(self, db: Any, *, evaluator_run_id: str, evaluation_id: str,
                   execution_ref: str | None = None) -> None:
        db.execute("""INSERT INTO evolution.evaluator_runs
            (evaluator_run_id,evaluation_id,evaluator_id,evaluator_type,implementation_version,independence_relation,execution_ref)
            VALUES (%s,%s,%s,'DETERMINISTIC','1','DETERMINISTIC',%s)""",
            (evaluator_run_id,evaluation_id,self.evaluator_id,execution_ref))


class EvalSuiteArtifactResolver:
    """Loads evaluation definitions only through the persisted immutable artifact reference."""

    def __init__(self, db: Any, store: ArtifactStore, evaluator_db: Any | None = None) -> None:
        self.db = db
        self.store = store
        self.evaluator_db = evaluator_db or db

    def resolve(self, eval_suite_id: str, version: str) -> tuple[dict[str, Any], dict[str, Any]]:
        suite = self.db.execute("""SELECT eval_suite_id,version,scope_id,status,definition_ref,integrity_hash,metadata
            FROM evolution.eval_suite_versions WHERE eval_suite_id=%s AND version=%s""",
            (eval_suite_id,version)).fetchone()
        if not suite or suite["status"] != "AUTHORITATIVE":
            raise ValueError("evaluation suite version is not authoritative")
        try:
            if not suite["definition_ref"].startswith("sha256:"):
                raise ArtifactIntegrityError("evaluation suite reference is not content addressed")
            manifest = self.store.verify(suite["definition_ref"])
            raw = self.store.read(suite["definition_ref"])
            definition = json.loads(raw)
            observed_hash = hashlib.sha256(canonical_json(definition)).hexdigest()
            if observed_hash != suite["integrity_hash"] or manifest.content_hash != suite["definition_ref"].split(":",1)[1]:
                raise ArtifactIntegrityError("evaluation suite artifact integrity hash mismatch")
        except (ArtifactIntegrityError, OSError, ValueError, json.JSONDecodeError) as exc:
            with self.evaluator_db.transaction():
                self.evaluator_db.execute("""INSERT INTO evolution.eval_artifact_quarantine
                    (quarantine_id,eval_suite_id,eval_suite_version,definition_ref,expected_hash,observed_hash,reason,created_by)
                    VALUES (%s,%s,%s,%s,%s,NULL,%s,'evaluation-suite-resolver')""",
                    (_id("eq"),eval_suite_id,version,suite["definition_ref"],suite["integrity_hash"],str(exc)))
                _evolution_event(self.evaluator_db,"EVAL_ARTIFACT_QUARANTINED",scope_id=suite["scope_id"],
                    actor_id="evaluation-suite-resolver",payload={"suite_id":eval_suite_id,"version":version,
                    "definition_ref":suite["definition_ref"],"reason":str(exc)})
            raise ValueError("trusted evaluation artifact failed verification; evaluation rejected") from exc
        return suite, definition


class AutonomousImprovementCampaign:
    """Runs one bounded routing challenger from observed failures to a shadow decision."""

    def __init__(self, db: Any, artifact_store: ArtifactStore, evaluator_db: Any | None = None) -> None:
        self.db = db
        self.evaluator_db = evaluator_db or db
        self.resolver = EvalSuiteArtifactResolver(db,artifact_store,self.evaluator_db)
        self.discovery = OpportunityDiscoveryService(db)
        self.hypotheses = HypothesisGenerator()
        self.mutations = ChallengerGenerator(db)

    def run_routing_once(self, *, campaign_id: str, scope_id: str, goal_id: str,
                         champion_config: Mapping[str, Any],
                         code_revision: str, evaluator_id: str = "deterministic-evaluator-v1") -> dict[str, Any]:
        campaign = self.db.execute("SELECT * FROM runtime.improvement_campaigns WHERE campaign_id=%s FOR UPDATE", (campaign_id,)).fetchone()
        if not campaign or campaign["status"] in {"STOPPED", "COMPLETED"}:
            raise ValueError("improvement campaign is absent or stopped")
        accounting=CampaignAccounting(self.db)
        default_budget_class="EXPLOIT" if float((campaign["exploitation_allocation"] or {}).get("experiment_units",0))>0 else "EXPLORE"
        def reserve_stage(stage: str, dimensions: Mapping[str, int | float], budget_class: str | None = None) -> str:
            bounded_dimensions={"experiment_units":0,"tokens_input":0,"tokens_output":0,**dict(dimensions),"monetary_cost":0}
            reservation=accounting.reserve(campaign_id=campaign_id,stage=stage,
                idempotency_key=f"{campaign_id}:stage:{stage}",
                amounts=bounded_dimensions,budget_class=budget_class or default_budget_class)
            accounting.mark_dispatched(reservation)
            return reservation
        def settle_stage(reservation: str, usage: Mapping[str, int | float]) -> None:
            accounting.settle(reservation,actual={"experiment_units":0,"tokens_input":0,
                "tokens_output":0,"monetary_cost":0,**dict(usage)},terminal_status="SUCCEEDED")
        observation_reservation=reserve_stage("opportunity_investigation",
            {"experiment_units":1,"tokens_input":0,"tokens_output":0})
        limits = campaign["limits"] or {}
        count = self.db.execute("SELECT count(*) AS n FROM evolution.promotion_decisions WHERE scope_id=%s AND created_at >= %s",
                                (scope_id,campaign["created_at"])).fetchone()["n"]
        if count >= int(limits.get("max_challengers_per_hypothesis", 2)):
            ImprovementCampaignService(self.db).stop(campaign_id,"MAX_CHALLENGERS_REACHED")
            raise ValueError("campaign challenger limit reached")
        opportunities = self.discovery.scan_task_outcomes(scope_id=scope_id,goal_id=goal_id)
        settle_stage(observation_reservation,{"experiment_units":1})
        exploration_blocker = "NO_EXPLORATION_ALLOCATION"
        if not opportunities:
            # A healthy champion still receives bounded exploration when an
            # explicit explore allocation and a capability-compatible route exist.
            allocation = campaign["exploration_allocation"] or {}
            routes = champion_config.get("routes") or {}
            capability = None
            if float(allocation.get("experiment_units", 0)) > 0:
                for candidate_capability in sorted(routes):
                    eligible = self.db.execute("""SELECT executor_id,capabilities FROM runtime.executors
                        WHERE enabled AND capabilities @> %s ORDER BY executor_id""",
                        (_jsonb([candidate_capability]),)).fetchall()
                    if any(r["executor_id"] != routes[candidate_capability] for r in eligible):
                        capability = candidate_capability
                        break
            if capability:
                champion = self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s", (scope_id,)).fetchone()
                if champion:
                    evidence = {"scope_id":scope_id,"capability":capability,"task_type":capability,
                        "champion_genome_id":champion["genome_id"],"signal":"exploration allocation available while current route has no measured failure"}
                    fingerprint = _hash({"scope":scope_id,"kind":"EXPLORATION","capability":capability,
                                          "champion":champion["genome_id"]})
                    novel = Opportunity(_id("opp"),scope_id,goal_id,"OPPORTUNITY",fingerprint,
                        "Test a capability-compatible alternative while the current route has no measured failure",
                        (f"champion:{champion['genome_id']}",),"LOW",novelty="NOVEL",evidence_summary=evidence)
                    opportunities = [self.discovery.persist(novel,evidence=evidence)]
                elif not champion:
                    exploration_blocker = "NO_ACTIVE_CHAMPION"
            elif not capability:
                exploration_blocker = "NO_ROUTED_CAPABILITY"
        if not opportunities:
            ImprovementCampaignService(self.db).stop(campaign_id,"NO_ELIGIBLE_OPPORTUNITIES")
            return {"status":"STOPPED","reason":"NO_ELIGIBLE_OPPORTUNITIES","detail":exploration_blocker}
        opportunity = opportunities[0]
        self.discovery.prioritize(opportunity,campaign_id=campaign_id,selected=True)
        budget_class = self.db.execute("SELECT budget_class FROM runtime.opportunity_decisions WHERE campaign_id=%s AND opportunity_id=%s ORDER BY created_at DESC LIMIT 1",
                                       (campaign_id,opportunity.opportunity_id)).fetchone()["budget_class"]
        capability = str((opportunity.evidence_summary or {}).get("task_type", ""))
        if not capability:
            raise ValueError("opportunity has no measured task capability")
        current_route = (champion_config.get("routes") or {}).get(capability)
        candidates = self.db.execute("""SELECT executor_id FROM runtime.executors
            WHERE enabled AND capabilities @> %s ORDER BY executor_id""",
            (_jsonb([capability]),)).fetchall()
        alternative = next((row["executor_id"] for row in candidates if row["executor_id"] != current_route), None)
        if alternative is None:
            ImprovementCampaignService(self.db).stop(campaign_id,"NO_ELIGIBLE_OPPORTUNITIES")
            return {"status":"STOPPED","reason":"NO_ALTERNATIVE_EXECUTOR"}
        proposal_fingerprint = _hash({"scope":scope_id,"changes":{f"routes.{capability}":alternative}})
        evidence_fingerprint = _hash(sorted(opportunity.evidence_refs))
        if self.db.execute("""SELECT 1 FROM evolution.mutation_proposals
            WHERE scope_id=%s AND proposal_fingerprint=%s AND evidence_fingerprint=%s LIMIT 1""",
            (scope_id,proposal_fingerprint,evidence_fingerprint)).fetchone():
            ImprovementCampaignService(self.db).stop(campaign_id,"DUPLICATE_LOOP_DETECTED")
            raise ValueError("materially duplicate mutation proposal suppressed")
        ImprovementCampaignService(self.db).reserve(campaign_id=campaign_id,budget_class=budget_class,
            dimensions={"opportunities":1,"selected_opportunities":1,"hypotheses":1,
                "challengers":1,"evaluations":1},
            idempotency_key=f"{campaign_id}:{opportunity.fingerprint}:{alternative}")
        hypothesis_reservation=reserve_stage("hypothesis_generation",{"hypotheses":1,"tokens_input":0,"tokens_output":0},budget_class)
        challenger_reservation=reserve_stage("challenger_creation",{"challengers":1},budget_class)
        candidate_config = self.mutations.minimal_candidate(tier="E1",config=champion_config,
            changes={f"routes.{capability}":alternative})
        counter_rows = self.db.execute("""SELECT task_id,status FROM runtime.tasks
            WHERE goal_id=%s AND required_capabilities ? %s AND status='ACCEPTED' ORDER BY task_id""",
            (goal_id,capability)).fetchall()
        counter_refs = [f"task:{row['task_id']}:{row['status']}" for row in counter_rows]
        hypothesis = self.hypotheses.generate(opportunity,evidence_summary=opportunity.evidence_summary or {},
            contradicting_refs=counter_refs,
            search_scope=f"accepted outcomes for goal {goal_id} and capability {capability}",
            contradiction_search_completed=True)
        self.hypotheses.persist(self.db,hypothesis)
        settle_stage(hypothesis_reservation,{"hypotheses":1})

        # Bind all authority and evaluation inputs before running the deterministic evaluator.
        evolution = self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s", (scope_id,)).fetchone()
        suite = self.db.execute("""SELECT eval_suite_id,version,integrity_hash,metadata FROM evolution.eval_suite_versions
            WHERE scope_id=%s AND status='AUTHORITATIVE'""", (scope_id,)).fetchone()
        if not evolution or not suite:
            raise ValueError("scope requires an active champion and authoritative EvalSuiteVersion")
        suite, suite_definition = self.resolver.resolve(suite["eval_suite_id"],suite["version"])
        settle_stage(challenger_reservation,{"challengers":1})
        evaluation_reservation=reserve_stage("challenger_evaluation",{"evaluations":1,"attempts":1,
            "wall_time":float(limits.get("max_wall_time_seconds",900))},budget_class)
        suite_hash = hashlib.sha256(canonical_json(suite_definition)).hexdigest()
        champion_id = evolution["genome_id"]
        candidate_id, observation_id, mutation_id = _id("genome"), _id("obs"), _id("mutation")
        candidate_hash = _hash(candidate_config)
        observation_payload = {"evidence_refs":list(opportunity.evidence_refs),"opportunity_id":opportunity.opportunity_id}
        with self.db.transaction():
            self.db.execute("UPDATE runtime.improvement_campaigns SET status='EVALUATING' WHERE campaign_id=%s", (campaign_id,))
            self.db.execute("INSERT INTO evolution.observations(observation_id,scope_id,source_refs,summary,created_by,metadata) VALUES (%s,%s,%s,%s,%s,%s)",
                (observation_id,scope_id,_jsonb(list(opportunity.evidence_refs)),opportunity.description,"opportunity-scout",_jsonb(observation_payload)))
            self.db.execute("""INSERT INTO evolution.mutation_proposals
                (mutation_id,scope_id,observation_refs,parent_genome_id,candidate_config_ref,candidate_config_hash,hypothesis,expected_effect,created_by,status,proposal_fingerprint,evidence_fingerprint)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'autonomous-campaign','REGISTERED',%s,%s)""",
                (mutation_id,scope_id,_jsonb([observation_id]),champion_id,
                 f"config-sha256:{candidate_hash}",candidate_hash,hypothesis.statement,
                 _jsonb(dict(hypothesis.expected_effect)),proposal_fingerprint,evidence_fingerprint))
            self.db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,mutation_rationale,created_by,metadata)
                VALUES (%s,%s,%s,'CHALLENGER',%s,%s,%s,%s,'autonomous-campaign',%s)""",
                (candidate_id,scope_id,_id("v"),f"config-sha256:{candidate_hash}",candidate_hash,
                 f"Change route for {capability}",hypothesis.statement,_jsonb({"mutation_id":mutation_id,"tier":"E1"})))
            self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",(candidate_id,champion_id))
            frozen = {"suite_id":suite["eval_suite_id"],"suite_version":suite["version"],
                "suite_hash":suite_hash,"scope_id":scope_id,"candidate_genome_id":candidate_id,
                "champion_genome_id":champion_id,"candidate_config":candidate_config,
                "champion_config":dict(champion_config),"code_revision":code_revision,
                "suite_definition_ref":suite["definition_ref"],
                "task_input_refs":list(opportunity.evidence_refs),"conditions":{"matched":True}}
            pack_id = _id("epack"); pack_hash = _hash(frozen)
            self.db.execute("""INSERT INTO evolution.evaluation_packs
                (pack_id,scope_id,candidate_genome_id,champion_genome_id,eval_suite_id,eval_suite_version,pack_hash,definition,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'autonomous-campaign')""",
                (pack_id,scope_id,candidate_id,champion_id,suite["eval_suite_id"],suite["version"],pack_hash,_jsonb(frozen)))

        # The challenger and frozen pack are durable before the evaluator role sees them.
        # The evaluator has no mutation or promotion authority.
        import time
        evaluation_started=time.monotonic()
        champion_result = evaluate_routing_genome(champion_config,suite_definition,expected_suite_hash=suite_hash)
        candidate_result = evaluate_routing_genome(candidate_config,suite_definition,expected_suite_hash=suite_hash)
        evaluation_id = _id("eval")
        directions = (suite["metadata"] or {}).get("metric_directions", {})
        wins = pareto_dominates(candidate_result.metrics,champion_result.metrics,directions)
        decision = "WOULD_PROMOTE" if wins else "REJECT"
        with self.evaluator_db.transaction():
            self.evaluator_db.execute("""INSERT INTO evolution.evaluation_records
                (evaluation_id,scope_id,candidate_genome_id,champion_genome_id,eval_suite_id,eval_suite_version,
                 conditions_ref,evaluator_version,evaluator_id,result_refs,metrics,status,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'deterministic-evaluator-v1',%s,%s,%s,'COMPLETED',now())""",
                    (evaluation_id,scope_id,candidate_id,champion_id,suite["eval_suite_id"],suite["version"],
                     f"evaluation-pack:{pack_id}:{pack_hash}",evaluator_id,
                     _jsonb([champion_result.result_ref,candidate_result.result_ref]),
                     _jsonb({"champion":dict(champion_result.metrics),"candidate":dict(candidate_result.metrics),"conditions_equivalent":True})))
            EvaluatorWorker().record_run(self.evaluator_db,evaluator_run_id=_id("erun"),evaluation_id=evaluation_id,
                                         execution_ref=f"evaluation-pack:{pack_id}")
        settle_stage(evaluation_reservation,{"evaluations":1,"attempts":1,
            "wall_time":time.monotonic()-evaluation_started})
        with self.db.transaction():
            self.db.execute("SELECT evolution.record_challenger_outcome(%s,%s)",
                            (candidate_id,"CHALLENGER" if wins else "REJECTED"))
            decision_id=_id("pdec")
            self.db.execute("""INSERT INTO evolution.promotion_decisions
                (decision_id,scope_id,candidate_genome_id,champion_genome_id,evaluation_refs,decision,mode,rationale_ref,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,'SHADOW',%s,'autonomous-campaign')""",
                (decision_id,scope_id,candidate_id,champion_id,_jsonb([evaluation_id]),decision,
                 f"evaluation-pack:{pack_id}:{pack_hash}"))
            if not wins:
                learning={"statement":"The tested alternative did not improve the governed metrics.",
                          "candidate_metrics":dict(candidate_result.metrics),"champion_metrics":dict(champion_result.metrics),
                          "evaluation_id":evaluation_id,"mutation_id":mutation_id}
                knowledge_id=_id("know")
                self.db.execute("""INSERT INTO runtime.knowledge_objects
                    (knowledge_id,kind,structured_payload,created_by,status,evidence_quality,reproducibility,review_status,independence)
                    VALUES (%s,'EVIDENCE',%s,'evaluator-worker','ACTIVE','HIGH','REPRODUCIBLE','INDEPENDENTLY_REVIEWED','INDEPENDENT')""",
                    (knowledge_id,_jsonb(learning)))
            payload={"campaign_id":campaign_id,"opportunity_id":opportunity.opportunity_id,
                     "hypothesis_id":hypothesis.hypothesis_id,"mutation_id":mutation_id,
                     "candidate_genome_id":candidate_id,"evaluation_pack_id":pack_id,
                     "evaluation_id":evaluation_id,"decision":decision,"mode":"SHADOW"}
            self.db.execute("INSERT INTO evolution.evolution_events(event_id,event_type,scope_id,actor_id,payload,payload_hash) VALUES (%s,'SHADOW_DECISION_CREATED',%s,'autonomous-campaign',%s,%s)",
                (_id("eevt"),scope_id,_jsonb(payload),_hash(payload)))
            self.db.execute("UPDATE runtime.improvement_campaigns SET status='COMPLETED',ended_at=now() WHERE campaign_id=%s",(campaign_id,))
        ImprovementCampaignService(self.db).settle(campaign_id=campaign_id,budget_class=budget_class,
            idempotency_key=f"{campaign_id}:{opportunity.fingerprint}:{alternative}",
            usage={})
        return {"opportunity_id":opportunity.opportunity_id,"hypothesis_id":hypothesis.hypothesis_id,
            "mutation_id":mutation_id,"candidate_genome_id":candidate_id,"evaluation_pack_id":pack_id,
            "evaluation_id":evaluation_id,"decision_id":decision_id,"decision":decision,
            "champion_genome_id":champion_id,"candidate_metrics":dict(candidate_result.metrics),
            "budget_class":budget_class,"material_difference":"routing executor changed for one measured capability",
            "champion_metrics":dict(champion_result.metrics),"mode":"SHADOW"}


class ChallengerGenerator:
    def __init__(self, db: Any) -> None:
        self.policy = AutonomousMutationPolicy(db)

    def minimal_candidate(self, *, tier: str, config: Mapping[str, Any], changes: Mapping[str, Any]) -> dict[str, Any]:
        self.policy.validate_patch(tier=tier, changes=changes)
        candidate = json.loads(json.dumps(config))
        for path, value in changes.items():
            parts = path.split(".")
            target = candidate
            for part in parts[:-1]:
                target = target.setdefault(part, {})
                if not isinstance(target, dict):
                    raise ValueError("mutation path collides with a non-object configuration value")
            target[parts[-1]] = value
        return candidate


class EvaluationPackService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def freeze(self, *, scope_id: str, candidate_id: str, champion_id: str,
               suite_id: str, suite_version: str, definition: Mapping[str, Any],
               created_by: str) -> tuple[str, str]:
        pack_id = _id("epack")
        frozen = {"scope_id": scope_id, "candidate_genome_id": candidate_id,
                  "champion_genome_id": champion_id, "eval_suite_id": suite_id,
                  "eval_suite_version": suite_version, "definition": dict(definition)}
        digest = _hash(frozen)
        with self.db.transaction():
            current = self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s FOR SHARE", (scope_id,)).fetchone()
            suite = self.db.execute("SELECT status,integrity_hash FROM evolution.eval_suite_versions WHERE eval_suite_id=%s AND version=%s AND scope_id=%s FOR SHARE",
                                    (suite_id, suite_version, scope_id)).fetchone()
            if not current or current["genome_id"] != champion_id or not suite or suite["status"] != "AUTHORITATIVE":
                raise ValueError("champion or authoritative evaluation suite changed before freeze")
            self.db.execute("""INSERT INTO evolution.evaluation_packs
                (pack_id,scope_id,candidate_genome_id,champion_genome_id,eval_suite_id,eval_suite_version,pack_hash,definition,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (pack_id,scope_id,candidate_id,champion_id,suite_id,suite_version,digest,_jsonb(frozen),created_by))
        return pack_id, digest


class HumanPromotionService:
    """CAS promotion path; never called from autonomous campaign execution."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def authorize(self, *, authorization_id: str, scope_id: str, expected_champion_id: str,
                  challenger_id: str, shadow_decision_id: str, evaluation_refs: Sequence[str],
                  authorized_by: str, action: str = "PROMOTE") -> None:
        if not authorized_by or action not in {"PROMOTE", "ROLLBACK"}:
            raise ValueError("explicit governance authority and valid action are required")
        with self.db.transaction():
            decision = self.db.execute("SELECT decision,scope_id,candidate_genome_id,champion_genome_id FROM evolution.promotion_decisions WHERE decision_id=%s",
                                      (shadow_decision_id,)).fetchone()
            valid_promotion = (decision and decision["candidate_genome_id"] == challenger_id
                               and decision["champion_genome_id"] == expected_champion_id)
            valid_rollback = (action == "ROLLBACK" and decision
                              and decision["candidate_genome_id"] == expected_champion_id
                              and decision["champion_genome_id"] == challenger_id)
            if (not decision or decision["scope_id"] != scope_id or decision["decision"] != "WOULD_PROMOTE"
                    or not (valid_promotion or valid_rollback)):
                raise ValueError("authorization must reference a matching positive shadow decision")
            active = self.db.execute("SELECT genome_id FROM evolution.scope_champions WHERE scope_id=%s",(scope_id,)).fetchone()
            if not active or active["genome_id"] != expected_champion_id:
                raise ValueError("authorization expected champion is already stale")
            decision_row = self.db.execute("SELECT created_by,evaluation_refs FROM evolution.promotion_decisions WHERE decision_id=%s",(shadow_decision_id,)).fetchone()
            if set(evaluation_refs) != set(decision_row["evaluation_refs"] or []):
                raise ValueError("authorization must cite the complete shadow evaluation set")
            evaluation_ids = list(decision_row["evaluation_refs"] or [])
            evaluator_rows = self.db.execute("SELECT evaluator_id FROM evolution.evaluation_records WHERE evaluation_id=ANY(%s)",(evaluation_ids,)).fetchall()
            proposer = self.db.execute("SELECT created_by FROM evolution.system_genomes WHERE genome_id=%s",(decision["candidate_genome_id"],)).fetchone()
            forbidden_authorities = {decision_row["created_by"],proposer["created_by"] if proposer else None}
            forbidden_authorities.update(row["evaluator_id"] for row in evaluator_rows)
            if authorized_by in forbidden_authorities:
                raise ValueError("promotion authority must be distinct from proposer, evaluator, and shadow decision creator")
            self.db.execute("SELECT evolution.authorize_promotion(%s,%s,%s,%s,%s,%s,%s,%s)",
                (authorization_id,scope_id,expected_champion_id,challenger_id,shadow_decision_id,
                 _jsonb(list(evaluation_refs)),authorized_by,action))

    def execute(self, authorization_id: str) -> str:
        with self.db.transaction():
            result = self.db.execute("SELECT evolution.execute_authorized_promotion(%s) AS result",
                                     (authorization_id,)).fetchone()["result"]
        if result == "STALE":
            raise ValueError("stale authorization: champion changed")
        return result

    def authorize_rollback(self, *, authorization_id: str, scope_id: str, expected_champion_id: str,
                           rollback_target_id: str, shadow_decision_id: str,
                           authorized_by: str) -> None:
        # Rollback target must be the exact prior champion recorded on current genome.
        row = self.db.execute("SELECT rollback_ref FROM evolution.system_genomes WHERE genome_id=%s", (expected_champion_id,)).fetchone()
        if not row or row["rollback_ref"] != rollback_target_id:
            raise ValueError("rollback target is not the recorded prior champion")
        decision = self.db.execute("SELECT evaluation_refs FROM evolution.promotion_decisions WHERE decision_id=%s",(shadow_decision_id,)).fetchone()
        if not decision:
            raise ValueError("rollback requires existing shadow decision evidence")
        self.authorize(authorization_id=authorization_id,scope_id=scope_id,
            expected_champion_id=expected_champion_id,challenger_id=rollback_target_id,
            shadow_decision_id=shadow_decision_id,evaluation_refs=list(decision["evaluation_refs"] or []),
            authorized_by=authorized_by,action="ROLLBACK")
