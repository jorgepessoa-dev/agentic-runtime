"""Durable plan versioning and deterministic task-graph materialization."""
from __future__ import annotations

import json
import uuid
from typing import Any

from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.contracts.coordination import (
    PlanBounds, PlanNode, PlanVersionProposal, canonical_plan_hash,
    validate_plan_version,
)
from agentic_runtime.contracts.serialization import canonical_hash as _canonical_hash
from agentic_runtime.persistence.events import jsonb as _jsonb


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _proposal_dict(proposal: PlanVersionProposal) -> dict[str, Any]:
    return {
        "plan_id": proposal.plan_id, "goal_id": proposal.goal_id,
        "created_by": proposal.created_by, "parent_version": proposal.parent_version,
        "nodes": [{
            "node_key": node.node_key, "task_type": node.task_type,
            "objective": node.objective,
            "required_capabilities": list(node.required_capabilities),
            "input_refs": list(node.input_refs), "output_contract": dict(node.output_contract),
            "acceptance_criteria": dict(node.acceptance_criteria),
            "verifier_capabilities": list(node.verifier_capabilities),
            "depends_on": list(node.depends_on),
            "dependency_requirements": dict(node.dependency_requirements),
            "budget": dict(node.budget), "parent_node_key":node.parent_node_key,
        } for node in proposal.nodes],
        "estimated_budget": dict(proposal.estimated_budget),
        "max_depth": proposal.max_depth,
        "max_children_per_node": proposal.max_children_per_node,
        "max_descendants": proposal.max_descendants,
        "max_concurrent_descendants": proposal.max_concurrent_descendants,
        "max_retries_per_node": proposal.max_retries_per_node,
        "max_wall_time_seconds": proposal.max_wall_time_seconds,
        "max_artifact_bytes": proposal.max_artifact_bytes,
    }


class CoordinationService:
    """Coordinator-only operations; planners and workers have no DB access."""

    EDGE_TYPES = {"ACCEPTED": "REQUIRES_ACCEPTED",
                  "ARTIFACT": "REQUIRES_ARTIFACT", "ORDER_ONLY": "ORDER_ONLY"}

    def __init__(self, connection: Any, *, bounds: PlanBounds = PlanBounds()) -> None:
        self.db = connection
        self.bounds = bounds

    def _event(self, *, event_type: str, goal_id: str, plan_version_id: str,
               actor_id: str, payload: dict[str, Any], task_id: str | None = None) -> str:
        event_id = _id("evt")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        import hashlib
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        self.db.execute("""INSERT INTO runtime.events
            (event_id,event_type,goal_id,task_id,actor_type,actor_id,correlation_id,schema_version,payload,payload_hash)
            VALUES (%s,%s,%s,%s,'RUNTIME',%s,%s,'1',%s,%s)""",
            (event_id,event_type,goal_id,task_id,actor_id,plan_version_id,_jsonb(payload),digest))
        return event_id

    def propose(self, plan_version_id: str, proposal: PlanVersionProposal) -> str:
        validate_plan_version(proposal, self.bounds)
        plan = _proposal_dict(proposal)
        digest = canonical_plan_hash(proposal)
        limits = {key: plan[key] for key in (
            "max_depth", "max_children_per_node", "max_descendants",
            "max_concurrent_descendants", "max_retries_per_node",
            "max_wall_time_seconds", "max_artifact_bytes")}
        conflicting_proposal = False
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"m9-plan:{proposal.plan_id}",))
            goal = self.db.execute("""SELECT g.status,c.status AS campaign_status
                FROM runtime.goals g JOIN runtime.campaigns c USING(campaign_id)
                WHERE g.goal_id=%s FOR UPDATE OF g,c""", (proposal.goal_id,)).fetchone()
            if not goal or goal["status"] != "ACTIVE" or goal["campaign_status"] != "ACTIVE":
                raise ValueError("plan proposals require an active goal and campaign")
            parent_id = None
            version = 1
            if proposal.parent_version is not None:
                parent = self.db.execute("""SELECT plan_version_id,status FROM runtime.coordination_plan_versions
                    WHERE plan_id=%s AND version=%s FOR UPDATE""",
                    (proposal.plan_id, proposal.parent_version)).fetchone()
                if not parent or parent["status"] not in {"ACCEPTED", "REJECTED"}:
                    raise ValueError("replan parent must be a terminal persisted version")
                parent_id, version = parent["plan_version_id"], proposal.parent_version + 1
            if version>self.bounds.max_plan_versions:
                raise ValueError("plan version count exceeds the mission limit")
            prior = self.db.execute("""SELECT plan_version_id,canonical_sha256,goal_id,proposer
                FROM runtime.coordination_plan_versions WHERE plan_id=%s AND version=%s""",
                (proposal.plan_id,version)).fetchone()
            if prior:
                if prior["canonical_sha256"]==digest and prior["goal_id"]==proposal.goal_id \
                        and prior["proposer"]==proposal.created_by:
                    return prior["plan_version_id"]
                self._event(event_type="PLAN_REPLAN_PROPOSAL_REJECTED",goal_id=proposal.goal_id,
                    plan_version_id=prior["plan_version_id"],actor_id=proposal.created_by,
                    payload={"plan_id":proposal.plan_id,"version":version,
                        "conflicting_proposal_id":plan_version_id,"sha256":digest,
                        "proposal":plan,"disposition":"STALE_CONFLICT",
                        "reason":"successor version identity already claimed by a different proposal"})
                conflicting_proposal = True
            if not prior:
                self.db.execute("""INSERT INTO runtime.coordination_plan_versions
                (plan_version_id,plan_id,goal_id,version,parent_plan_version_id,status,proposer,
                 canonical_sha256,proposal,limits)
                VALUES (%s,%s,%s,%s,%s,'PROPOSED',%s,%s,%s,%s)""",
                (plan_version_id, proposal.plan_id, proposal.goal_id, version, parent_id,
                 proposal.created_by, digest, _jsonb(plan), _jsonb(limits)))
                for node in proposal.nodes:
                    self.db.execute("""INSERT INTO runtime.coordination_plan_nodes
                    (plan_version_id,node_key,parent_node_key,task_type,objective,required_capabilities,input_refs,
                     output_contract,acceptance_criteria,verifier_capabilities,budget)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (plan_version_id,node.node_key,node.parent_node_key,node.task_type,node.objective,
                     _jsonb(list(node.required_capabilities)),_jsonb(list(node.input_refs)),
                     _jsonb(dict(node.output_contract)),_jsonb(dict(node.acceptance_criteria)),
                     _jsonb(list(node.verifier_capabilities)),_jsonb(dict(node.budget))))
                for node in proposal.nodes:
                    for dependency in node.depends_on:
                        self.db.execute("""INSERT INTO runtime.coordination_plan_edges
                        (plan_version_id,predecessor_key,successor_key,requirement)
                        VALUES (%s,%s,%s,%s)""",
                        (plan_version_id,dependency,node.node_key,
                         node.dependency_requirements.get(dependency,"ACCEPTED")))
                self._event(event_type="PLAN_VERSION_PROPOSED",goal_id=proposal.goal_id,
                            plan_version_id=plan_version_id,actor_id=proposal.created_by,
                            payload={"plan_id":proposal.plan_id,"version":version,
                                     "sha256":digest,"node_count":len(proposal.nodes)})
        if conflicting_proposal:
            raise ValueError("plan version identity was reused with different content; losing proposal preserved")
        return plan_version_id

    def accept(self, plan_version_id: str, *, accepted_by: str) -> dict[str, str]:
        if not accepted_by:
            raise ValueError("accepting authority is required")
        with self.db.transaction():
            row = self.db.execute("""SELECT p.*,g.campaign_id,g.status AS goal_status,
                c.status AS campaign_status,c.budget AS campaign_budget,c.max_children AS campaign_max_children
                FROM runtime.coordination_plan_versions p
                JOIN runtime.goals g USING(goal_id) JOIN runtime.campaigns c USING(campaign_id)
                WHERE p.plan_version_id=%s FOR UPDATE OF p,g,c""", (plan_version_id,)).fetchone()
            if row and row["status"] == "ACCEPTED" and row["accepted_by"] == accepted_by:
                return {r["node_key"]:r["task_id"] for r in self.db.execute("""SELECT node_key,task_id
                    FROM runtime.coordination_plan_nodes WHERE plan_version_id=%s ORDER BY node_key""",
                    (plan_version_id,)).fetchall()}
            if not row or row["status"] != "PROPOSED":
                raise ValueError("only a proposed plan version can be accepted")
            if row["proposer"]==accepted_by:
                raise ValueError("planner cannot accept its own plan")
            if row["goal_status"] != "ACTIVE" or row["campaign_status"] != "ACTIVE":
                raise ValueError("plan goal and campaign must remain active")
            if row["parent_plan_version_id"]:
                outstanding=self.db.execute("""SELECT count(*) AS n FROM runtime.tasks
                    WHERE plan_version_id=%s AND status IN
                      ('LEASED','CONTEXT_VALIDATED','RUNNING','RESULT_COMMITTED','VERIFIED','WAITING_CHILDREN','WAITING_TOOL',
                       'WAITING_IO','BLOCKED','CHECKPOINTED')""",
                    (row["parent_plan_version_id"],)).fetchone()["n"]
                if outstanding:
                    raise ValueError("successor plan cannot activate while prior-version work is active")
                # A successor proposal replaces dispatch for the old immutable
                # version. Retire unstarted/retryable nodes in the same commit
                # so stale outbox hints cannot leave orphan queued work behind.
                superseded=self.db.execute("""SELECT task_id,goal_id,status FROM runtime.tasks
                    WHERE plan_version_id=%s AND status IN ('QUEUED','RETRY_PENDING')
                    ORDER BY created_at,task_id FOR UPDATE""",
                    (row["parent_plan_version_id"],)).fetchall()
                for task in superseded:
                    self.db.execute("UPDATE runtime.tasks SET status='CANCELLED',updated_at=now() WHERE task_id=%s",
                                    (task["task_id"],))
                    self._event(event_type="TASK_CANCELLED",goal_id=task["goal_id"],
                        plan_version_id=row["parent_plan_version_id"],actor_id=accepted_by,
                        task_id=task["task_id"],payload={"reason":"superseded by accepted plan version",
                            "successor_plan_version_id":plan_version_id})
                    CampaignAccounting(self.db).finish_task_reservation(task_id=task["task_id"],
                        attempt_id=None,terminal_status="CANCELLED")
                    delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                        SET status='CANCELLED',updated_at=now() WHERE child_task_id=%s
                          AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED')
                        RETURNING delegation_id""",(task["task_id"],)).fetchone()
                    if delegation:
                        self._event(event_type="DELEGATION_CANCELLED",goal_id=task["goal_id"],
                            plan_version_id=row["parent_plan_version_id"],actor_id=accepted_by,
                            task_id=task["task_id"],payload={"delegation_id":delegation["delegation_id"],
                                "reason":"successor plan accepted"})
            plan = row["proposal"]
            nodes = tuple(PlanNode(
                node_key=n["node_key"], task_type=n["task_type"], objective=n["objective"],
                required_capabilities=tuple(n["required_capabilities"]),
                input_refs=tuple(n["input_refs"]), output_contract=n["output_contract"],
                acceptance_criteria=n["acceptance_criteria"],
                verifier_capabilities=tuple(n["verifier_capabilities"]),
                depends_on=tuple(n["depends_on"]),
                dependency_requirements=n.get("dependency_requirements", {}),
                budget=n["budget"],parent_node_key=n.get("parent_node_key"))
                for n in plan["nodes"])
            proposal = PlanVersionProposal(
                plan_id=plan["plan_id"],goal_id=plan["goal_id"],created_by=plan["created_by"],
                nodes=nodes,estimated_budget=plan["estimated_budget"],
                parent_version=plan["parent_version"],max_depth=plan["max_depth"],
                max_children_per_node=plan["max_children_per_node"],
                max_descendants=plan["max_descendants"],
                max_concurrent_descendants=plan["max_concurrent_descendants"],
                max_retries_per_node=plan["max_retries_per_node"],
                max_wall_time_seconds=plan["max_wall_time_seconds"],
                max_artifact_bytes=plan["max_artifact_bytes"])
            validate_plan_version(proposal, self.bounds)
            if canonical_plan_hash(proposal) != row["canonical_sha256"]:
                raise ValueError("persisted plan canonical hash mismatch")
            accepted_versions=self.db.execute("""SELECT proposal FROM runtime.coordination_plan_versions
                WHERE plan_id=%s AND status='ACCEPTED'""",(proposal.plan_id,)).fetchall()
            existing_node_count=self.db.execute("""SELECT count(*) AS n FROM runtime.tasks t
                JOIN runtime.coordination_plan_versions v
                  ON v.plan_version_id=t.plan_version_id
                WHERE v.plan_id=%s""",(proposal.plan_id,)).fetchone()["n"]
            if existing_node_count+len(proposal.nodes)>self.bounds.max_nodes*self.bounds.max_plan_versions:
                raise ValueError("cumulative plan versions exceed the mission node limit")
            prior_descendants=self.db.execute("""SELECT count(*) AS n FROM runtime.tasks
                WHERE campaign_id=%s AND parent_task_id IS NOT NULL""",(row["campaign_id"],)).fetchone()["n"]
            new_descendants=sum(node.parent_node_key is not None for node in proposal.nodes)
            if prior_descendants+new_descendants>min(row["campaign_max_children"],self.bounds.max_descendants):
                raise ValueError("cumulative plan descendants exceed campaign or mission limit")
            campaign_budget=row["campaign_budget"] or {}
            budget_aliases={"calls":"max_cognitive_calls","wall_time":"max_wall_time_seconds",
                "tokens_input":"max_tokens_input","tokens_output":"max_tokens_output",
                "monetary_cost":"max_cost"}
            aggregate_budget={}
            for version_row in accepted_versions:
                for dimension,amount in (version_row["proposal"].get("estimated_budget") or {}).items():
                    aggregate_budget[dimension]=aggregate_budget.get(dimension,0.0)+float(amount)
            for dimension, amount in proposal.estimated_budget.items():
                aggregate_budget[dimension]=aggregate_budget.get(dimension,0.0)+float(amount)
            for dimension, amount in aggregate_budget.items():
                cap_key=budget_aliases.get(dimension,dimension)
                if cap_key not in campaign_budget or float(amount)>float(campaign_budget[cap_key]):
                    raise ValueError(f"cumulative plan budget for {dimension} is not bounded by campaign policy")
            node_ids = {node.node_key: _id("task") for node in proposal.nodes}
            node_by_key={node.node_key:node for node in proposal.nodes}
            depths: dict[str,int]={}
            def depth(key: str) -> int:
                if key not in depths:
                    parent=node_by_key[key].parent_node_key
                    depths[key]=0 if parent is None else depth(parent)+1
                return depths[key]
            for node in proposal.nodes:
                task_id = node_ids[node.node_key]
                budget = dict(node.budget)
                budget["max_attempts"] = int(budget.get("max_attempts", proposal.max_retries_per_node + 1))
                metadata = {"node_key":node.node_key,
                            "objective":node.objective,"acceptance_criteria":dict(node.acceptance_criteria),
                            "verifier_capabilities":list(node.verifier_capabilities),
                            "requires_independent_verification":bool(node.verifier_capabilities or
                                node.acceptance_criteria.get("requires_independent_verification",False)),
                            "max_artifact_bytes":proposal.max_artifact_bytes}
                if node.task_type=="generic_agent_task" and "worker_sleep_seconds" in budget:
                    metadata["worker_sleep_seconds"]=float(budget["worker_sleep_seconds"])
                request = {"campaign_id":row["campaign_id"],"goal_id":proposal.goal_id,
                           "task_type":node.task_type,"input_refs":list(node.input_refs),
                           "output_contract":dict(node.output_contract),"budget":budget,
                           "required_capabilities":list(node.required_capabilities),"metadata":metadata}
                request_hash = _canonical_hash(request)
                task_budget=dict(budget)
                task_max_attempts=task_budget.pop("max_attempts",None)
                self.db.execute("""INSERT INTO runtime.tasks
                    (task_id,campaign_id,goal_id,plan_version_id,parent_task_id,task_type,status,
                     required_capabilities,input_refs,output_contract,budget,max_attempts,max_children,
                     depth_remaining,idempotency_key,request_hash,metadata)
                    VALUES (%s,%s,%s,%s,%s,%s,'QUEUED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (task_id,row["campaign_id"],proposal.goal_id,plan_version_id,
                     node_ids[node.parent_node_key] if node.parent_node_key else None,node.task_type,
                     _jsonb(list(node.required_capabilities)),_jsonb(list(node.input_refs)),
                     _jsonb(dict(node.output_contract)),_jsonb(task_budget),task_max_attempts,
                     proposal.max_children_per_node,max(0,proposal.max_depth-depth(node.node_key)),
                     f"m9:{plan_version_id}:{node.node_key}",request_hash,_jsonb(metadata)))
                self.db.execute("""UPDATE runtime.coordination_plan_nodes SET task_id=%s
                    WHERE plan_version_id=%s AND node_key=%s""",
                    (task_id,plan_version_id,node.node_key))
                event_id=self._event(event_type="TASK_QUEUED",goal_id=proposal.goal_id,
                    plan_version_id=plan_version_id,actor_id=accepted_by,
                    payload={"task_id":task_id,"node_key":node.node_key,
                             "required_capabilities":list(node.required_capabilities),
                             "plan_version_id":plan_version_id},task_id=task_id)
                self.db.execute("""INSERT INTO runtime.outbox
                    (outbox_id,event_id,topic,idempotency_key,payload)
                    VALUES (%s,%s,'task.dispatch',%s,%s) ON CONFLICT(idempotency_key) DO NOTHING""",
                    (_id("out"),event_id,f"dispatch:{task_id}",_jsonb({"task_id":task_id})))
            for node in proposal.nodes:
                for predecessor in node.depends_on:
                    self.db.execute("""INSERT INTO runtime.task_dependencies
                        (task_id,depends_on_task_id,dependency_type)
                        VALUES (%s,%s,%s)""",
                        (node_ids[node.node_key],node_ids[predecessor],
                         {"ACCEPTED":"REQUIRES_ACCEPTED", "ARTIFACT":"REQUIRES_ARTIFACT",
                          "ORDER_ONLY":"ORDER_ONLY"}[node.dependency_requirements.get(predecessor,"ACCEPTED")]))
            for node in proposal.nodes:
                if node.parent_node_key is None:
                    continue
                delegation_id=_id("deleg")
                delegation_request={"plan_version_id":plan_version_id,"parent_node_key":node.parent_node_key,
                    "child_node_key":node.node_key,"capabilities":list(node.required_capabilities),
                    "input_refs":list(node.input_refs),"budget":dict(node.budget)}
                request_hash=_canonical_hash(delegation_request)
                self.db.execute("""INSERT INTO runtime.coordination_delegations
                    (delegation_id,idempotency_key,request_sha256,plan_version_id,delegator,
                     parent_task_id,child_task_id,depth,capability_contract,input_refs,output_contract,
                     acceptance_criteria,budget,deadline,status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        now()+(%s * interval '1 second'),'PLAN_ACCEPTED')""",
                    (delegation_id,f"{plan_version_id}:{node.node_key}",request_hash,plan_version_id,
                     accepted_by,node_ids[node.parent_node_key],node_ids[node.node_key],depth(node.node_key),
                     _jsonb(list(node.required_capabilities)),_jsonb(list(node.input_refs)),
                     _jsonb(dict(node.output_contract)),_jsonb(dict(node.acceptance_criteria)),
                     _jsonb(dict(node.budget)),proposal.max_wall_time_seconds))
            self.db.execute("""UPDATE runtime.coordination_plan_versions
                SET status='ACCEPTED',accepted_by=%s,accepted_at=now()
                WHERE plan_version_id=%s""",(accepted_by,plan_version_id))
            self._event(event_type="PLAN_VERSION_ACCEPTED",goal_id=proposal.goal_id,
                plan_version_id=plan_version_id,actor_id=accepted_by,
                payload={"sha256":row["canonical_sha256"],"tasks":node_ids})
            return node_ids

    def record_independent_verification(self, *, verification_id: str, task_id: str,
            attempt_id: str, verifier_task_id: str, verifier_ref: str,
            outcome: str, result_hash: str, evidence_refs: list[str]) -> str:
        """Record an independent child-task review of a delivered result."""
        if outcome not in {"ACCEPTED", "REJECTED", "REPAIR_REQUIRED", "ESCALATED"}:
            raise ValueError("unknown verification outcome")
        if task_id == verifier_task_id:
            raise ValueError("a task cannot verify itself")
        with self.db.transaction():
            existing=self.db.execute("""SELECT task_id,attempt_id,verifier_ref,verifier_task_id,
                result_hash,status,evidence_refs FROM runtime.task_verifications WHERE verification_id=%s""",
                (verification_id,)).fetchone()
            if existing:
                expected={"task_id":task_id,"attempt_id":attempt_id,"verifier_ref":verifier_ref,
                    "verifier_task_id":verifier_task_id,"result_hash":result_hash,"status":outcome,
                    "evidence_refs":evidence_refs}
                if dict(existing)!=expected:
                    raise ValueError("verification identity reused with different content")
                return verification_id
            producer = self.db.execute("""SELECT t.status,t.result_hash,t.metadata,t.plan_version_id,t.goal_id,
                t.lease_epoch,a.lease_epoch AS attempt_epoch,a.status AS attempt_status,
                a.worker_id AS producer_worker FROM runtime.tasks t
                JOIN runtime.attempts a ON a.attempt_id=%s AND a.task_id=t.task_id
                WHERE t.task_id=%s FOR UPDATE OF t,a""", (attempt_id,task_id)).fetchone()
            reviewer = self.db.execute("""SELECT t.status,t.required_capabilities,t.result_refs,t.metadata,t.plan_version_id,t.goal_id,
                a.worker_id AS reviewer_worker FROM runtime.tasks t
                JOIN runtime.attempts a ON a.task_id=t.task_id AND a.status='ACCEPTED'
                WHERE t.task_id=%s ORDER BY a.completed_at DESC LIMIT 1 FOR UPDATE OF t""",
                (verifier_task_id,)).fetchone()
            if not producer or producer["status"] != "RESULT_COMMITTED" \
                    or producer["result_hash"] != result_hash or producer["attempt_status"]!="RESULT_COMMITTED" \
                    or producer["attempt_epoch"]!=producer["lease_epoch"]:
                raise ValueError("verification must bind to a current delivered result hash")
            if not producer["metadata"].get("requires_independent_verification"):
                raise ValueError("task contract does not require independent verification")
            if not reviewer or reviewer["status"] != "ACCEPTED":
                raise ValueError("verifier task must be independently accepted first")
            plan_version=producer["plan_version_id"]
            if not plan_version or reviewer["plan_version_id"]!=plan_version \
                    or reviewer["goal_id"]!=producer["goal_id"]:
                raise ValueError("verifier must belong to the producer's current plan version")
            current=self.db.execute("""SELECT p.plan_version_id FROM runtime.coordination_plan_versions p
                JOIN runtime.goals g USING(goal_id) WHERE p.plan_version_id=%s AND p.status='ACCEPTED'
                AND g.status='ACTIVE' AND p.version=(SELECT max(v.version)
                    FROM runtime.coordination_plan_versions v WHERE v.plan_id=p.plan_id AND v.status='ACCEPTED')
                FOR UPDATE OF p,g""",(plan_version,)).fetchone()
            if not current:
                raise ValueError("verifier must belong to the producer's current plan version")
            if producer["producer_worker"] == reviewer["reviewer_worker"]:
                raise ValueError("producer worker cannot verify its own result")
            required = set(producer["metadata"].get("verifier_capabilities", []))
            if not required.issubset(set(reviewer["required_capabilities"])):
                raise ValueError("reviewer task does not satisfy the verifier capability contract")
            if not evidence_refs or not set(evidence_refs).issubset(set(reviewer["result_refs"] or [])):
                raise ValueError("verification evidence must reference the accepted reviewer delivery")
            verified_refs=self.db.execute("""SELECT artifact_id,producer_task_id,verification_status
                FROM runtime.artifacts WHERE artifact_id=ANY(%s)""",(evidence_refs,)).fetchall()
            if len(verified_refs)!=len(set(evidence_refs)) or any(
                    a["producer_task_id"]!=verifier_task_id or a["verification_status"]!="VERIFIED"
                    for a in verified_refs):
                raise ValueError("verification evidence artifact lineage is not verified")
            self.db.execute("""INSERT INTO runtime.task_verifications
                (verification_id,task_id,attempt_id,verifier_ref,verifier_task_id,result_hash,
                 status,evidence_refs,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())""",
                (verification_id,task_id,attempt_id,verifier_ref,verifier_task_id,result_hash,
                 outcome,_jsonb(evidence_refs)))
        return verification_id

    def accept_goal(self, goal_id: str, *, final_task_id: str, artifact_id: str,
                    accepted_by: str) -> str:
        """Mark a governed goal achieved only after its current graph is accepted."""
        if not accepted_by:
            raise ValueError("accepting authority is required")
        with self.db.transaction():
            goal = self.db.execute("""SELECT status,artifact_ref FROM runtime.goals
                WHERE goal_id=%s FOR UPDATE""", (goal_id,)).fetchone()
            if goal and goal["status"] == "ACHIEVED":
                if goal["artifact_ref"] == artifact_id:
                    return artifact_id
                raise ValueError("achieved goal is immutable")
            if not goal or goal["status"] != "ACTIVE":
                raise ValueError("only an active goal can be accepted")
            current = self.db.execute("""SELECT p.plan_version_id,p.plan_id,p.version
                FROM runtime.coordination_plan_versions p
                JOIN runtime.coordination_plan_nodes n USING(plan_version_id)
                WHERE p.goal_id=%s AND p.status='ACCEPTED' AND n.task_id=%s
                  AND NOT EXISTS (SELECT 1 FROM runtime.coordination_plan_versions newer
                    WHERE newer.plan_id=p.plan_id AND newer.version>p.version
                      AND newer.status='ACCEPTED')
                FOR UPDATE OF p""", (goal_id,final_task_id)).fetchone()
            if not current:
                raise ValueError("goal has no current accepted plan")
            graph = self.db.execute("""SELECT count(*) AS total,
                    count(*) FILTER (WHERE t.status='ACCEPTED') AS accepted
                FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
                WHERE n.plan_version_id=%s""", (current["plan_version_id"],)).fetchone()
            if graph["total"] == 0 or graph["accepted"] != graph["total"]:
                raise ValueError("every node in the current plan must be accepted")
            nonterminal = self.db.execute("""SELECT count(*) AS n FROM runtime.tasks
                WHERE goal_id=%s AND status NOT IN ('ACCEPTED','REJECTED','NEEDS_REVIEW',
                    'FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED')""",
                (goal_id,)).fetchone()["n"]
            if nonterminal:
                raise ValueError("goal still has nonterminal task work")
            result = self.db.execute("""SELECT t.status,t.result_refs,a.verification_status,
                    a.producer_task_id,a.producer_attempt_id
                FROM runtime.tasks t JOIN runtime.artifacts a ON a.artifact_id=%s
                WHERE t.task_id=%s AND t.goal_id=%s""",
                (artifact_id,final_task_id,goal_id)).fetchone()
            if not result or result["status"] != "ACCEPTED" or result["verification_status"] != "VERIFIED" \
                    or result["producer_task_id"] != final_task_id \
                    or artifact_id not in (result["result_refs"] or []):
                raise ValueError("goal acceptance requires an artifact from its accepted final task")
            self.db.execute("UPDATE runtime.goals SET status='ACHIEVED',artifact_ref=%s WHERE goal_id=%s",
                (artifact_id,goal_id))
            self._event(event_type="GOAL_ACHIEVED",goal_id=goal_id,
                plan_version_id=current["plan_version_id"],actor_id=accepted_by,
                task_id=final_task_id,payload={"plan_id":current["plan_id"],
                    "plan_version":current["version"],"final_task_id":final_task_id,
                    "artifact_id":artifact_id})
        return artifact_id

    def request_plan_cancellation(self, plan_version_id: str, *, requested_by: str) -> dict[str, Any]:
        """Cancel queued plan work and durably signal current remote attempts.

        Active task state remains nonterminal until the authenticated worker
        acknowledges cancellation under the same lease fence. Missing or
        expired worker authority is reported for ordinary reconciliation.
        """
        if not requested_by:
            raise ValueError("cancellation authority is required")
        with self.db.transaction():
            plan=self.db.execute("""SELECT p.plan_id,p.version,p.status,p.goal_id,
                (SELECT max(latest.version) FROM runtime.coordination_plan_versions latest
                 WHERE latest.plan_id=p.plan_id AND latest.status='ACCEPTED') AS current_version
                FROM runtime.coordination_plan_versions p WHERE p.plan_version_id=%s FOR UPDATE""",
                (plan_version_id,)).fetchone()
            if not plan or plan["status"]!="ACCEPTED" or plan["version"]!=plan["current_version"]:
                raise ValueError("only the current accepted plan version can be cancelled")
            rows=self.db.execute("""SELECT t.task_id,t.status,t.goal_id,l.attempt_id,l.worker_id,
                    a.worker_instance_id,l.lease_epoch,l.lease_until,l.status AS lease_status,
                    wi.status AS instance_status,(l.lease_until>now()) AS lease_current
                FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
                LEFT JOIN runtime.leases l ON l.task_id=t.task_id
                LEFT JOIN runtime.attempts a ON a.attempt_id=l.attempt_id
                LEFT JOIN runtime.worker_instances wi ON wi.worker_instance_id=a.worker_instance_id
                WHERE n.plan_version_id=%s ORDER BY t.created_at,t.task_id FOR UPDATE OF t""",
                (plan_version_id,)).fetchall()
            cancelled=[]; commands=[]; unresolved=[]; terminal=[]
            for row in rows:
                if row["status"] in {"ACCEPTED","REJECTED","NEEDS_REVIEW","FAILED_TRANSIENT",
                        "FAILED_PERMANENT","CANCELLED","QUARANTINED","BUDGET_EXCEEDED"}:
                    terminal.append(row["task_id"])
                    continue
                lease_active=(row["lease_status"]=="ACTIVE" and row["lease_current"] is True)
                if lease_active:
                    if not row["worker_instance_id"] or row["instance_status"] in {None,"OFFLINE","REJECTED"}:
                        unresolved.append({"task_id":row["task_id"],"reason":"active lease has no commandable worker instance"})
                        continue
                    previous=self.db.execute("""SELECT command_id,status FROM runtime.worker_commands
                        WHERE worker_instance_id=%s AND task_id=%s AND attempt_id=%s AND lease_epoch=%s
                          AND command='CANCEL_ATTEMPT' AND status IN ('PENDING','DELIVERED','ACKNOWLEDGED')
                        ORDER BY created_at LIMIT 1 FOR UPDATE""",
                        (row["worker_instance_id"],row["task_id"],row["attempt_id"],row["lease_epoch"])).fetchone()
                    if previous:
                        commands.append({"command_id":previous["command_id"],"task_id":row["task_id"],
                                         "status":previous["status"],"idempotent":True})
                        continue
                    command_id="cmd_cancel_"+uuid.uuid4().hex
                    self.db.execute("""INSERT INTO runtime.worker_commands
                        (command_id,worker_instance_id,task_id,attempt_id,lease_epoch,command,status)
                        VALUES (%s,%s,%s,%s,%s,'CANCEL_ATTEMPT','PENDING')""",
                        (command_id,row["worker_instance_id"],row["task_id"],row["attempt_id"],row["lease_epoch"]))
                    self._event(event_type="PLAN_TASK_CANCELLATION_REQUESTED",goal_id=row["goal_id"],
                        plan_version_id=plan_version_id,actor_id=requested_by,task_id=row["task_id"],
                        payload={"plan_id":plan["plan_id"],"plan_version":plan["version"],
                            "worker_id":row["worker_id"],"worker_instance_id":row["worker_instance_id"],
                            "attempt_id":row["attempt_id"],"lease_epoch":row["lease_epoch"],
                            "command_id":command_id})
                    commands.append({"command_id":command_id,"task_id":row["task_id"],
                                     "status":"PENDING","idempotent":False})
                    continue
                if row["status"] in {"DRAFT","QUEUED","RETRY_PENDING","BLOCKED"}:
                    self.db.execute("UPDATE runtime.tasks SET status='CANCELLED',updated_at=now() WHERE task_id=%s",
                                    (row["task_id"],))
                    CampaignAccounting(self.db).finish_task_reservation(task_id=row["task_id"],
                        attempt_id=None,terminal_status="CANCELLED")
                    self._event(event_type="TASK_CANCELLED",goal_id=row["goal_id"],
                        plan_version_id=plan_version_id,actor_id=requested_by,task_id=row["task_id"],
                        payload={"reason":"accepted plan cancellation","plan_id":plan["plan_id"],
                                 "plan_version":plan["version"]})
                    delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                        SET status='CANCELLED',updated_at=now() WHERE child_task_id=%s
                          AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED')
                        RETURNING delegation_id""",(row["task_id"],)).fetchone()
                    if delegation:
                        self._event(event_type="DELEGATION_CANCELLED",goal_id=row["goal_id"],
                            plan_version_id=plan_version_id,actor_id=requested_by,task_id=row["task_id"],
                            payload={"delegation_id":delegation["delegation_id"],
                                     "reason":"accepted plan cancellation"})
                    cancelled.append(row["task_id"])
                else:
                    unresolved.append({"task_id":row["task_id"],"reason":"nonterminal task awaits lease reconciliation"})
            self._event(event_type="PLAN_CANCELLATION_RECONCILED",goal_id=plan["goal_id"],
                plan_version_id=plan_version_id,actor_id=requested_by,
                payload={"plan_id":plan["plan_id"],"cancelled_unstarted":cancelled,
                    "command_ids":[item["command_id"] for item in commands],"unresolved":unresolved})
            return {"plan_version_id":plan_version_id,"cancelled_unstarted":cancelled,
                    "commands":commands,"unresolved":unresolved,"already_terminal":terminal}
