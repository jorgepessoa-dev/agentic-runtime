from __future__ import annotations

import uuid
from typing import Any, Mapping

from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.coordinator.state import TaskState, require_transition
from agentic_runtime.coordinator.claim import TaskClaimService
from agentic_runtime.coordinator.transitions import TaskTransitionService
from agentic_runtime.coordinator.reconciliation import ReconciliationService
from agentic_runtime.contracts.plan import PlanProposal, validate_plan
from agentic_runtime.contracts.output import validate_output
from agentic_runtime.contracts.serialization import canonical_hash as _canonical_hash
from agentic_runtime.persistence.events import jsonb as _jsonb, runtime_event


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class Coordinator:
    """Small PostgreSQL-backed authoritative task coordinator."""

    def __init__(self, connection: Any) -> None:
        self.db = connection
        self._task_claims = TaskClaimService(connection, self._event)
        self._task_transitions = TaskTransitionService(connection, self._event)
        self._reconciliation = ReconciliationService(connection, self._event)

    def _event(self, *, event_type: str, actor_id: str, correlation_id: str,
               campaign_id: str | None = None, goal_id: str | None = None,
               task_id: str | None = None, attempt_id: str | None = None,
               payload: Mapping[str, Any] | None = None, causation_id: str | None = None) -> str:
        event_id = _id("evt")
        return runtime_event(self.db, event_id, event_type, actor_id=actor_id,
            correlation_id=correlation_id, campaign_id=campaign_id, goal_id=goal_id,
            task_id=task_id, attempt_id=attempt_id, payload=payload or {},
            causation_id=causation_id)

    def create_campaign(self, campaign_id: str, *, idempotency_key: str,
                        description: str, created_by: str, budget: Mapping[str, Any],
                        max_children: int = 6) -> str:
        request = {"description": description, "budget": dict(budget),"max_children":max_children}
        request_hash = _canonical_hash(request)
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"campaign:{idempotency_key}",))
            row = self.db.execute("SELECT campaign_id,request_hash FROM runtime.campaigns WHERE idempotency_key=%s FOR UPDATE",
                                  (idempotency_key,)).fetchone()
            if row:
                if row["request_hash"] != request_hash:
                    raise ValueError("idempotency key reused with different campaign request")
                return row["campaign_id"]
            self.db.execute("""INSERT INTO runtime.campaigns
                (campaign_id,idempotency_key,request_hash,description,status,budget,max_children,created_by)
                VALUES (%s,%s,%s,%s,'ACTIVE',%s,%s,%s)""",
                (campaign_id,idempotency_key,request_hash,description,_jsonb(dict(budget)),max_children,created_by))
            self._event(event_type="CAMPAIGN_CREATED", actor_id=created_by,
                        correlation_id=campaign_id, campaign_id=campaign_id, payload=request)
        return campaign_id

    def cancel_dispatchable_campaign_tasks(self, campaign_id: str, *, actor_id: str,
                                          reason: str) -> int:
        """Cancel queued work after its owning execution campaign is terminal.

        Claiming already fences terminal campaigns. This reconciliation also
        gives their never-started/retry-pending tasks an explicit durable state
        and event instead of leaving work that can never become runnable.
        Active attempts are deliberately handled by lease/recovery logic.
        """
        with self.db.transaction():
            campaign = self.db.execute("""SELECT status FROM runtime.campaigns
                WHERE campaign_id=%s FOR UPDATE""", (campaign_id,)).fetchone()
            if not campaign:
                raise ValueError("campaign not found")
            if campaign["status"] not in {"CANCELLED", "COMPLETED"}:
                return 0
            rows = self.db.execute("""SELECT task_id,goal_id,status FROM runtime.tasks
                WHERE campaign_id=%s AND status IN ('DRAFT','QUEUED','RETRY_PENDING')
                ORDER BY created_at,task_id FOR UPDATE""", (campaign_id,)).fetchall()
            for row in rows:
                self.db.execute("UPDATE runtime.tasks SET status='CANCELLED',updated_at=now() WHERE task_id=%s",
                                (row["task_id"],))
                self._event(event_type="TASK_CANCELLED", actor_id=actor_id,
                    correlation_id=row["task_id"], campaign_id=campaign_id,
                    goal_id=row["goal_id"], task_id=row["task_id"],
                    payload={"reason":reason,"previous_status":row["status"]})
                CampaignAccounting(self.db).finish_task_reservation(task_id=row["task_id"],
                    attempt_id=None,terminal_status="CANCELLED")
            return len(rows)

    def create_goal(self, goal_id: str, campaign_id: str, *, description: str,
                    mission_ref: str, created_by: str, priority: int = 0) -> str:
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.goals
                (goal_id,campaign_id,status,description,priority,mission_ref,created_by)
                VALUES (%s,%s,'ACTIVE',%s,%s,%s,%s)""",
                (goal_id,campaign_id,description,priority,mission_ref,created_by))
            self._event(event_type="GOAL_CREATED", actor_id=created_by, correlation_id=goal_id,
                        campaign_id=campaign_id, goal_id=goal_id, payload={"description": description})
        return goal_id

    def create_opportunity(self, opportunity_id: str, goal_id: str, *, kind: str,
                           description: str, observation_refs: list[str],
                           created_by: str = "coordinator") -> str:
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.opportunities
                (opportunity_id,goal_id,kind,observation_refs,description,status)
                VALUES (%s,%s,%s,%s,%s,'OPEN')""",
                (opportunity_id,goal_id,kind,_jsonb(observation_refs),description))
            self._event(event_type="OPPORTUNITY_RECORDED",actor_id=created_by,
                correlation_id=opportunity_id,goal_id=goal_id,
                payload={"kind":kind,"observation_refs":observation_refs})
        return opportunity_id

    def create_hypothesis(self, hypothesis_id: str, opportunity_id: str, *,
                          statement: str, falsification_ref: str | None,
                          created_by: str) -> str:
        with self.db.transaction():
            self.db.execute("""INSERT INTO runtime.hypotheses
                (hypothesis_id,opportunity_id,statement,falsification_ref,status,created_by)
                VALUES (%s,%s,%s,%s,'PROPOSED',%s)""",
                (hypothesis_id,opportunity_id,statement,falsification_ref,created_by))
            self._event(event_type="HYPOTHESIS_PROPOSED",actor_id=created_by,
                correlation_id=hypothesis_id,payload={"opportunity_id":opportunity_id,
                "statement":statement,"falsification_ref":falsification_ref})
        return hypothesis_id

    def materialize_plan(self, proposal: PlanProposal, *, campaign_id: str,
                         task_id_for: Mapping[str, str], idempotency_prefix: str) -> list[str]:
        """Validate an untrusted proposal, then atomically persist tasks and dispatch intents."""
        validate_plan(proposal)
        if set(task_id_for) != {item.proposal_id for item in proposal.proposed_tasks}:
            raise ValueError("every proposed task needs one runtime task identity")
        tasks_json = [{"proposal_id": item.proposal_id, "task_type": item.task_type,
                       "capabilities": item.required_capabilities,
                       "input_refs": item.input_refs,"budget": item.budget}
                      for item in proposal.proposed_tasks]
        dep_json = [[item.proposal_id,parent] for item in proposal.proposed_tasks for parent in item.depends_on]
        with self.db.transaction():
            goal = self.db.execute("SELECT campaign_id FROM runtime.goals WHERE goal_id=%s FOR SHARE",
                                   (proposal.goal_id,)).fetchone()
            if not goal or goal["campaign_id"] != campaign_id:
                raise ValueError("plan goal is absent or belongs to another campaign")
            self.db.execute("""INSERT INTO runtime.plan_proposals
                (plan_id,goal_id,proposed_tasks,dependencies,rationale_ref,estimated_budget,created_by,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'VALIDATED')""",
                (proposal.plan_id,proposal.goal_id,_jsonb(tasks_json),_jsonb(dep_json),
                 proposal.rationale_ref,_jsonb(dict(proposal.estimated_budget)),proposal.created_by))
            for item in proposal.proposed_tasks:
                dependencies = [(task_id_for[parent],"REQUIRES_ACCEPTED") for parent in item.depends_on]
                self.create_task(task_id_for[item.proposal_id],campaign_id,task_type=item.task_type,
                    idempotency_key=f"{idempotency_prefix}:{item.proposal_id}",input_refs=list(item.input_refs),
                    output_contract=item.output_contract,goal_id=proposal.goal_id,plan_id=proposal.plan_id,
                    required_capabilities=list(item.required_capabilities),budget=item.budget,
                    dependencies=dependencies,created_by="coordinator")
            self._event(event_type="PLAN_MATERIALIZED",actor_id="coordinator",
                correlation_id=proposal.plan_id,goal_id=proposal.goal_id,
                campaign_id=campaign_id,payload={"plan_id":proposal.plan_id,"tasks":list(task_id_for.values())})
            self.db.execute("UPDATE runtime.plan_proposals SET status='MATERIALIZED' WHERE plan_id=%s", (proposal.plan_id,))
        return [task_id_for[item.proposal_id] for item in proposal.proposed_tasks]

    def create_task(self, task_id: str, campaign_id: str, *, task_type: str,
                    idempotency_key: str, input_refs: list[str] | None = None,
                    output_contract: Mapping[str, Any] | None = None, goal_id: str | None = None,
                    plan_id: str | None = None,
                    parent_task_id: str | None = None, priority: int = 0,
                    required_capabilities: list[str] | None = None,
                    metadata: Mapping[str, Any] | None = None,
                    budget: Mapping[str, Any] | None = None, max_children: int = 0,
                    depth_remaining: int = 0, dependencies: list[tuple[str, str]] | None = None,
                    created_by: str = "coordinator") -> str:
        request = {"campaign_id": campaign_id, "task_type": task_type, "input_refs": input_refs or [],
                   "output_contract": output_contract or {}, "goal_id": goal_id,
                   "plan_id": plan_id,"parent_task_id": parent_task_id, "budget": budget or {},
                   "priority":priority,"required_capabilities":required_capabilities or [],
                   "metadata":dict(metadata or {}),"max_children":max_children,"depth_remaining":depth_remaining,
                   "dependencies":dependencies or []}
        request_hash = _canonical_hash(request)
        with self.db.transaction():
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"task:{idempotency_key}",))
            prior = self.db.execute("SELECT task_id,request_hash FROM runtime.tasks WHERE idempotency_key=%s FOR UPDATE",
                                    (idempotency_key,)).fetchone()
            if prior:
                if prior["request_hash"] != request_hash:
                    raise ValueError("idempotency key reused with different task request")
                return prior["task_id"]
            campaign = self.db.execute("SELECT status FROM runtime.campaigns WHERE campaign_id=%s FOR UPDATE",
                                       (campaign_id,)).fetchone()
            if not campaign or campaign["status"] != "ACTIVE":
                raise ValueError("new tasks require an active campaign")
            task_budget=dict(budget or {})
            configured_attempts=task_budget.pop("max_attempts", None)
            if configured_attempts is not None and (isinstance(configured_attempts,bool)
                    or not isinstance(configured_attempts,int) or configured_attempts < 0
                    or configured_attempts > 2147483647):
                raise ValueError("max_attempts must be an integer between zero and 2147483647")
            task_metadata=dict(metadata or {})
            plan_version_id=task_metadata.pop("plan_version_id",None)
            self.db.execute("""INSERT INTO runtime.tasks
                (task_id,campaign_id,goal_id,plan_id,plan_version_id,parent_task_id,task_type,status,priority,
                 required_capabilities,input_refs,output_contract,budget,max_attempts,max_children,metadata,
                 depth_remaining,idempotency_key,request_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'QUEUED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (task_id,campaign_id,goal_id,plan_id,plan_version_id,parent_task_id,task_type,priority,
                 _jsonb(required_capabilities or []),_jsonb(input_refs or []),
                 _jsonb(output_contract or {}),_jsonb(task_budget),configured_attempts,max_children,
                 _jsonb(task_metadata),depth_remaining,idempotency_key,request_hash))
            for depends_on, dep_type in dependencies or []:
                self.db.execute("INSERT INTO runtime.task_dependencies(task_id,depends_on_task_id,dependency_type) VALUES (%s,%s,%s)",
                                (task_id,depends_on,dep_type))
            event_id = self._event(event_type="TASK_QUEUED", actor_id=created_by, correlation_id=task_id,
                        campaign_id=campaign_id, goal_id=goal_id, task_id=task_id, payload=request)
            self.db.execute("""INSERT INTO runtime.outbox
                (outbox_id,event_id,topic,idempotency_key,payload)
                VALUES (%s,%s,'task.dispatch',%s,%s)
                ON CONFLICT(idempotency_key) DO NOTHING""",
                (_id("out"),event_id,f"dispatch:{task_id}",_jsonb({"task_id":task_id})))
        return task_id

    def register_worker(self, worker_id: str, capabilities: list[str]) -> None:
        self.db.execute("""INSERT INTO runtime.workers(worker_id,status,capabilities)
            VALUES (%s,'IDLE',%s) ON CONFLICT(worker_id) DO UPDATE SET
            status='IDLE',capabilities=EXCLUDED.capabilities,updated_at=now()""",
            (worker_id,_jsonb(capabilities)))

    def choose_executor(self, *, task_id: str, attempt_id: str, required_capabilities: list[str],
                        policy_ref: str, preferred_executor: str | None = None,
                        excluded_executor_refs: list[str] | None = None) -> str:
        """Deterministically choose a configured eligible executor and persist alternatives."""
        excluded = set(excluded_executor_refs or [])
        rows = self.db.execute("""SELECT executor_id,capabilities,enabled,metadata FROM runtime.executors
            ORDER BY executor_id""").fetchall()
        eligible = [row for row in rows if row["enabled"] and row["executor_id"] not in excluded
                    and set(required_capabilities).issubset(set(row["capabilities"]))
                    and row["metadata"].get("health", "HEALTHY") == "HEALTHY"]
        if not eligible:
            raise ValueError("no executor satisfies capabilities and runtime policy")
        selected = next((row for row in eligible if row["executor_id"] == preferred_executor), eligible[0])
        self.db.execute("""INSERT INTO runtime.routing_decisions
            (decision_id,task_id,attempt_id,required_capabilities,eligible_executor_refs,
             excluded_executor_refs,selected_executor_ref,policy_ref,reason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (_id("route"),task_id,attempt_id,_jsonb(required_capabilities),
             _jsonb([row["executor_id"] for row in eligible]),_jsonb(sorted(excluded)),
             selected["executor_id"],policy_ref,
             "preferred eligible executor" if selected["executor_id"] == preferred_executor else "first eligible by stable executor identity"))
        return selected["executor_id"]

    def claim(self, worker_id: str, *, lease_seconds: int = 60,
              allowed_task_types: list[str] | None = None,
              resource_classes: list[str] | None = None,
              worker_instance_id: str | None = None) -> dict[str, Any] | None:
        """Facade for atomic task claim and lease issuance."""
        return self._task_claims.claim(worker_id, attempt_id=_id("att"),
            lease_seconds=lease_seconds, allowed_task_types=allowed_task_types,
            resource_classes=resource_classes, worker_instance_id=worker_instance_id)

    def transition(self, task_id: str, attempt_id: str, lease_epoch: int,
                   target: TaskState, *, actor_id: str, payload: Mapping[str, Any] | None = None) -> None:
        self._task_transitions.transition(task_id, attempt_id, lease_epoch, target,
            actor_id=actor_id, payload=payload)

    def heartbeat(self, task_id: str, worker_id: str, attempt_id: str, lease_epoch: int,
                  *, lease_seconds: int = 60) -> bool:
        return self._task_transitions.heartbeat(task_id, worker_id, attempt_id, lease_epoch,
            lease_seconds=lease_seconds)

    def authorize_child(self, *, request_id: str, parent_task_id: str, parent_attempt_id: str,
                        lease_epoch: int, child_task_id: str, idempotency_key: str,
                        task_type: str, goal: str, capabilities: list[str], input_refs: list[str],
                        requested_budget: Mapping[str, float], max_depth: int = 1) -> dict[str, Any]:
        """Authorize and reserve child budget before making the child dispatchable."""
        with self.db.transaction():
            parent = self.db.execute("""SELECT t.*,l.worker_id,l.attempt_id,l.lease_epoch AS current_epoch,
                l.status AS lease_status FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE t.task_id=%s FOR UPDATE OF t,l""", (parent_task_id,)).fetchone()
            if not parent or parent["lease_status"] != "ACTIVE" or parent["attempt_id"] != parent_attempt_id \
                    or parent["current_epoch"] != lease_epoch or parent["status"] != "RUNNING":
                raise ValueError("subtask request lacks a current parent lease")
            if parent.get("plan_version_id"):
                raise ValueError("accepted plan is immutable; new delegation requires an accepted successor plan")
            task_request = {"campaign_id": parent["campaign_id"], "task_type": task_type,
                            "goal": goal, "input_refs": input_refs, "budget": dict(requested_budget),
                            "parent_task_id":parent_task_id,"capabilities":capabilities}
            request_hash = _canonical_hash(task_request)
            self.db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"child:{idempotency_key}",))
            previous = self.db.execute("SELECT task_id,request_hash FROM runtime.tasks WHERE idempotency_key=%s FOR UPDATE",
                                       (idempotency_key,)).fetchone()
            if previous:
                if previous["request_hash"] != request_hash:
                    raise ValueError("child idempotency key reused with different request")
                return {"approved":True,"child_task_id":previous["task_id"],
                        "reserved_budget":dict(requested_budget),"reason":"duplicate authorized request"}
            children = self.db.execute("SELECT count(*) AS n FROM runtime.tasks WHERE parent_task_id=%s", (parent_task_id,)).fetchone()["n"]
            if children >= parent["max_children"]:
                raise ValueError("parent child limit reached")
            depth = int(parent["depth_remaining"])
            if depth <= 0 or depth > max_depth:
                raise ValueError("delegation depth limit reached")
            limits = parent["budget"] or {}
            reserved = parent["reserved_budget"] or {}
            campaign = self.db.execute("SELECT budget,reserved_budget FROM runtime.campaigns WHERE campaign_id=%s FOR UPDATE",
                                       (parent["campaign_id"],)).fetchone()
            campaign_children = self.db.execute("SELECT count(*) AS n FROM runtime.tasks WHERE campaign_id=%s AND parent_task_id IS NOT NULL",
                                                (parent["campaign_id"],)).fetchone()["n"]
            campaign_limits_row = self.db.execute("SELECT max_children FROM runtime.campaigns WHERE campaign_id=%s",
                                                  (parent["campaign_id"],)).fetchone()
            if campaign_children >= campaign_limits_row["max_children"]:
                raise ValueError("campaign child limit reached")
            campaign_limits = campaign["budget"] or {}
            campaign_reserved = campaign["reserved_budget"] or {}
            for dimension, amount in requested_budget.items():
                available = float(limits.get(dimension, 0)) - float(reserved.get(dimension, 0))
                if amount <= 0 or float(amount) > available:
                    raise ValueError(f"insufficient reserved budget for {dimension}")
                campaign_available = float(campaign_limits.get(dimension, 0)) - float(campaign_reserved.get(dimension, 0))
                if float(amount) > campaign_available:
                    raise ValueError(f"campaign budget exhausted for {dimension}")
                reserved[dimension] = float(reserved.get(dimension, 0)) + float(amount)
                campaign_reserved[dimension] = float(campaign_reserved.get(dimension, 0)) + float(amount)
            self.db.execute("UPDATE runtime.tasks SET reserved_budget=%s WHERE task_id=%s",
                            (_jsonb(reserved),parent_task_id))
            self.db.execute("UPDATE runtime.campaigns SET reserved_budget=%s WHERE campaign_id=%s",
                            (_jsonb(campaign_reserved),parent["campaign_id"]))
            child_reservation=CampaignAccounting(self.db).reserve_child_task(goal_id=parent["goal_id"],
                task_id=child_task_id,idempotency_key=idempotency_key)
            child_budget=dict(requested_budget)
            child_max_attempts=child_budget.pop("max_attempts",None)
            self.db.execute("""INSERT INTO runtime.tasks
                (task_id,campaign_id,goal_id,plan_version_id,parent_task_id,task_type,status,
                 required_capabilities,input_refs,budget,max_attempts,max_children,depth_remaining,
                 idempotency_key,request_hash,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,'QUEUED',%s,%s,%s,%s,0,%s,%s,%s,%s)""",
                (child_task_id,parent["campaign_id"],parent["goal_id"],parent["plan_version_id"],
                 parent_task_id,task_type,_jsonb(capabilities),_jsonb(input_refs),_jsonb(child_budget),
                 child_max_attempts,max(0,depth-1),idempotency_key,request_hash,_jsonb({"goal": goal})))
            self.db.execute("INSERT INTO runtime.task_dependencies(task_id,depends_on_task_id,dependency_type) VALUES (%s,%s,'ORDER_ONLY')",
                            (child_task_id,parent_task_id))
            if child_reservation:
                CampaignAccounting(self.db).attach_and_dispatch(child_reservation,task_id=child_task_id)
            self._event(event_type="TASK_QUEUED",actor_id="coordinator",
                correlation_id=request_id,campaign_id=parent["campaign_id"],goal_id=parent["goal_id"],
                task_id=child_task_id,payload=task_request)
            child_event = self._event(event_type="CHILD_TASK_AUTHORIZED",actor_id="coordinator",
                correlation_id=request_id,campaign_id=parent["campaign_id"],goal_id=parent["goal_id"],
                task_id=child_task_id,attempt_id=None,payload={"parent_task_id": parent_task_id,
                "reserved_budget": dict(requested_budget), "depth_remaining": depth-1})
            outbox_id = _id("out")
            self.db.execute("""INSERT INTO runtime.outbox(outbox_id,event_id,topic,idempotency_key,payload)
                VALUES (%s,%s,'task.dispatch',%s,%s) ON CONFLICT(idempotency_key) DO NOTHING""",
                (outbox_id,child_event,f"dispatch:{child_task_id}",_jsonb({"task_id": child_task_id})))
            return {"approved": True, "child_task_id": child_task_id,
                    "reserved_budget": dict(requested_budget), "depth_remaining": depth-1}

    def reconcile_expired_leases(self, *, retry_delay_seconds: int = 0) -> list[str]:
        return self._reconciliation.reconcile_expired_leases(retry_delay_seconds=retry_delay_seconds)

    def reconcile_orphaned_attempts(self, *, batch_size: int = 100) -> list[str]:
        return self._reconciliation.reconcile_orphaned_attempts(batch_size=batch_size)

    def reconcile_impossible_joins(self, *, batch_size: int = 100) -> list[str]:
        return self._reconciliation.reconcile_impossible_joins(batch_size=batch_size)

    def reconcile_abandoned_delegations(self, *, batch_size: int = 100) -> list[str]:
        return self._reconciliation.reconcile_abandoned_delegations(batch_size=batch_size)

    def reconcile_expired_plan_deadlines(self, *, batch_size: int = 100) -> list[str]:
        return self._reconciliation.reconcile_expired_plan_deadlines(batch_size=batch_size)

    def reconcile_runtime(self, *, scan_id: str | None = None,
                          retry_delay_seconds: int = 0) -> dict[str, Any]:
        return self._reconciliation.reconcile_runtime(scan_id=scan_id,
            retry_delay_seconds=retry_delay_seconds)

    def reconcile_exhausted_campaigns(self) -> int:
        return self._reconciliation.reconcile_exhausted_campaigns()

    def commit_result(self, *, task_id: str, attempt_id: str, lease_epoch: int,
                      artifact_refs: list[str], result_hash: str, actor_id: str = "worker",
                      submission_metadata: Mapping[str, Any] | None = None) -> str:
        """Commit only artifact references already verified by the artifact store."""
        hash_mismatch = False
        event_id = ""
        with self.db.transaction():
            row = self.db.execute("""SELECT t.status,t.campaign_id,t.goal_id,t.lease_epoch,
                l.attempt_id,l.lease_epoch AS active_epoch,l.status AS lease_status
                FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE t.task_id=%s FOR UPDATE OF t,l""", (task_id,)).fetchone()
            if not row or row["lease_status"] != "ACTIVE" or row["attempt_id"] != attempt_id \
                    or row["lease_epoch"] != lease_epoch or row["active_epoch"] != lease_epoch:
                raise ValueError("stale or missing lease fencing token")
            if not artifact_refs:
                raise ValueError("result requires at least one verified artifact")
            artifacts = self.db.execute("""SELECT artifact_id,producer_task_id,producer_attempt_id,
                content_hash,verification_status FROM runtime.artifacts WHERE artifact_id = ANY(%s)""",
                (artifact_refs,)).fetchall()
            if len(artifacts) != len(set(artifact_refs)) or any(
                    a["verification_status"] != "VERIFIED" or a["producer_task_id"] != task_id
                    or a["producer_attempt_id"] != attempt_id for a in artifacts):
                raise ValueError("artifact verification or producer lineage failed")
            if result_hash not in {artifact["content_hash"] for artifact in artifacts}:
                event_id = self._event(event_type="TASK_RESULT_REJECTED",actor_id=actor_id,
                    correlation_id=task_id,campaign_id=row["campaign_id"],goal_id=row["goal_id"],
                    task_id=task_id,attempt_id=attempt_id,payload={"artifact_refs":artifact_refs,
                        "declared_result_hash":result_hash,
                        "verified_artifact_hashes":sorted({artifact["content_hash"] for artifact in artifacts}),
                        "reason":"declared result hash does not match a verified artifact"})
                hash_mismatch = True
            else:
                require_transition(row["status"], TaskState.RESULT_COMMITTED)
                self.db.execute("UPDATE runtime.tasks SET status='RESULT_COMMITTED',result_refs=%s,result_hash=%s,updated_at=now() WHERE task_id=%s",
                                (_jsonb(artifact_refs),result_hash,task_id))
                self.db.execute("UPDATE runtime.attempts SET status='RESULT_COMMITTED' WHERE attempt_id=%s", (attempt_id,))
                event_id = self._event(event_type="TASK_RESULT_COMMITTED",actor_id=actor_id,
                    correlation_id=task_id,campaign_id=row["campaign_id"],goal_id=row["goal_id"],
                    task_id=task_id,attempt_id=attempt_id,payload={"artifact_refs": artifact_refs,"result_hash": result_hash,
                        **dict(submission_metadata or {})})
                delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                    SET status='DELIVERED',updated_at=now() WHERE child_task_id=%s
                    AND status IN ('PLAN_ACCEPTED','AUTHORIZED') RETURNING delegation_id""",(task_id,)).fetchone()
                if delegation:
                    self._event(event_type="DELEGATION_DELIVERED",actor_id=actor_id,
                        correlation_id=delegation["delegation_id"],campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],task_id=task_id,attempt_id=attempt_id,
                        payload={"delegation_id":delegation["delegation_id"],"result_hash":result_hash})
                self.db.execute("""INSERT INTO runtime.outbox(outbox_id,event_id,topic,idempotency_key,payload)
                    VALUES (%s,%s,'result.verify',%s,%s) ON CONFLICT(idempotency_key) DO NOTHING""",
                    (_id("out"),event_id,f"verify:{task_id}:{lease_epoch}",
                     _jsonb({"task_id": task_id,"attempt_id": attempt_id,"lease_epoch": lease_epoch})))
        if hash_mismatch:
            raise ValueError("declared result hash does not match a verified artifact")
        return event_id

    def register_artifact(self, *, task_id: str, attempt_id: str, lease_epoch: int,
                          manifest: Any, artifact_store: Any, input_manifest_hash: str,
                          schema_version: str = "1") -> str:
        """Verify bytes and producer lineage before adding the durable manifest."""
        verified = artifact_store.verify(manifest.artifact_id)
        if (verified.content_hash != manifest.content_hash
                or verified.producer_attempt_id != attempt_id):
            raise ValueError("artifact hash or producer attempt mismatch")
        artifact_bytes=artifact_store.read(manifest.artifact_id)
        with self.db.transaction():
            lease = self.db.execute("""SELECT t.metadata FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE t.task_id=%s AND t.lease_epoch=%s AND l.attempt_id=%s
                  AND l.lease_epoch=%s AND l.status='ACTIVE' AND l.lease_until>now()
                FOR UPDATE OF t,l""", (task_id,lease_epoch,attempt_id,lease_epoch)).fetchone()
            if not lease:
                raise ValueError("artifact producer attempt has stale lease")
            artifact_cap=(lease["metadata"] or {}).get("max_artifact_bytes")
            if artifact_cap is not None and len(artifact_bytes)>int(artifact_cap):
                raise ValueError("artifact exceeds the accepted plan size bound")
            self.db.execute("""INSERT INTO runtime.artifacts
                (artifact_id,kind,schema_version,content_hash,producer_task_id,producer_attempt_id,
                 input_manifest_hash,location,metadata,verification_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'VERIFIED')
                ON CONFLICT(artifact_id) DO NOTHING""",
                (manifest.artifact_id,manifest.kind,schema_version,manifest.content_hash,task_id,
                 attempt_id,input_manifest_hash,manifest.location,_jsonb(dict(manifest.metadata))))
            row = self.db.execute("SELECT content_hash,producer_task_id,producer_attempt_id,verification_status FROM runtime.artifacts WHERE artifact_id=%s",
                                  (manifest.artifact_id,)).fetchone()
            if not row or row["content_hash"] != verified.content_hash or row["producer_task_id"] != task_id \
                    or row["producer_attempt_id"] != attempt_id or row["verification_status"] != "VERIFIED":
                raise ValueError("existing artifact identity conflicts with verified bytes")
        return manifest.artifact_id

    def accept_verified_result(self, *, task_id: str, attempt_id: str, lease_epoch: int,
                               artifact_store: Any, actor_id: str = "verifier") -> str:
        """Accept only committed results whose content-addressed artifacts still verify."""
        with self.db.transaction():
            row = self.db.execute("""SELECT t.status,t.campaign_id,t.goal_id,t.result_refs,t.result_hash,t.metadata,
                t.lease_epoch,l.attempt_id,l.lease_epoch AS active_epoch,l.status AS lease_status
                FROM runtime.tasks t JOIN runtime.leases l USING(task_id)
                WHERE t.task_id=%s FOR UPDATE OF t,l""", (task_id,)).fetchone()
            if not row or row["status"] != "RESULT_COMMITTED" or row["attempt_id"] != attempt_id \
                    or row["active_epoch"] != lease_epoch or row["lease_epoch"] != lease_epoch \
                    or row["lease_status"] != "ACTIVE":
                raise ValueError("result is not current or committed under the active lease")
            if row["metadata"].get("requires_independent_verification"):
                independent = self.db.execute("""SELECT 1 FROM runtime.task_verifications v
                    JOIN runtime.tasks producer ON producer.task_id=v.task_id
                    JOIN runtime.attempts source_attempt ON source_attempt.attempt_id=v.attempt_id
                    JOIN runtime.tasks reviewer ON reviewer.task_id=v.verifier_task_id
                    JOIN runtime.attempts reviewer_attempt ON reviewer_attempt.task_id=reviewer.task_id
                       AND reviewer_attempt.status='ACCEPTED'
                    WHERE v.task_id=%s AND v.attempt_id=%s AND v.status='ACCEPTED'
                      AND v.result_hash=%s AND reviewer.status='ACCEPTED'
                      AND reviewer.task_id<>producer.task_id
                      AND reviewer_attempt.worker_id<>source_attempt.worker_id
                    LIMIT 1""", (task_id,attempt_id,row["result_hash"])).fetchone()
                if not independent:
                    raise ValueError("independent verifier evidence is required before acceptance")
            refs = row["result_refs"]
            artifacts = self.db.execute("""SELECT artifact_id,content_hash,producer_task_id,
                producer_attempt_id,verification_status,location FROM runtime.artifacts
                WHERE artifact_id=ANY(%s)""", (refs,)).fetchall()
            intact = len(artifacts) == len(refs)
            if intact:
                for artifact in artifacts:
                    try:
                        manifest = artifact_store.verify(artifact["artifact_id"])
                        intact = (manifest.content_hash == artifact["content_hash"]
                                  and artifact["verification_status"] == "VERIFIED"
                                  and artifact["producer_task_id"] == task_id
                                  and artifact["producer_attempt_id"] == attempt_id)
                        if intact:
                            contract = self.db.execute("SELECT output_contract FROM runtime.tasks WHERE task_id=%s",
                                                       (task_id,)).fetchone()["output_contract"]
                            validate_output(artifact_store.read(artifact["artifact_id"]),contract)
                    except Exception:
                        intact = False
                    if not intact:
                        break
            if not intact:
                self.db.execute("UPDATE runtime.model_runs SET schema_valid=false WHERE task_id=%s AND attempt_id=%s",
                                (task_id,attempt_id))
                self.db.execute("UPDATE runtime.artifacts SET verification_status='QUARANTINED' WHERE artifact_id=ANY(%s)", (refs,))
                self.db.execute("UPDATE runtime.tasks SET status='QUARANTINED',updated_at=now() WHERE task_id=%s", (task_id,))
                self.db.execute("UPDATE runtime.attempts SET status='QUARANTINED',completed_at=now() WHERE attempt_id=%s", (attempt_id,))
                self.db.execute("UPDATE runtime.leases SET status='RELEASED',updated_at=now() WHERE task_id=%s AND attempt_id=%s AND lease_epoch=%s",
                    (task_id,attempt_id,lease_epoch))
                self.db.execute("""UPDATE runtime.workers SET status='IDLE',updated_at=now()
                    WHERE worker_id=(SELECT worker_id FROM runtime.attempts WHERE attempt_id=%s)
                      AND status='BUSY' AND NOT EXISTS (SELECT 1 FROM runtime.leases
                        WHERE worker_id=(SELECT worker_id FROM runtime.attempts WHERE attempt_id=%s)
                          AND status='ACTIVE' AND lease_until>now())""",(attempt_id,attempt_id))
                CampaignAccounting(self.db).finish_task_reservation(task_id=task_id,
                    attempt_id=attempt_id,terminal_status="QUARANTINED")
                self._event(event_type="TASK_QUARANTINED",actor_id=actor_id,correlation_id=task_id,
                    campaign_id=row["campaign_id"],goal_id=row["goal_id"],task_id=task_id,
                    attempt_id=attempt_id,payload={"reason":"artifact lineage or manifest verification failed"})
                delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                    SET status='REJECTED',updated_at=now() WHERE child_task_id=%s
                      AND status NOT IN ('ACCEPTED','REJECTED','CANCELLED')
                    RETURNING delegation_id""",(task_id,)).fetchone()
                if delegation:
                    self._event(event_type="DELEGATION_REJECTED",actor_id=actor_id,
                        correlation_id=delegation["delegation_id"],campaign_id=row["campaign_id"],
                        goal_id=row["goal_id"],task_id=task_id,attempt_id=attempt_id,
                        payload={"delegation_id":delegation["delegation_id"],
                            "reason":"producer artifact integrity verification failed"})
                return "QUARANTINED"
            # schema_valid is verifier-owned evidence. Remote workers do not
            # get to assert it; this follows successful control-plane output
            # contract validation of the content-addressed artifact.
            self.db.execute("UPDATE runtime.model_runs SET schema_valid=true WHERE task_id=%s AND attempt_id=%s",
                            (task_id,attempt_id))
            require_transition("RESULT_COMMITTED",TaskState.VERIFIED)
            self.db.execute("UPDATE runtime.tasks SET status='VERIFIED',updated_at=now() WHERE task_id=%s", (task_id,))
            self.db.execute("UPDATE runtime.attempts SET status='VERIFIED' WHERE attempt_id=%s", (attempt_id,))
            self._event(event_type="TASK_VERIFIED",actor_id=actor_id,correlation_id=task_id,
                campaign_id=row["campaign_id"],goal_id=row["goal_id"],task_id=task_id,
                attempt_id=attempt_id,payload={"artifact_refs":refs})
            delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                SET status='VERIFIED',updated_at=now() WHERE child_task_id=%s
                AND status='DELIVERED' RETURNING delegation_id""",(task_id,)).fetchone()
            if delegation:
                self._event(event_type="DELEGATION_VERIFIED",actor_id=actor_id,
                    correlation_id=delegation["delegation_id"],campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],task_id=task_id,attempt_id=attempt_id,
                    payload={"delegation_id":delegation["delegation_id"]})
            require_transition("VERIFIED",TaskState.ACCEPTED)
            self.db.execute("UPDATE runtime.tasks SET status='ACCEPTED',updated_at=now() WHERE task_id=%s", (task_id,))
            self.db.execute("UPDATE runtime.attempts SET status='ACCEPTED',completed_at=now() WHERE attempt_id=%s", (attempt_id,))
            self.db.execute("UPDATE runtime.leases SET status='RELEASED',updated_at=now() WHERE task_id=%s", (task_id,))
            self.db.execute("UPDATE runtime.workers SET status='IDLE',updated_at=now() WHERE worker_id=(SELECT worker_id FROM runtime.attempts WHERE attempt_id=%s)", (attempt_id,))
            event_id = self._event(event_type="TASK_ACCEPTED",actor_id=actor_id,correlation_id=task_id,
                campaign_id=row["campaign_id"],goal_id=row["goal_id"],task_id=task_id,
                attempt_id=attempt_id,payload={"artifact_refs":refs,"result_hash":row["result_hash"]})
            delegation=self.db.execute("""UPDATE runtime.coordination_delegations
                SET status='ACCEPTED',updated_at=now() WHERE child_task_id=%s
                AND status='VERIFIED' RETURNING delegation_id""",(task_id,)).fetchone()
            if delegation:
                self._event(event_type="DELEGATION_ACCEPTED",actor_id=actor_id,
                    correlation_id=delegation["delegation_id"],campaign_id=row["campaign_id"],
                    goal_id=row["goal_id"],task_id=task_id,attempt_id=attempt_id,
                    payload={"delegation_id":delegation["delegation_id"],"accepted_event_id":event_id})
            knowledge_id = _id("know")
            self.db.execute("""INSERT INTO runtime.knowledge_objects
                (knowledge_id,kind,content_ref,created_by,source_task_id,source_artifact_refs,
                 status,evidence_quality,reproducibility,review_status)
                VALUES (%s,'EVIDENCE',%s,%s,%s,%s,'ACTIVE','MODERATE','UNKNOWN','UNREVIEWED')""",
                (knowledge_id,event_id,actor_id,task_id,_jsonb(refs)))
            # Remote executions persist reported usage before acceptance; settle
            # the same campaign ledger inside the result transaction, never in a
            # worker-side shadow budget.
            CampaignAccounting(self.db).finish_task_reservation(task_id=task_id,
                attempt_id=attempt_id,terminal_status="SUCCEEDED")
            return knowledge_id
