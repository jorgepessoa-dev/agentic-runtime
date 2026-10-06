"""Persistence services for the bounded E2 workflow-genome lifecycle.

Candidate/evaluation operations use the ordinary runtime connection. Champion
changes require the separately supplied governance/promotion connection.
"""
from __future__ import annotations

import hashlib
import uuid
from typing import Any, Mapping, Sequence

from agentic_runtime.persistence.events import jsonb as _jsonb
from agentic_runtime.accounting.campaign import CampaignAccounting
from agentic_runtime.evolution.e2 import (E2PolicyError, WorkflowMutation,
    apply_workflow_mutation, digest, E2_STRUCTURAL_ALLOWLIST,
    e2_structural_allowlist_hash)


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class E2EvolutionStore:
    """Append-only E2 evidence and separately authorized champion transitions."""

    def __init__(self, db: Any, *, governance_db: Any | None = None,
                 evaluator_db: Any | None = None, verifier_db: Any | None = None,
                 promotion_db: Any | None = None, artifact_store: Any | None = None) -> None:
        self.db = db
        self.governance_db = governance_db
        self.evaluator_db = evaluator_db
        self.verifier_db = verifier_db
        self.promotion_db = promotion_db
        self.artifact_store = artifact_store

    @staticmethod
    def _pg_hash(db: Any, value: Any) -> str:
        return db.execute("SELECT encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS digest",
                          (_jsonb(value),)).fetchone()["digest"]

    def _event(self, db: Any, *, scope_id: str, event_type: str, actor_id: str,
               payload: Mapping[str, Any], campaign_id: str | None = None,
               mutation_id: str | None = None) -> str:
        event_id = _id("e2evt")
        db.execute("""INSERT INTO evolution.e2_events
            (event_id,scope_id,campaign_id,mutation_id,event_type,actor_id,payload,payload_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (event_id,scope_id,campaign_id,mutation_id,event_type,actor_id,
             _jsonb(dict(payload)),self._pg_hash(db,dict(payload))))
        return event_id

    @staticmethod
    def _complete_campaign(db: Any, campaign_id: str, goal_id: str | None = None) -> None:
        db.execute("UPDATE runtime.improvement_campaigns SET status='COMPLETED',ended_at=now() WHERE campaign_id=%s AND status<>'COMPLETED'",
                   (campaign_id,))
        db.execute("UPDATE runtime.campaigns SET status='COMPLETED' WHERE campaign_id=%s AND status='ACTIVE'",
                   (campaign_id,))
        if goal_id:
            db.execute("UPDATE runtime.goals SET status='ACHIEVED' WHERE goal_id=%s AND status='ACTIVE'",
                       (goal_id,))

    @staticmethod
    def _validate_development_source_refs(db: Any, scope_id: str,
                                         source_refs: Sequence[str]) -> None:
        """Require observation inputs to resolve to this scope's public DEV surface."""
        if not source_refs or len(set(source_refs)) != len(source_refs):
            raise E2PolicyError("observation source references must be nonempty and unique")
        for source in source_refs:
            found=None
            if source.startswith("sha256:"):
                found=db.execute("SELECT 1 FROM evolution.e2_workflow_genomes WHERE scope_id=%s AND workflow_hash=%s",
                                 (scope_id,source.removeprefix("sha256:"))).fetchone()
            elif source.startswith("e2-scenario:"):
                found=db.execute("SELECT 1 FROM evolution.e2_evaluation_scenarios WHERE scope_id=%s AND scenario_id=%s AND partition='DEVELOPMENT'",
                                 (scope_id,source.removeprefix("e2-scenario:"))).fetchone()
            elif source.startswith("e2-run:"):
                found=db.execute("SELECT 1 FROM evolution.e2_evaluation_runs WHERE scope_id=%s AND run_id=%s AND partition='DEVELOPMENT' AND status='COMPLETED'",
                                 (scope_id,source.removeprefix("e2-run:"))).fetchone()
            elif source.startswith("e2-artifact:"):
                found=db.execute("""SELECT 1 FROM evolution.e2_evaluation_run_evidence e
                    JOIN evolution.e2_evaluation_runs r USING(run_id)
                    WHERE r.scope_id=%s AND r.partition='DEVELOPMENT' AND r.status='COMPLETED'
                      AND e.artifact_id=%s LIMIT 1""",
                    (scope_id,source.removeprefix("e2-artifact:"))).fetchone()
            if not found:
                raise E2PolicyError("observation source is not persisted DEVELOPMENT evidence in this scope")

    def register_scope(self, *, scope_id: str, description: str, champion_id: str,
                       workflow_template: Mapping[str, Any], suite_id: str,
                       suite_version: str, suite_definition: Mapping[str, Any],
                       evaluator_version: str, policy: Mapping[str, Any],
                       created_by: str) -> str:
        """Register a new E2 scope and its immutable champion/template baseline."""
        authority = self.governance_db or self.db
        from agentic_runtime.evolution.e2 import _plan_from_dict
        from agentic_runtime.contracts.coordination import validate_plan_version
        validate_plan_version(_plan_from_dict(workflow_template))
        config = dict(workflow_template)
        config_hash = self._pg_hash(authority, config)
        policy_value = dict(policy)
        limits=dict(policy_value.get("limits",{}))
        defaults={"max_challengers":2,"max_retained_candidates":3,
                  "max_campaigns":1,"max_evaluations_per_candidate":12,
                  "max_artifact_bytes":1_048_576,"max_wall_time_seconds":120}
        for name, ceiling in defaults.items():
            value=int(limits.get(name,ceiling))
            if value<1 or value>ceiling:
                raise E2PolicyError(f"E2 scope limit {name} is outside its immutable ceiling")
            limits[name]=value
        policy_value["limits"]=limits
        policy_hash = digest({"scope_id": scope_id, "policy": policy_value,
                              "structural_allowlist": E2_STRUCTURAL_ALLOWLIST,
                              "structural_allowlist_hash": e2_structural_allowlist_hash(),
                              "tier": "E2"})
        suite_definition=dict(suite_definition)
        suite_hash=digest(suite_definition)
        suite_ref="e2-suite://"+suite_hash
        with authority.transaction():
            authority.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,%s)",
                              (scope_id,description))
            authority.execute("""INSERT INTO evolution.eval_suite_versions
                (eval_suite_id,version,scope_id,status,definition_ref,integrity_hash,created_by,metadata)
                VALUES (%s,%s,%s,'AUTHORITATIVE',%s,%s,%s,%s)""",
                (suite_id,suite_version,scope_id,suite_ref,suite_hash,created_by,
                 _jsonb({"tier":"E2","evaluator_version":evaluator_version})))
            authority.execute("""INSERT INTO evolution.e2_scope_policies
                (scope_id,policy_version,allowed_paths,limits,evaluation_policy,policy_hash,created_by)
                VALUES (%s,'1',%s,%s,%s,%s,%s)""",
                (scope_id,_jsonb(["/nodes",*E2_STRUCTURAL_ALLOWLIST["mutable_node_fields"]]),
                 _jsonb(policy_value.get("limits",{})),
                 _jsonb(policy_value.get("evaluation",{})),policy_hash,created_by))
            authority.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,
                 mutation_description,mutation_rationale,created_by,metadata)
                VALUES (%s,%s,'1','CHAMPION',%s,%s,'initial E2 workflow champion',%s,%s,%s)""",
                (champion_id,scope_id,"sha256:"+config_hash,config_hash,
                 "governed E2 workflow baseline",created_by,
                 _jsonb({"tier":"E2","template_hash":config_hash})))
            authority.execute("""INSERT INTO evolution.e2_workflow_genomes
                (genome_id,scope_id,parent_genome_id,workflow_template,workflow_hash,
                 changed_paths,provenance) VALUES (%s,%s,NULL,%s,%s,'[]'::jsonb,%s)""",
                (champion_id,scope_id,_jsonb(config),config_hash,
                 _jsonb({"actor":created_by,"tier":"E2"})))
            authority.execute("INSERT INTO evolution.scope_champions(scope_id,genome_id) VALUES (%s,%s)",
                              (scope_id,champion_id))
            self._event(authority,scope_id=scope_id,event_type="E2_SCOPE_REGISTERED",
                        actor_id=created_by,payload={"champion_id":champion_id,"template_hash":config_hash,
                                                    "policy_hash":policy_hash})
        authority.commit()
        return champion_id

    def observe_workflow(self, *, observation_id: str, scope_id: str,
            source_refs: Sequence[str], summary: str, created_by: str,
            measurements: Mapping[str, Any], partition: str = "DEVELOPMENT") -> str:
        """Persist a domain-neutral workflow observation before proposal generation."""
        if partition!="DEVELOPMENT":
            raise E2PolicyError("workflow mutation observations may only cite DEVELOPMENT evidence")
        if not source_refs or not summary.strip() or not created_by:
            raise E2PolicyError("workflow observation needs provenance, a summary and an actor")
        self._validate_development_source_refs(self.db,scope_id,source_refs)
        if not measurements or any(not isinstance(value,(int,float,bool,str)) for value in measurements.values()):
            raise E2PolicyError("workflow observation measurements must be bounded scalar values")
        with self.db.transaction():
            if not self.db.execute("SELECT 1 FROM evolution.e2_scope_policies WHERE scope_id=%s",
                                   (scope_id,)).fetchone():
                raise E2PolicyError("unknown E2 workflow observation scope")
            self.db.execute("""INSERT INTO evolution.observations
                (observation_id,scope_id,source_refs,summary,created_by,metadata)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (observation_id,scope_id,_jsonb(list(source_refs)),summary,created_by,
                 _jsonb({"tier":"E2","partition":partition,"measurements":dict(measurements)})))
            self._event(self.db,scope_id=scope_id,event_type="E2_WORKFLOW_OBSERVED",
                actor_id=created_by,payload={"observation_id":observation_id,
                    "source_refs":list(source_refs),"measurements":dict(measurements)})
        self.db.commit()
        return observation_id

    def propose(self, mutation: WorkflowMutation, *, proposer_id: str,
                idempotency_key: str) -> str:
        scope = self.db.execute("""SELECT p.policy_hash,p.allowed_paths,g.workflow_template,g.workflow_hash,
            g.scope_id,c.genome_id AS champion_id
            FROM evolution.e2_scope_policies p
            JOIN evolution.e2_workflow_genomes g ON g.scope_id=p.scope_id
            JOIN evolution.scope_champions c ON c.scope_id=p.scope_id AND c.genome_id=g.genome_id
            WHERE p.scope_id=%s AND g.genome_id=%s""",
            (mutation.scope_id,mutation.parent_genome_id)).fetchone()
        if not scope or scope["workflow_hash"] != mutation.parent_hash:
            raise E2PolicyError("mutation parent is absent, stale or hash-mismatched")
        if scope["allowed_paths"] != ["/nodes",*E2_STRUCTURAL_ALLOWLIST["mutable_node_fields"]]:
            raise E2PolicyError("persisted E2 structural allowlist differs from this validator")
        observation=self.db.execute("SELECT scope_id,source_refs,metadata FROM evolution.observations WHERE observation_id=%s",
                                    (mutation.observation_id,)).fetchone()
        if not observation or observation["scope_id"]!=mutation.scope_id:
            raise E2PolicyError("mutation must cite an existing observation in its E2 scope")
        if observation["metadata"].get("partition")!="DEVELOPMENT":
            raise E2PolicyError("mutation observations must use the frozen DEVELOPMENT partition")
        self._validate_development_source_refs(self.db,mutation.scope_id,observation["source_refs"])
        if mutation.rollback_genome_id != scope["champion_id"]:
            raise E2PolicyError("rollback target must be the current immutable champion")
        frozen=self.db.execute("SELECT scope_id FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
                                (mutation.evaluation_plan_ref,)).fetchone()
        if not frozen or frozen["scope_id"]!=mutation.scope_id:
            raise E2PolicyError("mutation must bind to an already-frozen evaluation pack in its scope")
        candidate = apply_workflow_mutation(scope["workflow_template"],mutation,
                                             persisted_parent_hash=scope["workflow_hash"])
        max_challengers=int(self.db.execute(
            "SELECT limits->>'max_challengers' AS n FROM evolution.e2_scope_policies WHERE scope_id=%s",
            (mutation.scope_id,)).fetchone()["n"])
        active_challengers=self.db.execute("""SELECT count(*) AS n FROM evolution.system_genomes
            WHERE scope_id=%s AND status='CHALLENGER'""",(mutation.scope_id,)).fetchone()["n"]
        if active_challengers>=max_challengers:
            raise E2PolicyError("bounded challenger portfolio is full")
        max_retained=int(self.db.execute(
            "SELECT limits->>'max_retained_candidates' AS n FROM evolution.e2_scope_policies WHERE scope_id=%s",
            (mutation.scope_id,)).fetchone()["n"])
        retained=self.db.execute("SELECT count(*) AS n FROM evolution.e2_workflow_genomes WHERE scope_id=%s",
                                 (mutation.scope_id,)).fetchone()["n"]-1
        if retained>=max_retained:
            raise E2PolicyError("bounded E2 candidate archive is full")
        proposal_value = {"mutation_id":mutation.mutation_id,"scope_id":mutation.scope_id,
            "parent_genome_id":mutation.parent_genome_id,"parent_hash":mutation.parent_hash,
            "observation_id":mutation.observation_id,
            "operations":list(mutation.operations),"rationale":mutation.rationale,
            "expected_effect":mutation.expected_effect,"risk":mutation.risk,
            "evaluation_plan_ref":mutation.evaluation_plan_ref,
            "rollback_genome_id":mutation.rollback_genome_id,"candidate_hash":digest(candidate)}
        proposal_hash=digest(proposal_value)
        prior=self.db.execute("SELECT mutation_id,proposal_hash FROM evolution.e2_mutation_proposals WHERE idempotency_key=%s",
                              (idempotency_key,)).fetchone()
        if prior:
            if prior["proposal_hash"]!=proposal_hash:
                raise E2PolicyError("idempotency key reused with changed mutation content")
            return prior["mutation_id"]
        max_campaigns=int(self.db.execute("SELECT limits->>'max_campaigns' AS n FROM evolution.e2_scope_policies WHERE scope_id=%s",
                                          (mutation.scope_id,)).fetchone()["n"])
        active=self.db.execute("""SELECT count(*) AS n FROM evolution.e2_mutation_proposals
            WHERE scope_id=%s AND status IN ('PROPOSED','MATERIALIZED','EVALUATING')""",
            (mutation.scope_id,)).fetchone()["n"]
        if active>=max_campaigns:
            raise E2PolicyError("bounded E2 campaign limit is full")
        campaign_id="e2campaign_"+mutation.mutation_id
        goal_id="e2goal_"+mutation.mutation_id
        with self.db.transaction():
            self.db.execute("""INSERT INTO evolution.mutation_proposals
                (mutation_id,scope_id,observation_refs,parent_genome_id,candidate_config_ref,
                 candidate_config_hash,hypothesis,expected_effect,created_by,status,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'PROPOSED',%s)""",
                (mutation.mutation_id,mutation.scope_id,_jsonb([mutation.observation_id]),
                 mutation.parent_genome_id,"sha256:"+digest(candidate),digest(candidate),
                 mutation.rationale,_jsonb({"expected_effect":mutation.expected_effect,
                                            "risk":mutation.risk,
                                            "evaluation_plan_ref":mutation.evaluation_plan_ref}),
                 proposer_id,_jsonb({"tier":"E2","proposal_hash":proposal_hash})))
            self.db.execute("""INSERT INTO runtime.campaigns
                (campaign_id,idempotency_key,request_hash,description,status,budget,max_children,created_by,metadata)
                VALUES (%s,%s,%s,'bounded E2 workflow evaluation','ACTIVE',%s,6,%s,%s)""",
                (campaign_id,"e2:"+idempotency_key,proposal_hash,
                _jsonb({"max_experiment_units":15,"max_wall_time_seconds":120,
                         "worker_sleep_seconds":12}),
                 proposer_id,_jsonb({"tier":"E2","mutation_id":mutation.mutation_id})))
            self.db.execute("""INSERT INTO runtime.goals
                (goal_id,campaign_id,status,description,priority,mission_ref,created_by,metadata)
                VALUES (%s,%s,'ACTIVE','Evaluate one bounded workflow-genome challenger',1,'e2-charter-v1',%s,%s)""",
                (goal_id,campaign_id,proposer_id,_jsonb({"scope_id":mutation.scope_id,"tier":"E2"})))
            self.db.execute("""INSERT INTO runtime.improvement_campaigns
                (campaign_id,scope_id,goal_id,status,budget,exploration_allocation,
                 exploitation_allocation,limits,created_by)
                VALUES (%s,%s,%s,'CREATED',%s,%s,%s,%s,%s)""",
                (campaign_id,mutation.scope_id,goal_id,
                 _jsonb({"max_experiment_units":15,"max_wall_time_seconds":120,
                         "worker_sleep_seconds":12}),
                 _jsonb({"experiment_units":15}),_jsonb({}),
                 _jsonb({"max_challengers":1,"max_evaluations":14,"max_attempts":40,
                         "max_campaign_children":0,"max_wall_time_seconds":120}),proposer_id))
            self.db.execute("""INSERT INTO evolution.e2_mutation_proposals
                (mutation_id,idempotency_key,campaign_id,scope_id,parent_genome_id,observation_id,parent_hash,operations,
                 rationale,expected_effect,risk,evaluation_plan_ref,rollback_genome_id,
                 proposal_hash,status,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PROPOSED',%s)""",
                (mutation.mutation_id,idempotency_key,campaign_id,mutation.scope_id,mutation.parent_genome_id,
                 mutation.observation_id,mutation.parent_hash,_jsonb(list(mutation.operations)),mutation.rationale,
                 mutation.expected_effect,mutation.risk,mutation.evaluation_plan_ref,
                 mutation.rollback_genome_id,proposal_hash,proposer_id))
            CampaignAccounting(self.db).reserve(campaign_id=campaign_id,stage="e2_challenger",
                idempotency_key="e2:"+mutation.mutation_id+":challenger",
                amounts={"experiment_units":1,"challengers":1},budget_class="EXPLORE")
            self._event(self.db,scope_id=mutation.scope_id,event_type="E2_MUTATION_PROPOSED",
                actor_id=proposer_id,mutation_id=mutation.mutation_id,
                payload={"proposal_hash":proposal_hash,"candidate_hash":digest(candidate)})
        self.db.commit()
        return mutation.mutation_id

    def materialize_challenger(self, *, mutation_id: str, genome_id: str,
                               created_by: str) -> str:
        row=self.db.execute("""SELECT m.*,base.candidate_config_hash,base.candidate_config_ref,
            g.workflow_template,g.workflow_hash,g.scope_id,
            s.genome_id AS champion_id FROM evolution.e2_mutation_proposals m
            JOIN evolution.mutation_proposals base ON base.mutation_id=m.mutation_id
            JOIN evolution.e2_workflow_genomes g ON g.genome_id=m.parent_genome_id
            JOIN evolution.scope_champions s ON s.scope_id=m.scope_id
            WHERE m.mutation_id=%s""",(mutation_id,)).fetchone()
        if (not row or row["status"]!="PROPOSED" or row["workflow_hash"]!=row["parent_hash"]
                or row["champion_id"]!=row["parent_genome_id"]
                or row["rollback_genome_id"]!=row["champion_id"]):
            raise E2PolicyError("proposal is not materializable against its current parent")
        mutation=WorkflowMutation(row["mutation_id"],row["scope_id"],row["parent_genome_id"],
            row["parent_hash"],row["observation_id"],row["rationale"],row["expected_effect"],row["risk"],
            row["evaluation_plan_ref"],row["rollback_genome_id"],tuple(row["operations"]))
        candidate=apply_workflow_mutation(row["workflow_template"],mutation,
                                           persisted_parent_hash=row["workflow_hash"])
        candidate_hash=digest(candidate)
        expected_proposal_hash=digest({"mutation_id":row["mutation_id"],"scope_id":row["scope_id"],
            "parent_genome_id":row["parent_genome_id"],"parent_hash":row["parent_hash"],
            "observation_id":row["observation_id"],"operations":list(row["operations"]),
            "rationale":row["rationale"],"expected_effect":row["expected_effect"],"risk":row["risk"],
            "evaluation_plan_ref":row["evaluation_plan_ref"],"rollback_genome_id":row["rollback_genome_id"],
            "candidate_hash":candidate_hash})
        if (row["candidate_config_hash"]!=candidate_hash
                or row["candidate_config_ref"]!="sha256:"+candidate_hash
                or row["proposal_hash"]!=expected_proposal_hash):
            raise E2PolicyError("candidate genome or delta hash differs from the immutable proposal")
        config_hash=self._pg_hash(self.db,candidate)
        # The existing canonical JSONB diff helper treats arrays atomically;
        # an coordination runtime DAG is held in one nodes array, so its exact changed path is
        # `nodes` regardless of which node-level JSON pointer was proposed.
        paths=["nodes"]
        scope_version=self.db.execute("SELECT coalesce(max(version::integer),0)+1 AS version FROM evolution.system_genomes WHERE scope_id=%s",
                                       (row["scope_id"],)).fetchone()["version"]
        version=str(scope_version)
        with self.db.transaction():
            self.db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
                 mutation_rationale,created_by,rollback_ref,metadata)
                VALUES (%s,%s,%s,'CHALLENGER',%s,%s,%s,%s,%s,%s,%s)""",
                (genome_id,row["scope_id"],version,"sha256:"+config_hash,config_hash,
                 "E2 workflow mutation "+mutation_id,row["rationale"],created_by,
                 row["rollback_genome_id"],_jsonb({"tier":"E2","mutation_id":mutation_id})))
            self.db.execute("""INSERT INTO evolution.e2_workflow_genomes
                (genome_id,scope_id,parent_genome_id,workflow_template,workflow_hash,changed_paths,provenance)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (genome_id,row["scope_id"],row["parent_genome_id"],_jsonb(candidate),config_hash,
                 _jsonb(paths),_jsonb({"mutation_id":mutation_id,"proposal_hash":row["proposal_hash"],
                                      "created_by":created_by})))
            accounting=CampaignAccounting(self.db)
            reservation=accounting.reserve(campaign_id=row["campaign_id"],stage="e2_challenger",
                idempotency_key="e2:"+mutation_id+":challenger",
                amounts={"experiment_units":1,"challengers":1},budget_class="EXPLORE")
            accounting.mark_dispatched(reservation)
            accounting.settle(reservation,actual={"experiment_units":1,"challengers":1},terminal_status="SUCCEEDED")
            self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",
                             (genome_id,row["parent_genome_id"]))
            self.db.execute("UPDATE evolution.e2_mutation_proposals SET status='MATERIALIZED' WHERE mutation_id=%s",
                            (mutation_id,))
            self._event(self.db,scope_id=row["scope_id"],event_type="E2_CHALLENGER_MATERIALIZED",
                actor_id=created_by,mutation_id=mutation_id,
                payload={"genome_id":genome_id,"workflow_hash":config_hash})
        self.db.commit()
        return genome_id

    def freeze_evaluation_pack(self, *, pack_id: str, scope_id: str, suite_id: str,
                               suite_version: str, evaluator_version: str,
                               workload_keys: Sequence[str],
                               workloads: Mapping[str, Mapping[str, Any]], repetitions: int,
                               holdout_scenarios: Mapping[str, Mapping[str, Any]],
                               created_by: str,
                               minimum_latency_improvement: float = 0.05) -> str:
        if len(set(workload_keys))<3 or repetitions<2 or repetitions>2:
            raise E2PolicyError("frozen deterministic evaluation requires at least three workloads and exactly two repetitions")
        if set(workloads)!=set(workload_keys):
            raise E2PolicyError("every workload key must bind one frozen workload definition")
        if not holdout_scenarios:
            raise E2PolicyError("an E2 suite requires at least one frozen HOLDOUT scenario")
        for key,value in workloads.items():
            if (set(value)!={"work_units"} or isinstance(value["work_units"],bool)
                    or not isinstance(value["work_units"],int) or not 1<=value["work_units"]<=5):
                raise E2PolicyError(f"workload {key} is outside the bounded generic deterministic workload contract")
        if set(workloads)&set(holdout_scenarios):
            raise E2PolicyError("DEVELOPMENT and HOLDOUT scenario keys must be disjoint")
        for key,value in holdout_scenarios.items():
            if (set(value)!={"work_units"} or isinstance(value["work_units"],bool)
                    or not isinstance(value["work_units"],int) or not 1<=value["work_units"]<=5):
                raise E2PolicyError(f"HOLDOUT scenario {key} is outside the bounded generic workload contract")
        if not 0 <= minimum_latency_improvement <= 1:
            raise E2PolicyError("minimum latency improvement must be a bounded fraction")
        db=self.governance_db or self.db
        scope_policy=db.execute("SELECT limits FROM evolution.e2_scope_policies WHERE scope_id=%s",
                                (scope_id,)).fetchone()
        if not scope_policy:
            raise E2PolicyError("unknown E2 evaluation scope")
        max_evals=int(scope_policy["limits"].get("max_evaluations_per_candidate",12))
        if len(set(workload_keys))*repetitions*2>max_evals:
            raise E2PolicyError("paired suite exceeds the frozen per-candidate evaluation limit")
        existing_pack=db.execute("SELECT pack_id FROM evolution.e2_evaluation_packs WHERE scope_id=%s",
                                 (scope_id,)).fetchone()
        if existing_pack:
            raise E2PolicyError("an E2 scope may freeze only one evaluation pack")
        suite=db.execute("""SELECT status,integrity_hash FROM evolution.eval_suite_versions
            WHERE eval_suite_id=%s AND version=%s AND scope_id=%s""",
            (suite_id,suite_version,scope_id)).fetchone()
        if not suite or suite["status"]!="AUTHORITATIVE":
            raise E2PolicyError("evaluation suite is not authoritative for this scope")
        scenarios=[]
        for partition,items in (("DEVELOPMENT",workloads),("HOLDOUT",holdout_scenarios)):
            for key,value in sorted(items.items()):
                scenario_id="e2scenario_"+digest({"pack_id":pack_id,"suite_id":suite_id,
                    "suite_version":suite_version,"partition":partition,"key":key})[:32]
                scenario_definition={"scenario_key":key,**dict(value)}
                scenarios.append((scenario_id,partition,key,scenario_definition,digest(scenario_definition)))
        definition={"workload_keys":sorted(set(workload_keys)),
            "workloads":{key:dict(workloads[key]) for key in sorted(workloads)},"repetitions":repetitions,
            "suite_id":suite_id,"suite_version":suite_version,"suite_hash":suite["integrity_hash"],
            "development_scenario_ids":sorted(item[0] for item in scenarios if item[1]=="DEVELOPMENT"),
            "holdout_scenario_ids":sorted(item[0] for item in scenarios if item[1]=="HOLDOUT"),
            "holdout_manifest_hash":digest(sorted((item[0],item[4]) for item in scenarios if item[1]=="HOLDOUT")),
            "evaluator_version":evaluator_version,"paired":True,
            "environment":"deterministic-worker-v1",
            "seeds":[{"workload_key":key,"repetition":repeat,
                      "seed":digest({"scope_id":scope_id,"suite_hash":suite["integrity_hash"],
                                     "workload_key":key,"repetition":repeat})}
                     for key in sorted(set(workload_keys)) for repeat in range(1,repetitions+1)],
            "minimum_latency_improvement":minimum_latency_improvement,
            "latency_repeatability_fraction":0.50,
            "postpromotion_latency_regression_fraction":0.05,
            "postpromotion_load_multiplier":5,
            "required_metrics":["success_rate","verifier_acceptance_rate","recovery_pass","latency_ms","retry_count"],
            "hard_gates":{"success_rate_nonregression":True,"verifier_nonregression":True,
                           "recovery_pass_required":True}}
        definition_hash=self._pg_hash(db,definition)
        with db.transaction():
            db.execute("""INSERT INTO evolution.e2_evaluation_packs
                (pack_id,scope_id,version,suite_id,suite_version,definition,definition_hash,evaluator_version,created_by)
                VALUES (%s,%s,(SELECT coalesce(max(version),0)+1 FROM evolution.e2_evaluation_packs WHERE scope_id=%s),
                    %s,%s,%s,%s,%s,%s)""",
                (pack_id,scope_id,scope_id,suite_id,suite_version,_jsonb(definition),definition_hash,
                 evaluator_version,created_by))
            for scenario_id,partition,key,value,scenario_hash in scenarios:
                db.execute("""INSERT INTO evolution.e2_evaluation_scenarios
                    (pack_id,scope_id,suite_id,suite_version,scenario_id,partition,definition,definition_hash,created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (pack_id,scope_id,suite_id,suite_version,scenario_id,partition,
                     _jsonb(value),scenario_hash,created_by))
        db.commit()
        return pack_id

    def record_evaluation(self, *, run_id: str, campaign_id: str, mutation_id: str,
                          candidate_genome_id: str, champion_genome_id: str, pack_id: str,
                          plan_version_id: str, recovery_event_id: str,
                          execution_evidence: Sequence[Mapping[str, Any]],
                          artifact_refs: Sequence[str],
                          side: str, workload_key: str, repetition: int, seed: str,
                          evaluator_id: str,
                          evidence_refs: Sequence[str], evidence_hash: str) -> str:
        db=(self.verifier_db if side=="HOLDOUT" else self.evaluator_db) or self.db
        authenticated=db.execute("SELECT session_user AS actor").fetchone()["actor"]
        if evaluator_id!=authenticated:
            raise E2PolicyError("partition evaluator identity must match the authenticated database principal")
        proposal=db.execute("SELECT created_by,scope_id,campaign_id,evaluation_plan_ref FROM evolution.e2_mutation_proposals WHERE mutation_id=%s",
                            (mutation_id,)).fetchone()
        if (not proposal or evaluator_id==proposal["created_by"] or proposal["campaign_id"]!=campaign_id or
                proposal["evaluation_plan_ref"]!=pack_id):
            raise E2PolicyError("evaluator must be independent from the proposer")
        mission=db.execute("""SELECT m.* FROM evolution.e2_evaluation_missions m
            WHERE m.run_id=%s AND m.mutation_id=%s AND m.campaign_id=%s AND m.genome_id=%s
              AND m.pack_id=%s AND m.side=%s AND m.workload_key=%s AND m.repetition=%s
              AND m.seed=%s AND m.plan_version_id=%s""",
            (run_id,mutation_id,campaign_id,champion_genome_id if side=="CHAMPION" else candidate_genome_id,
             pack_id,side,workload_key,repetition,seed,plan_version_id)).fetchone()
        if not mission:
            raise E2PolicyError("evaluation result lacks its immutable coordination runtime mission binding")
        pack=db.execute("SELECT definition FROM evolution.e2_evaluation_packs WHERE pack_id=%s AND scope_id=%s",
                        (pack_id,proposal["scope_id"])).fetchone()
        if not pack:
            raise E2PolicyError("run is outside the frozen evaluation pack")
        if len(evidence_hash)!=64 or any(ch not in "0123456789abcdef" for ch in evidence_hash):
            raise E2PolicyError("evaluation evidence hash must be SHA-256")
        if side=="HOLDOUT":
            scenario=db.execute("""SELECT scenario_id,partition,definition_hash,suite_id,suite_version
                FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND scope_id=%s
                  AND scenario_id=%s""",(pack_id,proposal["scope_id"],mission["scenario_id"])).fetchone()
            if (not scenario or scenario["partition"]!="HOLDOUT"
                    or scenario["scenario_id"] not in pack["definition"]["holdout_scenario_ids"]
                    or scenario["definition_hash"]!=mission["scenario_hash"]
                    or seed!=scenario["definition_hash"] or repetition!=1
                    or scenario["suite_id"]!=pack["definition"]["suite_id"]
                    or scenario["suite_version"]!=pack["definition"]["suite_version"]):
                raise E2PolicyError("HOLDOUT result does not match the frozen hidden scenario and suite version")
            if db.execute("""SELECT 1 FROM evolution.e2_evaluation_runs WHERE mutation_id=%s
                AND partition='DEVELOPMENT' AND evaluator_id=%s LIMIT 1""",
                (mutation_id,evaluator_id)).fetchone():
                raise E2PolicyError("HOLDOUT evaluator must be distinct from the development evaluator")
        else:
            if (mission["partition"]!="DEVELOPMENT" or workload_key not in pack["definition"]["workload_keys"]
                    or repetition not in range(1,pack["definition"]["repetitions"]+1)):
                raise E2PolicyError("run is outside the frozen DEVELOPMENT evaluation pack")
            expected_seed=next(item["seed"] for item in pack["definition"]["seeds"]
                               if item["workload_key"]==workload_key and item["repetition"]==repetition)
            if (seed!=expected_seed or mission["scenario_id"] not in pack["definition"]["development_scenario_ids"]):
                raise E2PolicyError("run seed or scenario is not frozen DEVELOPMENT input")
        if not evidence_refs:
            raise E2PolicyError("evaluation evidence references are absent")
        if (not artifact_refs or len(set(artifact_refs)) != len(artifact_refs)
                or not set(evidence_refs).issubset(set(artifact_refs))):
            raise E2PolicyError("evaluation must bind unique accepted coordination runtime artifacts and reference only those artifacts")
        # Bind the metric record to every node and verified artifact in the real
        # accepted coordination runtime plan graph, retaining per-attempt worker authority.
        graph=self.db.execute("""SELECT p.status AS plan_status,n.task_id,t.status AS task_status,
                t.result_refs,t.lease_epoch AS task_epoch
            FROM runtime.coordination_plan_versions p
            JOIN runtime.coordination_plan_nodes n ON n.plan_version_id=p.plan_version_id
            JOIN runtime.tasks t ON t.task_id=n.task_id
            WHERE p.plan_version_id=%s ORDER BY n.node_key""",(plan_version_id,)).fetchall()
        if (not graph or any(row["plan_status"]!="ACCEPTED" or row["task_status"]!="ACCEPTED"
                             for row in graph)):
            raise E2PolicyError("evaluation must bind to a fully accepted coordination runtime plan graph")
        if not execution_evidence or len({item.get("task_id") for item in execution_evidence})!=len(graph):
            raise E2PolicyError("evaluation evidence must cover every task in the coordination runtime plan graph")
        expected_tasks={row["task_id"]:row for row in graph}
        if {item.get("task_id") for item in execution_evidence}!=set(expected_tasks):
            raise E2PolicyError("evaluation evidence tasks do not match the frozen coordination runtime plan graph")
        evidence_artifacts=[]
        validated_evidence=[]
        for item in execution_evidence:
            required={"task_id","attempt_id","worker_id","worker_instance_id","lease_epoch","artifact_refs"}
            if set(item)!=required or not item["artifact_refs"]:
                raise E2PolicyError("coordination runtime execution evidence has missing or unsupported identity fields")
            task=expected_tasks[item["task_id"]]
            lease_epoch=int(item["lease_epoch"])
            if lease_epoch!=task["task_epoch"] or not set(item["artifact_refs"]).issubset(set(task["result_refs"] or [])):
                raise E2PolicyError("coordination runtime task evidence has a stale epoch or unaccepted artifact")
            attempt=self.db.execute("""SELECT status,task_id,worker_id,worker_instance_id,lease_epoch
                FROM runtime.attempts WHERE attempt_id=%s""",(item["attempt_id"],)).fetchone()
            if (not attempt or attempt["status"]!="ACCEPTED" or attempt["task_id"]!=item["task_id"]
                    or attempt["worker_id"]!=item["worker_id"]
                    or attempt["worker_instance_id"]!=item["worker_instance_id"]
                    or attempt["lease_epoch"]!=lease_epoch):
                raise E2PolicyError("coordination runtime attempt evidence does not match accepted worker authority")
            validated_evidence.append({**dict(item),"lease_epoch":lease_epoch})
            evidence_artifacts.extend(item["artifact_refs"])
        if set(evidence_artifacts)!=set(artifact_refs) or len(evidence_artifacts)!=len(set(evidence_artifacts)):
            raise E2PolicyError("evaluation artifact list does not exactly match coordination runtime task evidence")
        artifacts=self.db.execute("""SELECT artifact_id,producer_task_id,producer_attempt_id,
                verification_status,content_hash FROM runtime.artifacts
            WHERE artifact_id=ANY(%s)""",(list(artifact_refs),)).fetchall()
        if len(artifacts)!=len(artifact_refs) or any(
                a["verification_status"]!="VERIFIED"
                or a["artifact_id"] not in expected_tasks[a["producer_task_id"]]["result_refs"]
                or not any(e["task_id"]==a["producer_task_id"] and e["attempt_id"]==a["producer_attempt_id"]
                           for e in validated_evidence) for a in artifacts):
            raise E2PolicyError("evaluation artifact lineage is not verified against accepted coordination runtime attempts")
        if self.artifact_store is None:
            raise E2PolicyError("evaluation requires a configured coordination runtime artifact store for content verification")
        from agentic_runtime.artifacts.store import ArtifactIntegrityError
        for artifact in artifacts:
            try:
                content=self.artifact_store.read(artifact["artifact_id"])
            except (ArtifactIntegrityError,OSError,ValueError) as exc:
                raise E2PolicyError("evaluation artifact content failed integrity verification") from exc
            if hashlib.sha256(content).hexdigest()!=artifact["content_hash"]:
                raise E2PolicyError("evaluation artifact content hash differs from persisted coordination runtime evidence")
        if evidence_hash!=digest(sorted((a["artifact_id"],a["content_hash"]) for a in artifacts)):
            raise E2PolicyError("evaluation evidence hash does not match accepted artifact content hashes")
        recovery=db.execute("""SELECT event_type,occurred_at,payload FROM runtime.events
            WHERE event_id=%s""",(recovery_event_id,)).fetchone()
        if (not recovery or recovery["event_type"]!="RUNTIME_RECONCILIATION_COMPLETED"
                or recovery["occurred_at"]<mission["created_at"]):
            raise E2PolicyError("evaluation recovery reference is not a post-materialization coordination runtime reconciliation")
        graph_evidence=db.execute("""SELECT n.task_id,t.status,t.result_refs,t.lease_epoch,
                min(a.started_at) AS first_started,max(a.completed_at) FILTER (WHERE a.status='ACCEPTED') AS accepted_completed,
                count(a.attempt_id) AS attempt_count,
                coalesce(sum(extract(epoch from a.completed_at-a.started_at))
                    FILTER (WHERE a.completed_at IS NOT NULL),0) AS worker_seconds,
                bool_and(a.status IN ('ACCEPTED','ABANDONED','FAILED','FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED')) AS attempts_terminal
            FROM runtime.coordination_plan_nodes n
            JOIN runtime.tasks t USING(task_id)
            LEFT JOIN runtime.attempts a ON a.task_id=t.task_id
            WHERE n.plan_version_id=%s GROUP BY n.task_id,t.status,t.result_refs,t.lease_epoch""",
            (plan_version_id,)).fetchall()
        if not graph_evidence or any(row["status"]!="ACCEPTED" or not row["accepted_completed"]
                                     or not row["attempts_terminal"] for row in graph_evidence):
            raise E2PolicyError("coordination runtime plan graph has nonterminal or unaccepted execution evidence")
        active_lease=db.execute("""SELECT count(*) AS n FROM runtime.leases l
            JOIN runtime.coordination_plan_nodes n ON n.task_id=l.task_id
            WHERE n.plan_version_id=%s AND l.status='ACTIVE'""",(plan_version_id,)).fetchone()["n"]
        outbox_pending=db.execute("""SELECT count(*) AS n FROM runtime.outbox o
            JOIN runtime.events e USING(event_id) JOIN runtime.tasks t ON t.task_id=e.task_id
            WHERE t.plan_version_id=%s AND o.delivered_at IS NULL""",
            (plan_version_id,)).fetchone()["n"]
        rec=recovery["payload"]
        recovery_pass=(int(rec.get("orphaned_attempts",-1))==0
            and int(rec.get("tasks_without_current_lease",-1))==0
            and int(rec.get("pending_outbox",-1))==0
            and int(rec.get("incomplete_sandbox_cleanup",-1))==0
            and active_lease==0 and outbox_pending==0)
        total_attempts=sum(int(row["attempt_count"]) for row in graph_evidence)
        worker_seconds=sum(float(row["worker_seconds"] or 0) for row in graph_evidence)
        start=min(row["first_started"] for row in graph_evidence)
        end=max(row["accepted_completed"] for row in graph_evidence)
        verified_count=sum(1 for artifact in artifacts if artifact["verification_status"]=="VERIFIED")
        metrics={"success_rate":sum(row["status"]=="ACCEPTED" for row in graph_evidence)/len(graph_evidence),
            "verifier_acceptance_rate":verified_count/len(artifacts),"recovery_pass":recovery_pass,
            "latency_ms":max(0.0,(end-start).total_seconds()*1000.0),
            "retry_count":max(0,total_attempts-len(graph_evidence))}
        if not recovery_pass:
            raise E2PolicyError("persisted coordination runtime reconciliation did not prove clean recovery of the evaluation graph")
        reservation=self.db.execute("""SELECT reservation_id,status FROM runtime.improvement_reservations
            WHERE campaign_id=%s AND idempotency_key=%s""",(campaign_id,"e2:"+run_id)).fetchone()
        existing_run=db.execute("""SELECT campaign_id,mutation_id,candidate_genome_id,champion_genome_id,
                pack_id,plan_version_id,recovery_event_id,artifact_refs,side,partition,scenario_id,
                scenario_hash,workflow_hash,workload_key,repetition,
                seed,evaluator_id,metrics,evidence_refs,evidence_hash,status
            FROM evolution.e2_evaluation_runs WHERE run_id=%s""",(run_id,)).fetchone()
        expected_run={"campaign_id":campaign_id,"mutation_id":mutation_id,
            "candidate_genome_id":candidate_genome_id,"champion_genome_id":champion_genome_id,
            "pack_id":pack_id,"plan_version_id":plan_version_id,"recovery_event_id":recovery_event_id,
            "artifact_refs":list(artifact_refs),"side":side,"partition":mission["partition"],
            "scenario_id":mission["scenario_id"],"scenario_hash":mission["scenario_hash"],
            "workflow_hash":mission["workflow_hash"],"workload_key":workload_key,
            "repetition":repetition,"seed":seed,"evaluator_id":evaluator_id,"metrics":metrics,
            "evidence_refs":list(evidence_refs),"evidence_hash":evidence_hash,"status":"COMPLETED"}
        if existing_run:
            if dict(existing_run)!=expected_run:
                raise E2PolicyError("evaluation run identity was reused with different evidence")
            if reservation:
                CampaignAccounting(self.db).settle(reservation["reservation_id"],
                    actual={"experiment_units":1,"evaluations":1,"wall_time":worker_seconds},
                    terminal_status="SUCCEEDED")
                self.db.commit()
            return run_id
        if not reservation or reservation["status"]!="RESERVED":
            raise E2PolicyError("evaluation usage must be actively reserved before a run starts")
        with db.transaction():
            db.execute("""INSERT INTO evolution.e2_evaluation_runs
                (run_id,campaign_id,scope_id,mutation_id,candidate_genome_id,champion_genome_id,
                 pack_id,plan_version_id,recovery_event_id,artifact_refs,side,partition,scenario_id,
                 scenario_hash,workflow_hash,workload_key,repetition,seed,evaluator_id,metrics,evidence_refs,evidence_hash,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETED')""",
                (run_id,campaign_id,proposal["scope_id"],mutation_id,candidate_genome_id,
                 champion_genome_id,pack_id,plan_version_id,recovery_event_id,
                 _jsonb(list(artifact_refs)),side,mission["partition"],mission["scenario_id"],
                 mission["scenario_hash"],mission["workflow_hash"],workload_key,repetition,seed,evaluator_id,
                 _jsonb(dict(metrics)),_jsonb(list(evidence_refs)),evidence_hash))
            for item in validated_evidence:
                db.execute("""INSERT INTO evolution.e2_evaluation_run_evidence
                    (run_id,task_id,attempt_id,worker_id,worker_instance_id,lease_epoch,artifact_id)
                    SELECT %s,%s,%s,%s,%s,%s,x.artifact_id
                    FROM unnest(%s::text[]) AS x(artifact_id)""",
                    (run_id,item["task_id"],item["attempt_id"],item["worker_id"],
                     item["worker_instance_id"],item["lease_epoch"],list(item["artifact_refs"])))
        db.commit()
        CampaignAccounting(self.db).settle(reservation["reservation_id"],
                actual={"experiment_units":1,"evaluations":1,"wall_time":worker_seconds},
                terminal_status="SUCCEEDED")
        self.db.commit()
        return run_id

    def begin_evaluation(self, *, run_id: str, campaign_id: str) -> str:
        """Reserve and dispatch one bounded evaluation before the worker runs."""
        cap=self.db.execute("SELECT limits->>'max_wall_time_seconds' AS n FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                            (campaign_id,)).fetchone()
        if not cap:
            raise E2PolicyError("unknown E2 campaign")
        used=self.db.execute("""SELECT coalesce(sum(d.consumed) FILTER (WHERE r.status IN ('SETTLED','UNKNOWN')),0) AS consumed,
            coalesce(sum(d.reserved) FILTER (WHERE r.status='RESERVED'),0) AS reserved
            FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
            WHERE r.campaign_id=%s AND d.dimension='wall_time'""",(campaign_id,)).fetchone()
        wall_cap=float(cap["n"] or 120)-float(used["consumed"] or 0)-float(used["reserved"] or 0)
        if wall_cap<=0:
            raise E2PolicyError("E2 campaign wall-time budget is exhausted")
        evaluation_limit=self.db.execute("SELECT limits->>'max_evaluations' AS n FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                                         (campaign_id,)).fetchone()
        per_run_cap=max(1.0,float(cap["n"] or 120)/max(1,int(evaluation_limit["n"] or 12)))
        reservation_cap=min(wall_cap,per_run_cap)
        accounting=CampaignAccounting(self.db)
        reservation=accounting.reserve(campaign_id=campaign_id,stage="e2_evaluation",
            idempotency_key="e2:"+run_id,amounts={"experiment_units":1,"evaluations":1,"wall_time":reservation_cap},
            budget_class="EXPLORE")
        accounting.mark_dispatched(reservation)
        self.db.commit()
        return reservation

    def materialize_evaluation_mission(self, *, run_id: str, mutation_id: str,
            candidate_genome_id: str, champion_genome_id: str, pack_id: str,
            side: str, workload_key: str, repetition: int, seed: str,
            proposer_id: str, coordinator_id: str) -> dict[str, Any]:
        """Create one frozen E2 side as an ordinary accepted coordination runtime task graph.

        coordination runtime remains responsible for plan validation, task materialization,
        dependencies, dispatch, leases, attempts, worker execution and artifact
        acceptance. This method only binds a genome and frozen workload to that
        existing path.
        """
        if side not in {"CANDIDATE","CHAMPION","POSTPROMOTION","HOLDOUT"} or proposer_id==coordinator_id:
            raise E2PolicyError("coordination runtime evaluation mission requires a valid side and independent plan acceptance")
        row=self.db.execute("""SELECT m.campaign_id,m.scope_id,m.parent_genome_id,m.status,
                p.definition,p.definition_hash,g.genome_id,g.workflow_template,g.workflow_hash
            FROM evolution.e2_mutation_proposals m
            JOIN evolution.e2_evaluation_packs p ON p.pack_id=%s AND p.scope_id=m.scope_id
            JOIN evolution.e2_workflow_genomes g ON g.genome_id=%s AND g.scope_id=m.scope_id
            WHERE m.mutation_id=%s AND m.evaluation_plan_ref=%s""",
            (pack_id,champion_genome_id if side=="CHAMPION" else candidate_genome_id,
             mutation_id,pack_id)).fetchone()
        expected_genome=champion_genome_id if side=="CHAMPION" else candidate_genome_id
        allowed_status={"DECIDED"} if side=="POSTPROMOTION" else {"MATERIALIZED","EVALUATING"}
        if (not row or row["status"] not in allowed_status
                or row["genome_id"]!=expected_genome
                or (side=="CHAMPION" and row["parent_genome_id"]!=champion_genome_id)):
            raise E2PolicyError("E2 mission genome or mutation lineage is invalid")
        if side=="POSTPROMOTION" and not self.db.execute("""SELECT 1
            FROM evolution.e2_promotion_decisions d JOIN evolution.e2_promotion_authorizations a
              USING(authorization_id) WHERE a.mutation_id=%s AND d.promoted_genome_id=%s""",
            (mutation_id,candidate_genome_id)).fetchone():
            raise E2PolicyError("post-promotion mission requires a committed candidate promotion")
        definition=row["definition"]
        partition="HOLDOUT" if side=="HOLDOUT" else "DEVELOPMENT"
        scenario_db=self.verifier_db if side=="HOLDOUT" else self.db
        if scenario_db is None:
            raise E2PolicyError("HOLDOUT execution requires the separate verifier capability")
        scenario=scenario_db.execute("""SELECT scenario_id,partition,definition,definition_hash,
                suite_id,suite_version FROM evolution.e2_evaluation_scenarios
            WHERE pack_id=%s AND scope_id=%s AND partition=%s AND definition->>'scenario_key'=%s""",
            (pack_id,row["scope_id"],partition,workload_key)).fetchone()
        if not scenario or scenario["suite_id"]!=definition["suite_id"] or scenario["suite_version"]!=definition["suite_version"]:
            raise E2PolicyError("coordination runtime mission is outside the frozen suite partition")
        if side=="HOLDOUT":
            if scenario["scenario_id"] not in definition["holdout_scenario_ids"] or repetition!=1 or seed!=scenario["definition_hash"]:
                raise E2PolicyError("HOLDOUT mission identity differs from its frozen scenario")
            workload={"work_units":scenario["definition"]["work_units"]}
        else:
            if workload_key not in definition["workload_keys"] or repetition not in range(1,definition["repetitions"]+1):
                raise E2PolicyError("coordination runtime mission is outside the frozen DEVELOPMENT workload pack")
            frozen_seed=next(item["seed"] for item in definition["seeds"]
                             if item["workload_key"]==workload_key and item["repetition"]==repetition)
            if seed!=frozen_seed or scenario["scenario_id"] not in definition["development_scenario_ids"]:
                raise E2PolicyError("coordination runtime mission seed differs from frozen DEVELOPMENT input")
            workload=definition["workloads"][workload_key]
        reservation=self.db.execute("""SELECT status,dispatch_state FROM runtime.improvement_reservations
            WHERE campaign_id=%s AND idempotency_key=%s""",
            (row["campaign_id"],"e2:"+run_id)).fetchone()
        if not reservation or reservation["status"]!="RESERVED" or reservation["dispatch_state"]!="DISPATCHED":
            raise E2PolicyError("coordination runtime mission requires an active dispatched evaluation reservation")
        mission_db=(self.verifier_db if side=="HOLDOUT" else self.db)
        if mission_db is None:
            raise E2PolicyError("HOLDOUT mission idempotency requires the separate verifier capability")
        previous=mission_db.execute("""SELECT mission_id,plan_version_id,campaign_id,scope_id,mutation_id,
                genome_id,pack_id,side,partition,scenario_id,scenario_hash,workload_key,repetition,seed,workflow_hash
            FROM evolution.e2_evaluation_missions WHERE run_id=%s""",(run_id,)).fetchone()
        if previous:
            expected={"campaign_id":row["campaign_id"],"scope_id":row["scope_id"],
                "mutation_id":mutation_id,"genome_id":expected_genome,"pack_id":pack_id,
                "side":side,"partition":partition,"scenario_id":scenario["scenario_id"],
                "scenario_hash":scenario["definition_hash"],"workload_key":workload_key,"repetition":repetition,
                "seed":seed,"workflow_hash":row["workflow_hash"]}
            if any(previous[key]!=value for key,value in expected.items()):
                raise E2PolicyError("coordination runtime mission identity was reused with changed genome content")
            return {"mission_id":previous["mission_id"],"plan_version_id":previous["plan_version_id"],
                    "task_ids":{item["node_key"]:item["task_id"] for item in self.db.execute(
                        "SELECT node_key,task_id FROM runtime.coordination_plan_nodes WHERE plan_version_id=%s",
                        (previous["plan_version_id"],)).fetchall()}}

        import hashlib
        from dataclasses import replace
        from agentic_runtime.coordinator.service import Coordinator
        from agentic_runtime.coordinator.coordination import CoordinationService
        from agentic_runtime.evolution.e2 import _plan_from_dict
        plan_suffix=hashlib.sha256(run_id.encode()).hexdigest()[:24]
        mission_id="e2mission_"+plan_suffix
        plan_version_id="e2version_"+plan_suffix
        template=dict(row["workflow_template"])
        proposal=_plan_from_dict(template)
        multiplier=(int(definition["postpromotion_load_multiplier"]) if side=="POSTPROMOTION" else 1)
        sleep_seconds=round(float(workload["work_units"])*0.1*multiplier,3)
        nodes=tuple(replace(node,budget={**dict(node.budget),"worker_sleep_seconds":sleep_seconds})
                    for node in proposal.nodes)
        proposal=replace(proposal,plan_id="e2plan_"+plan_suffix,goal_id=mission_id,
            created_by=proposer_id,nodes=nodes,
            estimated_budget={**dict(proposal.estimated_budget),
                "worker_sleep_seconds":sleep_seconds*len(nodes)})
        with self.db.transaction():
            Coordinator(self.db).create_goal(mission_id,row["campaign_id"],
                description=f"Evaluate immutable E2 workflow {expected_genome} on frozen workload {workload_key}",
                mission_ref="e2-m9-evaluation-v1",created_by=proposer_id)
            service=CoordinationService(self.db)
            service.propose(plan_version_id,proposal)
            task_ids=service.accept(plan_version_id,accepted_by=coordinator_id)
            self.db.execute("""INSERT INTO evolution.e2_evaluation_missions
                (run_id,campaign_id,scope_id,mutation_id,genome_id,pack_id,side,partition,scenario_id,
                 scenario_hash,workload_key,repetition,seed,workflow_hash,mission_id,plan_version_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (run_id,row["campaign_id"],row["scope_id"],mutation_id,expected_genome,
                 pack_id,side,partition,scenario["scenario_id"],scenario["definition_hash"],
                 workload_key,repetition,seed,row["workflow_hash"],mission_id,plan_version_id))
            self._event(self.db,scope_id=row["scope_id"],campaign_id=row["campaign_id"],
                event_type="E2_M9_EVALUATION_MISSION_MATERIALIZED",actor_id=coordinator_id,
                mutation_id=mutation_id,payload={"run_id":run_id,"genome_id":expected_genome,
                    "workflow_hash":row["workflow_hash"],"side":side,"partition":partition,
                    "scenario_id":scenario["scenario_id"],"scenario_hash":scenario["definition_hash"],
                    "workload_key":workload_key,
                    "repetition":repetition,"seed":seed,"mission_id":mission_id,
                    "plan_version_id":plan_version_id,"task_ids":task_ids})
        self.db.commit()
        return {"mission_id":mission_id,"plan_version_id":plan_version_id,"task_ids":task_ids}

    def finalize_evaluation_mission(self, *, run_id: str, accepted_by: str) -> str:
        """Complete the ordinary coordination runtime goal after every accepted graph node settles."""
        mission_db=self.verifier_db or self.db
        row=mission_db.execute("""SELECT m.mission_id,m.plan_version_id,p.goal_id
            FROM evolution.e2_evaluation_missions m
            JOIN runtime.coordination_plan_versions p USING(plan_version_id)
            WHERE m.run_id=%s""",(run_id,)).fetchone()
        if not row:
            raise E2PolicyError("unknown E2 coordination runtime evaluation mission")
        nodes=self.db.execute("""SELECT n.node_key,n.task_id,t.status,t.result_refs
            FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
            WHERE n.plan_version_id=%s ORDER BY n.node_key""",(row["plan_version_id"],)).fetchall()
        if not nodes or any(item["status"]!="ACCEPTED" for item in nodes):
            raise E2PolicyError("coordination runtime evaluation mission graph has unsettled tasks")
        final=nodes[-1]
        refs=list(final["result_refs"] or [])
        if len(refs)!=1:
            raise E2PolicyError("coordination runtime evaluation mission final task must have one verified artifact")
        from agentic_runtime.coordinator.coordination import CoordinationService
        CoordinationService(self.db).accept_goal(row["goal_id"],final_task_id=final["task_id"],
            artifact_id=refs[0],accepted_by=accepted_by)
        self.db.commit()
        return refs[0]

    def reconcile_evaluation_accounting(self) -> int:
        """Settle completed run reservations left open by a process restart."""
        rows=self.db.execute("""SELECT r.reservation_id,r.campaign_id,r.idempotency_key,
                extract(epoch from now()-r.created_at) AS age_seconds
            FROM runtime.improvement_reservations r
            WHERE r.status='RESERVED' AND r.idempotency_key LIKE 'e2:%'
              AND r.dispatch_state='DISPATCHED'""").fetchall()
        settled=0
        result_db=self.verifier_db or self.db
        for row in rows:
            run_id=row["idempotency_key"][3:]
            run=result_db.execute("SELECT status,plan_version_id FROM evolution.e2_evaluation_runs WHERE run_id=%s",
                                  (run_id,)).fetchone()
            if run and run["status"]=="COMPLETED":
                wall=self.db.execute("""SELECT coalesce(sum(extract(epoch from a.completed_at-a.started_at))
                    FILTER (WHERE a.completed_at IS NOT NULL),0) AS seconds
                    FROM runtime.coordination_plan_nodes n JOIN runtime.attempts a USING(task_id)
                    WHERE n.plan_version_id=%s""",(run["plan_version_id"],)).fetchone()["seconds"]
                CampaignAccounting(self.db).settle(row["reservation_id"],
                    actual={"experiment_units":1,"evaluations":1,"wall_time":float(wall or 0)},
                    terminal_status="SUCCEEDED")
                settled+=1
            elif not run:
                mission_db=self.verifier_db or self.db
                mission=mission_db.execute("SELECT plan_version_id FROM evolution.e2_evaluation_missions WHERE run_id=%s",
                                           (run_id,)).fetchone()
                if mission:
                    state=self.db.execute("""SELECT count(*) AS total,
                            count(*) FILTER (WHERE t.status='ACCEPTED') AS accepted,
                            count(*) FILTER (WHERE t.status IN ('REJECTED','NEEDS_REVIEW','FAILED_TRANSIENT',
                                'FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED')) AS failed,
                            count(*) FILTER (WHERE t.status NOT IN ('ACCEPTED','REJECTED','NEEDS_REVIEW',
                                'FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','QUARANTINED','BUDGET_EXCEEDED')) AS active
                        FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
                        WHERE n.plan_version_id=%s""",(mission["plan_version_id"],)).fetchone()
                    if state["total"] and (state["accepted"]==state["total"] or state["active"]):
                        # A complete mission without evaluator record is resumable;
                        # active work remains owned by ordinary coordination runtime recovery.
                        continue
                elif float(row["age_seconds"])<30:
                    # Reservation and immutable coordination runtime mission are created in two
                    # transactions. Allow the normal binding window before
                    # treating a crash as an unrecoverable dispatched run.
                    continue
                CampaignAccounting(self.db).settle(row["reservation_id"],
                    actual={"experiment_units":None,"evaluations":None,"wall_time":None},
                    terminal_status="STALE")
                settled+=1
        self.db.commit()
        return settled

    def compare(self, *, comparison_id: str, mutation_id: str, candidate_genome_id: str,
                champion_genome_id: str, pack_id: str, evaluator_id: str) -> str:
        db=self.evaluator_db or self.db
        authenticated=db.execute("SELECT session_user AS actor").fetchone()["actor"]
        if evaluator_id!=authenticated:
            raise E2PolicyError("comparison identity must match the authenticated evaluator principal")
        proposal=db.execute("SELECT created_by,scope_id,status,evaluation_plan_ref FROM evolution.e2_mutation_proposals WHERE mutation_id=%s",
                            (mutation_id,)).fetchone()
        if not proposal or evaluator_id==proposal["created_by"] or proposal["evaluation_plan_ref"]!=pack_id:
            raise E2PolicyError("independent evaluator is required")
        pack=db.execute("SELECT definition,definition_hash FROM evolution.e2_evaluation_packs WHERE pack_id=%s AND scope_id=%s",
                        (pack_id,proposal["scope_id"])).fetchone()
        if not pack: raise E2PolicyError("unknown frozen evaluation pack")
        existing=db.execute("SELECT comparison_id FROM evolution.e2_comparisons WHERE mutation_id=%s",
                            (mutation_id,)).fetchone()
        if existing:
            if existing["comparison_id"]!=comparison_id:
                raise E2PolicyError("mutation already has an immutable comparison")
            return comparison_id
        expected={(key,repetition) for key in pack["definition"]["workload_keys"]
                  for repetition in range(1,pack["definition"]["repetitions"]+1)}
        rows=db.execute("""SELECT side,workload_key,repetition,seed,metrics,evaluator_id,run_id
            FROM evolution.e2_evaluation_runs WHERE mutation_id=%s AND pack_id=%s
            AND partition='DEVELOPMENT' AND candidate_genome_id=%s AND champion_genome_id=%s
            ORDER BY side,workload_key,repetition""",
            (mutation_id,pack_id,candidate_genome_id,champion_genome_id)).fetchall()
        by_side={side:{} for side in ("CANDIDATE","CHAMPION")}
        for row in rows:
            if (row["workload_key"],row["repetition"]) not in expected:
                raise E2PolicyError("evaluation contains work outside the frozen paired corpus")
            if row["evaluator_id"]!=evaluator_id:
                raise E2PolicyError("evaluation runs do not share the frozen independent evaluator")
            by_side[row["side"]][(row["workload_key"],row["repetition"])]=row
        if set(by_side["CANDIDATE"])!=expected or set(by_side["CHAMPION"])!=expected:
            raise E2PolicyError("incomplete paired repetitions cannot be compared")
        for key in expected:
            if by_side["CANDIDATE"][key]["seed"]!=by_side["CHAMPION"][key]["seed"]:
                raise E2PolicyError("candidate and champion workload seeds are not paired")
        for side in ("CANDIDATE","CHAMPION"):
            for workload in pack["definition"]["workload_keys"]:
                repetitions=[by_side[side][(workload,index)]["metrics"]
                             for index in range(1,pack["definition"]["repetitions"]+1)]
                stable=("success_rate","verifier_acceptance_rate","recovery_pass","retry_count")
                if any(any(item[key]!=repetitions[0][key] for key in stable) for item in repetitions[1:]):
                    raise E2PolicyError("deterministic repeated safety and quality metrics are not reproducible")
                latencies=[float(item["latency_ms"]) for item in repetitions]
                mean_latency=sum(latencies)/len(latencies)
                if mean_latency<=0 or (max(latencies)-min(latencies))/mean_latency>pack["definition"]["latency_repeatability_fraction"]:
                    raise E2PolicyError("measured repeated latency exceeds the frozen repeatability tolerance")
        names=("success_rate","verifier_acceptance_rate","latency_ms","retry_count")
        means={}
        for side in ("CANDIDATE","CHAMPION"):
            means[side]={name:sum(float(row["metrics"][name]) for row in by_side[side].values())/len(expected)
                         for name in names}
            means[side]["recovery_pass"]=all(bool(row["metrics"]["recovery_pass"]) for row in by_side[side].values())
        definition=pack["definition"]
        hard={"success_nonregression":means["CANDIDATE"]["success_rate"]>=means["CHAMPION"]["success_rate"],
              "verifier_nonregression":means["CANDIDATE"]["verifier_acceptance_rate"]>=means["CHAMPION"]["verifier_acceptance_rate"],
              "recovery_pass":means["CANDIDATE"]["recovery_pass"],
              "champion_recovery_pass":means["CHAMPION"]["recovery_pass"],
              "primary_improvement":means["CANDIDATE"]["latency_ms"]<=means["CHAMPION"]["latency_ms"]*(1-definition["minimum_latency_improvement"])}
        eligible=all(hard.values())
        evidence={"pack_hash":pack["definition_hash"],"runs":sorted(row["run_id"] for row in rows),
                  "metrics":means,"hard_gates":hard,"eligible":eligible}
        evidence_hash=self._pg_hash(db,evidence)
        with db.transaction():
            db.execute("""INSERT INTO evolution.e2_comparisons
                (comparison_id,scope_id,mutation_id,candidate_genome_id,champion_genome_id,
                 pack_id,evaluation_run_ids,hard_gates,comparison,eligible,evaluated_by,evidence_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (comparison_id,proposal["scope_id"],mutation_id,candidate_genome_id,champion_genome_id,
                 pack_id,_jsonb(evidence["runs"]),_jsonb(hard),_jsonb(means),eligible,evaluator_id,evidence_hash))
            self._event(db,scope_id=proposal["scope_id"],event_type="E2_EVALUATION_COMPARED",
                actor_id=evaluator_id,mutation_id=mutation_id,payload=evidence)
        db.commit()
        campaign_row=self.db.execute("SELECT campaign_id,status FROM evolution.e2_mutation_proposals WHERE mutation_id=%s",
                                     (mutation_id,)).fetchone()
        if not campaign_row or campaign_row["status"]!="MATERIALIZED":
            raise E2PolicyError("mutation lifecycle changed while comparison was being recorded")
        with self.db.transaction():
            if eligible:
                self.db.execute("UPDATE evolution.e2_mutation_proposals SET status='EVALUATING' WHERE mutation_id=%s",
                                (mutation_id,))
            else:
                self.db.execute("UPDATE evolution.system_genomes SET status='REJECTED' WHERE genome_id=%s AND status='CHALLENGER'",
                                (candidate_genome_id,))
                self.db.execute("UPDATE evolution.e2_mutation_proposals SET status='REJECTED' WHERE mutation_id=%s",
                                (mutation_id,))
                goal=self.db.execute("SELECT goal_id FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                                     (campaign_row["campaign_id"],)).fetchone()
                self._complete_campaign(self.db,campaign_row["campaign_id"],goal["goal_id"] if goal else None)
        self.db.commit()
        return comparison_id

    def authorize_and_promote(self, *, authorization_id: str, decision_id: str,
                              comparison_id: str, authorized_by: str) -> str:
        if self.governance_db is None or self.promotion_db is None:
            raise E2PolicyError("separate governance and promotion capabilities are required")
        gov=self.governance_db
        authenticated=gov.execute("SELECT session_user AS actor").fetchone()["actor"]
        if authorized_by!=authenticated:
            raise E2PolicyError("authorization identity must match the authenticated governance principal")
        comparison=gov.execute("SELECT mutation_id FROM evolution.e2_comparisons WHERE comparison_id=%s",
                              (comparison_id,)).fetchone()
        if not comparison:
            raise E2PolicyError("unknown E2 evaluation comparison")
        already=gov.execute("""SELECT a.authorization_id,d.decision_id,d.comparison_id
            FROM evolution.e2_promotion_authorizations a LEFT JOIN evolution.e2_promotion_decisions d
              ON d.authorization_id=a.authorization_id WHERE a.mutation_id=%s""",
            (comparison["mutation_id"],)).fetchone()
        if already:
            if already["authorization_id"]!=authorization_id or already["comparison_id"]!=comparison_id:
                raise E2PolicyError("mutation already has a different immutable promotion authorization")
            if already["decision_id"]:
                if already["decision_id"]==decision_id:
                    return decision_id
                raise E2PolicyError("promotion decision id conflicts with committed decision")
        row=gov.execute("""SELECT c.*,m.rollback_genome_id,m.created_by AS proposer,
            m.campaign_id,p.policy_hash,s.genome_id AS current_champion,g.version
            FROM evolution.e2_comparisons c
            JOIN evolution.e2_mutation_proposals m ON m.mutation_id=c.mutation_id
            JOIN evolution.e2_scope_policies p ON p.scope_id=c.scope_id
            JOIN evolution.scope_champions s ON s.scope_id=c.scope_id
            JOIN evolution.system_genomes g ON g.genome_id=c.candidate_genome_id
            WHERE c.comparison_id=%s""",(comparison_id,)).fetchone()
        if not row or not row["eligible"] or row["current_champion"]!=row["champion_genome_id"]:
            raise E2PolicyError("comparison is ineligible or champion baseline is stale")
        if authorized_by in {row["proposer"],row["evaluated_by"]}:
            raise E2PolicyError("promotion authority must be independent of proposer and evaluator")
        pack=gov.execute("SELECT definition FROM evolution.e2_evaluation_packs WHERE pack_id=%s",
                         (row["pack_id"],)).fetchone()
        candidate=gov.execute("SELECT workflow_hash,created_at FROM evolution.e2_workflow_genomes WHERE genome_id=%s",
                              (row["candidate_genome_id"],)).fetchone()
        required_holdouts=gov.execute("""SELECT scenario_id,definition_hash,suite_id,suite_version
            FROM evolution.e2_evaluation_scenarios WHERE pack_id=%s AND partition='HOLDOUT'""",
            (row["pack_id"],)).fetchall()
        holdout_runs=gov.execute("""SELECT r.scenario_id,r.scenario_hash,r.workflow_hash,r.evaluator_id,
                r.partition,r.status,r.metrics,r.created_at,m.created_at AS mission_created_at,
                s.definition_hash,s.suite_id,s.suite_version,p.suite_id AS pack_suite_id,
                p.suite_version AS pack_suite_version
            FROM evolution.e2_evaluation_runs r
            JOIN evolution.e2_evaluation_missions m USING(run_id)
            JOIN evolution.e2_evaluation_scenarios s ON s.pack_id=r.pack_id AND s.scenario_id=r.scenario_id
            JOIN evolution.e2_evaluation_packs p ON p.pack_id=r.pack_id
            WHERE r.mutation_id=%s AND r.pack_id=%s AND r.side='HOLDOUT'
              AND r.candidate_genome_id=%s""",
            (row["mutation_id"],row["pack_id"],row["candidate_genome_id"])).fetchall()
        actual_by_id={item["scenario_id"]:item for item in holdout_runs}
        if (not pack or not candidate or not required_holdouts
                or set(actual_by_id)!=set(item["scenario_id"] for item in required_holdouts)):
            raise E2PolicyError("promotion requires complete frozen HOLDOUT evaluation")
        for frozen in required_holdouts:
            result=actual_by_id[frozen["scenario_id"]]
            metrics=result["metrics"]
            if (result["partition"]!="HOLDOUT" or result["status"]!="COMPLETED"
                    or result["scenario_hash"]!=frozen["definition_hash"]
                    or result["definition_hash"]!=frozen["definition_hash"]
                    or result["workflow_hash"]!=candidate["workflow_hash"]
                    or result["evaluator_id"] in {row["proposer"],row["evaluated_by"],authorized_by}
                    or result["mission_created_at"]<candidate["created_at"]
                    or result["suite_id"]!=frozen["suite_id"] or result["suite_version"]!=frozen["suite_version"]
                    or result["suite_id"]!=result["pack_suite_id"]
                    or result["suite_version"]!=result["pack_suite_version"]
                    or float(metrics.get("success_rate",0))<1.0
                    or float(metrics.get("verifier_acceptance_rate",0))<1.0
                    or not bool(metrics.get("recovery_pass"))):
                raise E2PolicyError("HOLDOUT evidence is forged, stale, cross-suite, or failed a hard gate")
        if not already:
            with gov.transaction():
                gov.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"e2-promotion-authorization:{row['mutation_id']}",))
                # A competing governance request may have passed the earlier
                # read before this transaction acquired the mutation lock.
                current=gov.execute("""SELECT authorization_id,comparison_id
                    FROM evolution.e2_promotion_authorizations WHERE mutation_id=%s""",
                    (row["mutation_id"],)).fetchone()
                if current:
                    if (current["authorization_id"]!=authorization_id
                            or current["comparison_id"]!=comparison_id):
                        raise E2PolicyError("mutation already has a different immutable promotion authorization")
                else:
                    gov.execute("""INSERT INTO evolution.e2_promotion_authorizations
                        (authorization_id,scope_id,mutation_id,comparison_id,expected_champion_id,
                         candidate_genome_id,rollback_genome_id,policy_hash,authorized_by,status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'AUTHORIZED')""",
                        (authorization_id,row["scope_id"],row["mutation_id"],comparison_id,
                         row["champion_genome_id"],row["candidate_genome_id"],row["rollback_genome_id"],
                         row["policy_hash"],authorized_by))
            gov.commit()
        db=self.promotion_db
        promotion_actor=db.execute("SELECT session_user AS actor").fetchone()["actor"]
        if promotion_actor in {authorized_by,row["proposer"],row["evaluated_by"]}:
            raise E2PolicyError("promotion executor must be independent from governance, proposer and evaluator")
        with db.transaction():
            result=db.execute("SELECT evolution.execute_e2_promotion(%s,%s,%s) AS decision_id",
                              (authorization_id,decision_id,promotion_actor)).fetchone()
            if result["decision_id"]!=decision_id:
                raise E2PolicyError("promotion procedure returned an unexpected decision identity")
        db.commit()
        return decision_id

    def postpromotion_check(self, *, check_id: str, decision_id: str, run_id: str,
            recovery_event_id: str, execution_evidence: Sequence[Mapping[str, Any]],
            artifact_refs: Sequence[str], checked_by: str) -> bool:
        if self.verifier_db is None:
            raise E2PolicyError("post-promotion verifier capability is required")
        db=self.verifier_db
        actor=db.execute("SELECT session_user AS actor").fetchone()["actor"]
        if checked_by!=actor:
            raise E2PolicyError("post-promotion verifier identity must match authenticated principal")
        decision=db.execute("""SELECT d.scope_id,d.promoted_genome_id,a.authorized_by,
            a.mutation_id,c.evaluated_by,c.comparison,p.definition AS pack_definition,
            m.created_by AS proposer,m.campaign_id FROM evolution.e2_promotion_decisions d
            JOIN evolution.e2_promotion_authorizations a USING(authorization_id)
            JOIN evolution.e2_comparisons c ON c.comparison_id=d.comparison_id
            JOIN evolution.e2_evaluation_packs p ON p.pack_id=c.pack_id
            JOIN evolution.e2_mutation_proposals m ON m.mutation_id=a.mutation_id
            WHERE d.decision_id=%s""",(decision_id,)).fetchone()
        if not decision: raise E2PolicyError("unknown promotion decision")
        if checked_by in {decision["authorized_by"],decision["evaluated_by"],decision["proposer"]}:
            raise E2PolicyError("post-promotion verifier must be independent of proposal, evaluation and promotion")
        mission=db.execute("""SELECT * FROM evolution.e2_evaluation_missions
            WHERE run_id=%s AND mutation_id=%s AND side='POSTPROMOTION'
              AND genome_id=%s""",(run_id,decision["mutation_id"],decision["promoted_genome_id"])).fetchone()
        if not mission:
            raise E2PolicyError("post-promotion check requires a real coordination runtime mission for the promoted genome")
        if not artifact_refs or not execution_evidence:
            raise E2PolicyError("post-promotion check requires persisted coordination runtime task and artifact evidence")
        graph=db.execute("""SELECT p.status AS plan_status,n.task_id,t.status,t.result_refs,t.lease_epoch
            FROM runtime.coordination_plan_versions p
            JOIN runtime.coordination_plan_nodes n ON n.plan_version_id=p.plan_version_id
            JOIN runtime.tasks t USING(task_id) WHERE p.plan_version_id=%s""",
            (mission["plan_version_id"],)).fetchall()
        if (not graph or any(x["plan_status"]!="ACCEPTED" or x["status"]!="ACCEPTED" for x in graph)
                or {x["task_id"] for x in graph}!={x.get("task_id") for x in execution_evidence}):
            raise E2PolicyError("post-promotion coordination runtime graph is not fully and correctly accepted")
        expected={x["task_id"]:x for x in graph}; covered=[]
        validated=[]
        for item in execution_evidence:
            required={"task_id","attempt_id","worker_id","worker_instance_id","lease_epoch","artifact_refs"}
            if set(item)!=required or item["task_id"] not in expected or not item["artifact_refs"]:
                raise E2PolicyError("post-promotion worker evidence is malformed")
            if (int(item["lease_epoch"])!=expected[item["task_id"]]["lease_epoch"]
                    or not set(item["artifact_refs"]).issubset(set(expected[item["task_id"]]["result_refs"] or []))):
                raise E2PolicyError("post-promotion worker epoch or artifact reference is stale")
            attempt=db.execute("""SELECT task_id,status,worker_id,worker_instance_id,lease_epoch
                FROM runtime.attempts WHERE attempt_id=%s""",(item["attempt_id"],)).fetchone()
            if (not attempt or attempt["task_id"]!=item["task_id"] or attempt["status"]!="ACCEPTED"
                    or attempt["worker_id"]!=item["worker_id"]
                    or attempt["worker_instance_id"]!=item["worker_instance_id"]
                    or attempt["lease_epoch"]!=int(item["lease_epoch"])):
                raise E2PolicyError("post-promotion attempt is not accepted under the cited worker authority")
            validated.append({**dict(item),"lease_epoch":int(item["lease_epoch"])})
            covered.extend(item["artifact_refs"])
        if set(covered)!=set(artifact_refs) or len(covered)!=len(set(covered)):
            raise E2PolicyError("post-promotion artifact list does not match task evidence")
        artifacts=db.execute("""SELECT artifact_id,producer_task_id,producer_attempt_id,
                verification_status,content_hash FROM runtime.artifacts WHERE artifact_id=ANY(%s)""",
            (list(artifact_refs),)).fetchall()
        if len(artifacts)!=len(artifact_refs) or any(a["verification_status"]!="VERIFIED"
                or a["artifact_id"] not in expected[a["producer_task_id"]]["result_refs"]
                or not any(e["task_id"]==a["producer_task_id"] and e["attempt_id"]==a["producer_attempt_id"]
                           for e in validated) for a in artifacts):
            raise E2PolicyError("post-promotion artifacts are not verified coordination runtime outputs")
        if self.artifact_store is None:
            raise E2PolicyError("post-promotion check requires a configured coordination runtime artifact store")
        from agentic_runtime.artifacts.store import ArtifactIntegrityError
        for artifact in artifacts:
            try:
                content=self.artifact_store.read(artifact["artifact_id"])
            except (ArtifactIntegrityError,OSError,ValueError) as exc:
                raise E2PolicyError("post-promotion artifact content failed integrity verification") from exc
            if hashlib.sha256(content).hexdigest()!=artifact["content_hash"]:
                raise E2PolicyError("post-promotion artifact hash differs from persisted coordination runtime evidence")
        evidence_hash=digest(sorted((a["artifact_id"],a["content_hash"]) for a in artifacts))
        recovery=db.execute("""SELECT event_type,occurred_at,payload FROM runtime.events
            WHERE event_id=%s""",(recovery_event_id,)).fetchone()
        if (not recovery or recovery["event_type"]!="RUNTIME_RECONCILIATION_COMPLETED"
                or recovery["occurred_at"]<mission["created_at"]):
            raise E2PolicyError("post-promotion recovery evidence is not a post-mission coordination runtime reconciliation")
        times=db.execute("""SELECT min(a.started_at) AS started,max(a.completed_at) AS completed,
                count(*) AS attempts,
                coalesce(sum(extract(epoch from a.completed_at-a.started_at))
                    FILTER (WHERE a.completed_at IS NOT NULL),0) AS worker_seconds
            FROM runtime.coordination_plan_nodes n
            JOIN runtime.attempts a ON a.task_id=n.task_id
            WHERE n.plan_version_id=%s""",(mission["plan_version_id"],)).fetchone()
        active=db.execute("""SELECT count(*) AS n FROM runtime.leases l
            JOIN runtime.coordination_plan_nodes n ON n.task_id=l.task_id
            WHERE n.plan_version_id=%s AND l.status='ACTIVE'""",(mission["plan_version_id"],)).fetchone()["n"]
        recovery_payload=recovery["payload"]
        recovery_pass=(all(int(recovery_payload.get(key,-1))==0 for key in
            ("orphaned_attempts","tasks_without_current_lease","pending_outbox","incomplete_sandbox_cleanup"))
            and active==0)
        metrics={"success_rate":sum(x["status"]=="ACCEPTED" for x in graph)/len(graph),
            "verifier_acceptance_rate":sum(a["verification_status"]=="VERIFIED" for a in artifacts)/len(artifacts),
            "recovery_pass":recovery_pass,
            "latency_ms":max(0.0,(times["completed"]-times["started"]).total_seconds()*1000.0),
            "retry_count":max(0,int(times["attempts"])-len(graph))}
        comparison=decision["comparison"]
        baseline=comparison["CHAMPION"]
        candidate_baseline=comparison["CANDIDATE"]
        hard=(float(metrics["success_rate"])>=float(baseline["success_rate"])
              and float(metrics["verifier_acceptance_rate"])>=float(baseline["verifier_acceptance_rate"])
              and metrics["recovery_pass"]
              and float(metrics["latency_ms"])<=float(candidate_baseline["latency_ms"])*
                  (1+float(decision["pack_definition"]["postpromotion_latency_regression_fraction"])))
        passed=hard
        duplicate=db.execute("SELECT check_id,plan_version_id,recovery_event_id,artifact_refs,passed,metrics,evidence_hash FROM evolution.e2_postpromotion_checks WHERE decision_id=%s",
                             (decision_id,)).fetchone()
        if duplicate:
            if (duplicate["check_id"],duplicate["plan_version_id"],duplicate["recovery_event_id"],
                duplicate["artifact_refs"],duplicate["passed"],duplicate["metrics"],duplicate["evidence_hash"]) == (
                check_id,mission["plan_version_id"],recovery_event_id,list(artifact_refs),passed,metrics,evidence_hash):
                if not duplicate["passed"]:
                    if self.promotion_db is None:
                        raise E2PolicyError("independent promotion capability is required for automatic rollback")
                    promoter=self.promotion_db.execute("SELECT session_user AS actor").fetchone()["actor"]
                    self._rollback(self.promotion_db,decision_id=decision_id,scope_id=decision["scope_id"],
                        failed_genome_id=decision["promoted_genome_id"],reason="post-promotion regression",
                        actor_id=promoter)
                db.commit()
                # A process/database failure may occur after the durable failed
                # post-check is written but before campaign completion/accounting.
                # The same immutable replay must finish those effects idempotently.
                campaign_row=self.db.execute("SELECT goal_id FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                                             (decision["campaign_id"],)).fetchone()
                self._complete_campaign(self.db,decision["campaign_id"],
                    campaign_row["goal_id"] if campaign_row else None)
                self.db.commit()
                reservation=self.db.execute("SELECT reservation_id FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",
                    (decision["campaign_id"],"e2:"+run_id)).fetchone()
                if reservation:
                    CampaignAccounting(self.db).settle(reservation["reservation_id"],
                        actual={"experiment_units":1,"evaluations":1,"wall_time":float(times["worker_seconds"] or 0)},
                        terminal_status="SUCCEEDED" if duplicate["passed"] else "FAILED")
                    self.db.commit()
                return bool(duplicate["passed"])
            raise E2PolicyError("post-promotion check already has immutable evidence")
        with db.transaction():
            db.execute("""INSERT INTO evolution.e2_postpromotion_checks
                (check_id,decision_id,plan_version_id,recovery_event_id,artifact_refs,passed,metrics,evidence_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (check_id,decision_id,mission["plan_version_id"],recovery_event_id,
                 _jsonb(list(artifact_refs)),passed,_jsonb(metrics),evidence_hash))
            for item in validated:
                db.execute("""INSERT INTO evolution.e2_postpromotion_check_evidence
                    (check_id,task_id,attempt_id,worker_id,worker_instance_id,lease_epoch,artifact_id)
                    SELECT %s,%s,%s,%s,%s,%s,x.artifact_id
                    FROM unnest(%s::text[]) AS x(artifact_id)""",
                    (check_id,item["task_id"],item["attempt_id"],item["worker_id"],
                     item["worker_instance_id"],item["lease_epoch"],list(item["artifact_refs"])))
        db.commit()
        self._event(db,scope_id=decision["scope_id"],campaign_id=decision["campaign_id"],
            event_type="E2_POSTPROMOTION_CHECK_RECORDED",actor_id=checked_by,
            payload={"check_id":check_id,"decision_id":decision_id,
                "plan_version_id":mission["plan_version_id"],"passed":passed,
                "metrics":metrics,"evidence_hash":evidence_hash})
        db.commit()
        if not passed:
            if self.promotion_db is None:
                raise E2PolicyError("independent promotion capability is required for automatic rollback")
            promoter=self.promotion_db.execute("SELECT session_user AS actor").fetchone()["actor"]
            self._rollback(self.promotion_db,decision_id=decision_id,scope_id=decision["scope_id"],
                failed_genome_id=decision["promoted_genome_id"],reason="post-promotion regression",
                actor_id=promoter)
        campaign=self.db.execute("SELECT goal_id FROM runtime.improvement_campaigns WHERE campaign_id=%s",
                                 (decision["campaign_id"],)).fetchone()
        self._complete_campaign(self.db,decision["campaign_id"],campaign["goal_id"] if campaign else None)
        self.db.commit()
        reservation=self.db.execute("SELECT reservation_id FROM runtime.improvement_reservations WHERE campaign_id=%s AND idempotency_key=%s",
                                    (decision["campaign_id"],"e2:"+run_id)).fetchone()
        if reservation:
            CampaignAccounting(self.db).settle(reservation["reservation_id"],
                actual={"experiment_units":1,"evaluations":1,"wall_time":float(times["worker_seconds"] or 0)},
                terminal_status="SUCCEEDED" if passed else "FAILED")
            self.db.commit()
        return passed

    def _rollback(self, db: Any, *, decision_id: str, scope_id: str,
                  failed_genome_id: str, reason: str, actor_id: str) -> str:
        if db is not self.promotion_db:
            raise E2PolicyError("rollback must use the separate promotion capability")
        authenticated=db.execute("SELECT session_user AS actor").fetchone()["actor"]
        if actor_id!=authenticated:
            raise E2PolicyError("rollback actor must match the authenticated promotion principal")
        rollback_id=_id("e2rollback")
        result=db.execute("SELECT evolution.execute_e2_rollback(%s,%s,%s) AS rollback_id",
                          (decision_id,rollback_id,authenticated)).fetchone()
        db.commit()
        return result["rollback_id"]
