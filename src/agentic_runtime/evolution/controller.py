from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Mapping

from agentic_runtime.persistence.events import jsonb as _jsonb


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def pareto_dominates(candidate: Mapping[str, float], champion: Mapping[str, float],
                     directions: Mapping[str, str]) -> bool:
    """Return true only when all governed dimensions are no worse and one is better."""
    if set(candidate) != set(champion) or set(candidate) != set(directions):
        raise ValueError("candidate, champion, and policy must use identical metric dimensions")
    if any(direction not in {"MAX", "MIN"} for direction in directions.values()):
        raise ValueError("metric directions must be declared by the evaluation policy")
    no_worse = all((candidate[key] >= champion[key] if direction == "MAX"
                    else candidate[key] <= champion[key]) for key,direction in directions.items())
    better = any((candidate[key] > champion[key] if direction == "MAX"
                  else candidate[key] < champion[key]) for key,direction in directions.items())
    return no_worse and better


class EvolutionController:
    """Persists challenger evaluation and SHADOW decisions; never edits a champion."""

    MODE = "SHADOW"

    def __init__(self, connection: Any) -> None:
        self.db = connection

    def register_scope_champion(self, *, scope_id: str, description: str, genome_id: str,
                                version: str, config_ref: str, config: Mapping[str, Any],
                                created_by: str = "bootstrap") -> str:
        digest = _hash(config)
        with self.db.transaction():
            self.db.execute("INSERT INTO evolution.scopes(scope_id,description) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (scope_id,description))
            self.db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
                 mutation_rationale,created_by)
                VALUES (%s,%s,%s,'CHAMPION',%s,%s,'initial champion','initial governed baseline',%s)""",
                (genome_id,scope_id,version,config_ref,digest,created_by))
            self.db.execute("INSERT INTO evolution.scope_champions(scope_id,genome_id) VALUES (%s,%s)",
                            (scope_id,genome_id))
            self._event("CHAMPION_REGISTERED",scope_id,genome_id,{"config_hash": digest},created_by)
        return genome_id

    def _event(self, event_type: str, scope_id: str, actor_id: str,
               payload: Mapping[str, Any], created_by: str, causation_id: str | None = None) -> str:
        event_id = _id("eev")
        self.db.execute("""INSERT INTO evolution.events
            (event_id,event_type,scope_id,actor_type,actor_id,causation_id,correlation_id,
             schema_version,payload,payload_hash)
            VALUES (%s,%s,%s,'RUNTIME',%s,%s,%s,'1',%s,%s)""",
            (event_id,event_type,scope_id,actor_id,causation_id,scope_id,
             _jsonb(dict(payload)),_hash(payload)))
        return event_id

    def register_eval_suite(self, *, eval_suite_id: str, version: str, scope_id: str,
                            definition_ref: str, integrity_hash: str,
                            created_by: str, supersedes: tuple[str, str] | None = None,
                            change_proposal_id: str | None = None,
                            metric_directions: Mapping[str, str]) -> None:
        """Register a governed suite version; callers must independently review it first."""
        if not metric_directions or any(direction not in {"MAX","MIN"} for direction in metric_directions.values()):
            raise ValueError("governed evaluation suite must declare valid metric directions")
        with self.db.transaction():
            row = self.db.execute("""SELECT status FROM evolution.eval_suite_versions
                WHERE eval_suite_id=%s AND version=%s""", (eval_suite_id,version)).fetchone()
            if row:
                raise ValueError("evaluation suite versions are immutable and cannot be replaced")
            if supersedes:
                if not change_proposal_id:
                    raise ValueError("successor suite requires a separately validated change proposal")
                proposal = self.db.execute("""SELECT status,independent_reviewer,authorized_by,created_by,validation_refs,
                    current_eval_suite_id,current_eval_suite_version,scope_id
                    FROM evolution.eval_suite_change_proposals WHERE change_id=%s FOR UPDATE""",
                    (change_proposal_id,)).fetchone()
                if (not proposal or proposal["status"] != "AUTHORIZED"
                        or not proposal["independent_reviewer"]
                        or proposal["independent_reviewer"] == proposal["created_by"]
                        or proposal["authorized_by"] in {None,proposal["created_by"],proposal["independent_reviewer"]}
                        or created_by in {proposal["created_by"],proposal["independent_reviewer"]}
                        or not proposal["validation_refs"]
                        or (proposal["current_eval_suite_id"],proposal["current_eval_suite_version"]) != supersedes
                        or proposal["scope_id"] != scope_id):
                    raise ValueError("suite successor lacks independent validation and authorization")
                prior = self.db.execute("""SELECT status,scope_id FROM evolution.eval_suite_versions
                    WHERE eval_suite_id=%s AND version=%s FOR UPDATE""", supersedes).fetchone()
                if not prior or prior["status"] != "AUTHORITATIVE" or prior["scope_id"] != scope_id:
                    raise ValueError("successor suite must supersede the current authoritative suite in scope")
                self.db.execute("UPDATE evolution.eval_suite_versions SET status='SUPERSEDED' WHERE eval_suite_id=%s AND version=%s",
                                supersedes)
            self.db.execute("""INSERT INTO evolution.eval_suite_versions
                (eval_suite_id,version,scope_id,status,definition_ref,integrity_hash,
                 supersedes_eval_suite_id,supersedes_version,created_by,metadata)
                VALUES (%s,%s,%s,'AUTHORITATIVE',%s,%s,%s,%s,%s,%s)""",
                (eval_suite_id,version,scope_id,definition_ref,integrity_hash,
                 supersedes[0] if supersedes else None,supersedes[1] if supersedes else None,
                 created_by,_jsonb({"metric_directions":dict(metric_directions)})))
            self._event("EVAL_SUITE_VERSION_REGISTERED",scope_id,created_by,
                        {"eval_suite_id":eval_suite_id,"version":version,
                         "integrity_hash":integrity_hash,"supersedes":supersedes},created_by)

    def propose_eval_suite_change(self, *, change_id: str, scope_id: str,
                                  observation_refs: list[str], current_eval_suite_id: str,
                                  current_eval_suite_version: str, proposed_definition_ref: str,
                                  proposed_integrity_hash: str, rationale: str,
                                  created_by: str) -> str:
        """Record a separate suite-change proposal; it is not authoritative by itself."""
        with self.db.transaction():
            self.db.execute("""INSERT INTO evolution.eval_suite_change_proposals
                (change_id,scope_id,observation_refs,current_eval_suite_id,current_eval_suite_version,
                 proposed_definition_ref,proposed_integrity_hash,rationale,status,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'PROPOSED',%s)""",
                (change_id,scope_id,_jsonb(observation_refs),current_eval_suite_id,
                 current_eval_suite_version,proposed_definition_ref,proposed_integrity_hash,
                 rationale,created_by))
            self._event("EVAL_SUITE_CHANGE_PROPOSED",scope_id,created_by,
                        {"change_id":change_id,"current_suite":[current_eval_suite_id,current_eval_suite_version],
                         "proposed_integrity_hash":proposed_integrity_hash},created_by)
        return change_id

    def validate_eval_suite_change(self, *, change_id: str, reviewer_id: str,
                                   validation_refs: list[str]) -> None:
        if not validation_refs:
            raise ValueError("independent validation evidence is required")
        with self.db.transaction():
            row = self.db.execute("SELECT status,created_by,scope_id FROM evolution.eval_suite_change_proposals WHERE change_id=%s FOR UPDATE",
                                  (change_id,)).fetchone()
            if not row or row["status"] != "PROPOSED" or row["created_by"] == reviewer_id:
                raise ValueError("suite change requires an independent reviewer")
            self.db.execute("UPDATE evolution.eval_suite_change_proposals SET status='INDEPENDENT_REVIEW',independent_reviewer=%s WHERE change_id=%s",
                            (reviewer_id,change_id))
            self.db.execute("UPDATE evolution.eval_suite_change_proposals SET status='VALIDATED',validation_refs=%s WHERE change_id=%s",
                            (_jsonb(validation_refs),change_id))
            self._event("EVAL_SUITE_CHANGE_VALIDATED",row["scope_id"],reviewer_id,
                        {"change_id":change_id,"validation_refs":validation_refs},reviewer_id)

    def authorize_eval_suite_change(self, *, change_id: str, authority_id: str) -> None:
        with self.db.transaction():
            row = self.db.execute("SELECT status,created_by,independent_reviewer,scope_id FROM evolution.eval_suite_change_proposals WHERE change_id=%s FOR UPDATE",
                                  (change_id,)).fetchone()
            if (not row or row["status"] != "VALIDATED"
                    or authority_id in {row["created_by"],row["independent_reviewer"]}):
                raise ValueError("suite change needs a distinct governance authority")
            self.db.execute("UPDATE evolution.eval_suite_change_proposals SET status='AUTHORIZED',authorized_by=%s WHERE change_id=%s",
                            (authority_id,change_id))
            self._event("EVAL_SUITE_CHANGE_AUTHORIZED",row["scope_id"],authority_id,
                        {"change_id":change_id},authority_id)

    def run_shadow(self, *, scope_id: str, observation_id: str, observation_refs: list[str],
                   observation: str, mutation_id: str, candidate_genome_id: str,
                   candidate_version: str, candidate_config_ref: str,
                   candidate_config: Mapping[str, Any], hypothesis: str,
                   expected_effect: Mapping[str, Any],
                   evaluator_version: str, conditions_ref: str,
                   evaluation_result_refs: list[str],
                   champion_metrics: Mapping[str, float], candidate_metrics: Mapping[str, float],
                   evaluator_id: str, proposer_id: str, rationale_ref: str,
                   created_by: str = "evolution-controller",
                   retain_for_diversity: bool = False) -> dict[str, str]:
        """Bind the active suite, record equivalent-condition metrics and decide in shadow."""
        config_hash = _hash(candidate_config)
        if not conditions_ref or not evaluation_result_refs:
            raise ValueError("evaluation requires persisted conditions and result evidence references")
        evaluation_id, decision_id = _id("eval"), _id("pdec")
        with self.db.transaction():
            scope = self.db.execute("""SELECT c.genome_id FROM evolution.scope_champions c
                WHERE c.scope_id=%s FOR UPDATE""", (scope_id,)).fetchone()
            if not scope:
                raise ValueError("scope has no registered champion")
            champion_id = scope["genome_id"]
            suite = self.db.execute("""SELECT eval_suite_id,version,integrity_hash,metadata
                FROM evolution.eval_suite_versions WHERE scope_id=%s AND status='AUTHORITATIVE' FOR SHARE""",
                (scope_id,)).fetchone()
            if not suite:
                raise ValueError("scope has no authoritative evaluation suite")
            eval_suite_id, eval_suite_version = suite["eval_suite_id"], suite["version"]
            if evaluator_id == proposer_id or not evaluator_id:
                raise ValueError("candidate proposer cannot be the sole evaluator")
            if created_by in {proposer_id,evaluator_id}:
                raise ValueError("shadow decision authority must be distinct from proposer and evaluator")
            self.db.execute("""INSERT INTO evolution.observations
                (observation_id,scope_id,source_refs,summary,created_by) VALUES (%s,%s,%s,%s,%s)""",
                (observation_id,scope_id,_jsonb(observation_refs),observation,proposer_id))
            self.db.execute("""INSERT INTO evolution.mutation_proposals
                (mutation_id,scope_id,observation_refs,parent_genome_id,candidate_config_ref,
                 candidate_config_hash,hypothesis,expected_effect,created_by,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'REGISTERED')""",
                (mutation_id,scope_id,_jsonb([observation_id]),champion_id,candidate_config_ref,
                 config_hash,hypothesis,_jsonb(dict(expected_effect)),proposer_id))
            self.db.execute("""INSERT INTO evolution.system_genomes
                (genome_id,scope_id,version,status,config_ref,config_hash,mutation_description,
                 mutation_rationale,created_by,metadata)
                VALUES (%s,%s,%s,'CHALLENGER',%s,%s,%s,%s,%s,%s)""",
                (candidate_genome_id,scope_id,candidate_version,candidate_config_ref,config_hash,
                 hypothesis,observation,proposer_id,_jsonb({"mutation_id":mutation_id})))
            self.db.execute("INSERT INTO evolution.genome_parents(genome_id,parent_genome_id) VALUES (%s,%s)",
                            (candidate_genome_id,champion_id))
            bound = self.db.execute("""SELECT status,integrity_hash FROM evolution.eval_suite_versions
                WHERE eval_suite_id=%s AND version=%s AND scope_id=%s""",
                (eval_suite_id,eval_suite_version,scope_id)).fetchone()
            if not bound or bound["status"] != "AUTHORITATIVE" or bound["integrity_hash"] != suite["integrity_hash"]:
                raise ValueError("cannot select or weaken the bound evaluation suite")
            metrics = {"champion": dict(champion_metrics), "candidate": dict(candidate_metrics),
                       "conditions_equivalent": True}
            self.db.execute("""INSERT INTO evolution.evaluation_records
                (evaluation_id,scope_id,candidate_genome_id,champion_genome_id,eval_suite_id,
                 eval_suite_version,conditions_ref,evaluator_version,evaluator_id,result_refs,metrics,status,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETED',now())""",
                (evaluation_id,scope_id,candidate_genome_id,champion_id,eval_suite_id,
                 eval_suite_version,conditions_ref,evaluator_version,evaluator_id,_jsonb(evaluation_result_refs),
                 _jsonb(metrics)))
            directions = dict((suite["metadata"] or {}).get("metric_directions", {}))
            wins = pareto_dominates(candidate_metrics,champion_metrics,directions)
            if wins:
                decision = "WOULD_PROMOTE"
                genome_status = "CHALLENGER"
            elif retain_for_diversity:
                decision = "RETAIN_FOR_DIVERSITY"
                genome_status = "RETAINED_FOR_DIVERSITY"
            else:
                decision = "REJECT"
                genome_status = "REJECTED"
            self.db.execute("UPDATE evolution.system_genomes SET status=%s WHERE genome_id=%s",
                            (genome_status,candidate_genome_id))
            self.db.execute("""INSERT INTO evolution.promotion_decisions
                (decision_id,scope_id,candidate_genome_id,champion_genome_id,evaluation_refs,
                 decision,mode,rationale_ref,created_by)
                VALUES (%s,%s,%s,%s,%s,%s,'SHADOW',%s,%s)""",
                (decision_id,scope_id,candidate_genome_id,champion_id,_jsonb([evaluation_id]),
                 decision,rationale_ref,created_by))
            self._event("SHADOW_PROMOTION_DECIDED",scope_id,decision_id,
                        {"candidate_genome_id":candidate_genome_id,"champion_genome_id":champion_id,
                         "evaluation_id":evaluation_id,"decision":decision,"mode":"SHADOW"},
                        created_by)
            # Intentional invariant: champion authority changes only through the governed promotion path.
            return {"champion_genome_id":champion_id,"candidate_genome_id":candidate_genome_id,
                    "evaluation_id":evaluation_id,"decision_id":decision_id,"decision":decision,
                    "mode":"SHADOW"}
