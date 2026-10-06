-- M10: durable E2 workflow-genome lifecycle. E1 records and policy are unchanged.
CREATE TABLE evolution.e2_scope_policies (
    scope_id text PRIMARY KEY REFERENCES evolution.scopes(scope_id),
    policy_version text NOT NULL,
    allowed_paths jsonb NOT NULL CHECK (allowed_paths='["/nodes","depends_on","dependency_requirements","parent_node_key","required_capabilities","verifier_capabilities","budget"]'::jsonb),
    limits jsonb NOT NULL,
    evaluation_policy jsonb NOT NULL,
    policy_hash char(64) NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL
);

CREATE TABLE evolution.e2_workflow_genomes (
    genome_id text PRIMARY KEY REFERENCES evolution.system_genomes(genome_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    tier text NOT NULL DEFAULT 'E2' CHECK (tier='E2'),
    parent_genome_id text REFERENCES evolution.e2_workflow_genomes(genome_id),
    workflow_template jsonb NOT NULL CHECK (jsonb_typeof(workflow_template)='object'),
    workflow_hash char(64) NOT NULL CHECK (workflow_hash ~ '^[0-9a-f]{64}$'),
    changed_paths jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(changed_paths)='array'),
    provenance jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(genome_id,scope_id),
    FOREIGN KEY(parent_genome_id,scope_id) REFERENCES evolution.e2_workflow_genomes(genome_id,scope_id)
);

CREATE TABLE evolution.e2_mutation_proposals (
    mutation_id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    campaign_id text NOT NULL UNIQUE REFERENCES runtime.improvement_campaigns(campaign_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    parent_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    observation_id text NOT NULL REFERENCES evolution.observations(observation_id),
    parent_hash char(64) NOT NULL CHECK (parent_hash ~ '^[0-9a-f]{64}$'),
    operations jsonb NOT NULL CHECK (jsonb_typeof(operations)='array' AND jsonb_array_length(operations)>0),
    rationale text NOT NULL,
    expected_effect text NOT NULL,
    risk text NOT NULL,
    evaluation_plan_ref text NOT NULL,
    rollback_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    proposal_hash char(64) NOT NULL CHECK (proposal_hash ~ '^[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('PROPOSED','MATERIALIZED','EVALUATING','DECIDED','REJECTED')),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(scope_id,parent_genome_id,proposal_hash)
);

CREATE TABLE evolution.e2_evaluation_packs (
    pack_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    version integer NOT NULL CHECK(version>0),
    suite_id text NOT NULL,
    suite_version text NOT NULL,
    definition jsonb NOT NULL,
    definition_hash char(64) NOT NULL CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    evaluator_version text NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(scope_id,version),
    FOREIGN KEY(suite_id,suite_version) REFERENCES evolution.eval_suite_versions(eval_suite_id,version)
);

-- Scenario definitions use explicit partitions. Runtime/evaluator identities
-- can see DEVELOPMENT rows only; HOLDOUT rows require verifier/governance authority.
CREATE TABLE evolution.e2_evaluation_scenarios (
    pack_id text NOT NULL REFERENCES evolution.e2_evaluation_packs(pack_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    suite_id text NOT NULL,
    suite_version text NOT NULL,
    scenario_id text NOT NULL,
    partition text NOT NULL CHECK(partition IN ('DEVELOPMENT','HOLDOUT')),
    definition jsonb NOT NULL CHECK(jsonb_typeof(definition)='object'),
    definition_hash char(64) NOT NULL CHECK(definition_hash ~ '^[0-9a-f]{64}$'),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(pack_id,scenario_id),
    FOREIGN KEY(suite_id,suite_version) REFERENCES evolution.eval_suite_versions(eval_suite_id,version)
);
ALTER TABLE evolution.e2_evaluation_scenarios ENABLE ROW LEVEL SECURITY;
CREATE POLICY e2_scenario_runtime_development ON evolution.e2_evaluation_scenarios
    FOR SELECT TO agentic_runtime_runtime,agentic_runtime_evaluator
    USING(partition='DEVELOPMENT');
CREATE POLICY e2_scenario_governed_read ON evolution.e2_evaluation_scenarios
    FOR SELECT TO agentic_runtime_verifier,agentic_runtime_governance,agentic_promotion_executor
    USING(true);
CREATE POLICY e2_scenario_governed_insert ON evolution.e2_evaluation_scenarios
    FOR INSERT TO agentic_runtime_governance
    WITH CHECK(true);

CREATE TABLE evolution.e2_evaluation_runs (
    run_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    mutation_id text NOT NULL REFERENCES evolution.e2_mutation_proposals(mutation_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    champion_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    pack_id text NOT NULL REFERENCES evolution.e2_evaluation_packs(pack_id),
    plan_version_id text NOT NULL REFERENCES runtime.coordination_plan_versions(plan_version_id),
    recovery_event_id text NOT NULL REFERENCES runtime.events(event_id),
    artifact_refs jsonb NOT NULL CHECK(jsonb_typeof(artifact_refs)='array' AND jsonb_array_length(artifact_refs)>0),
    side text NOT NULL CHECK(side IN ('CANDIDATE','CHAMPION','HOLDOUT')),
    partition text NOT NULL CHECK(partition IN ('DEVELOPMENT','HOLDOUT')),
    scenario_id text NOT NULL,
    workflow_hash char(64) NOT NULL CHECK(workflow_hash ~ '^[0-9a-f]{64}$'),
    scenario_hash char(64) NOT NULL CHECK(scenario_hash ~ '^[0-9a-f]{64}$'),
    workload_key text NOT NULL,
    repetition integer NOT NULL CHECK(repetition>0),
    seed text NOT NULL,
    evaluator_id text NOT NULL,
    metrics jsonb NOT NULL,
    evidence_refs jsonb NOT NULL CHECK(jsonb_typeof(evidence_refs)='array'),
    evidence_hash char(64) NOT NULL CHECK(evidence_hash ~ '^[0-9a-f]{64}$'),
    status text NOT NULL CHECK(status IN ('COMPLETED','FAILED','QUARANTINED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(mutation_id,pack_id,side,workload_key,repetition),
    CHECK(candidate_genome_id<>champion_genome_id),
    CHECK((side IN ('CANDIDATE','CHAMPION') AND partition='DEVELOPMENT') OR
          (side='HOLDOUT' AND partition='HOLDOUT')),
    FOREIGN KEY(pack_id,scenario_id) REFERENCES evolution.e2_evaluation_scenarios(pack_id,scenario_id)
);

CREATE TABLE evolution.e2_evaluation_run_evidence (
    run_id text NOT NULL REFERENCES evolution.e2_evaluation_runs(run_id),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    worker_id text NOT NULL REFERENCES runtime.workers(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    lease_epoch bigint NOT NULL CHECK(lease_epoch>0),
    artifact_id text NOT NULL REFERENCES runtime.artifacts(artifact_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(run_id,artifact_id),
    UNIQUE(attempt_id,artifact_id)
);

CREATE TABLE evolution.e2_evaluation_missions (
    run_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    mutation_id text NOT NULL REFERENCES evolution.e2_mutation_proposals(mutation_id),
    genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    pack_id text NOT NULL REFERENCES evolution.e2_evaluation_packs(pack_id),
    side text NOT NULL CHECK(side IN ('CANDIDATE','CHAMPION','POSTPROMOTION','HOLDOUT')),
    partition text NOT NULL CHECK(partition IN ('DEVELOPMENT','HOLDOUT')),
    scenario_id text NOT NULL,
    scenario_hash char(64) NOT NULL CHECK(scenario_hash ~ '^[0-9a-f]{64}$'),
    workload_key text NOT NULL,
    repetition integer NOT NULL CHECK(repetition>0),
    seed text NOT NULL,
    workflow_hash char(64) NOT NULL CHECK(workflow_hash ~ '^[0-9a-f]{64}$'),
    mission_id text NOT NULL UNIQUE REFERENCES runtime.goals(goal_id),
    plan_version_id text NOT NULL UNIQUE REFERENCES runtime.coordination_plan_versions(plan_version_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(mutation_id,pack_id,side,workload_key,repetition),
    CHECK((side IN ('CANDIDATE','CHAMPION','POSTPROMOTION') AND partition='DEVELOPMENT') OR
          (side='HOLDOUT' AND partition='HOLDOUT')),
    FOREIGN KEY(pack_id,scenario_id) REFERENCES evolution.e2_evaluation_scenarios(pack_id,scenario_id)
);

-- Partition isolation continues through mission metadata and derived results.
-- Runtime/evaluator identities can read DEVELOPMENT only; the verifier owns
-- HOLDOUT reads/writes, while governance/promotion may inspect it for decisions.
ALTER TABLE evolution.e2_evaluation_missions ENABLE ROW LEVEL SECURITY;
ALTER TABLE evolution.e2_evaluation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE evolution.e2_evaluation_run_evidence ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
   EXECUTE 'CREATE POLICY e2_missions_runtime_development_read ON evolution.e2_evaluation_missions FOR SELECT TO agentic_runtime_runtime USING (partition=''DEVELOPMENT'')';
   EXECUTE 'CREATE POLICY e2_missions_runtime_insert ON evolution.e2_evaluation_missions FOR INSERT TO agentic_runtime_runtime WITH CHECK (partition IN (''DEVELOPMENT'',''HOLDOUT''))';
   EXECUTE 'CREATE POLICY e2_runs_runtime_development_read ON evolution.e2_evaluation_runs FOR SELECT TO agentic_runtime_runtime USING (partition=''DEVELOPMENT'')';
   EXECUTE 'CREATE POLICY e2_run_evidence_runtime_development_read ON evolution.e2_evaluation_run_evidence FOR SELECT TO agentic_runtime_runtime USING (EXISTS (SELECT 1 FROM evolution.e2_evaluation_runs r WHERE r.run_id=e2_evaluation_run_evidence.run_id AND r.partition=''DEVELOPMENT''))';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_evaluator') THEN
   EXECUTE 'CREATE POLICY e2_missions_evaluator_development_read ON evolution.e2_evaluation_missions FOR SELECT TO agentic_runtime_evaluator USING (partition=''DEVELOPMENT'')';
   EXECUTE 'CREATE POLICY e2_runs_evaluator_development_read ON evolution.e2_evaluation_runs FOR SELECT TO agentic_runtime_evaluator USING (partition=''DEVELOPMENT'')';
   EXECUTE 'CREATE POLICY e2_runs_evaluator_development_insert ON evolution.e2_evaluation_runs FOR INSERT TO agentic_runtime_evaluator WITH CHECK (partition=''DEVELOPMENT'')';
   EXECUTE 'CREATE POLICY e2_run_evidence_evaluator_development_read ON evolution.e2_evaluation_run_evidence FOR SELECT TO agentic_runtime_evaluator USING (EXISTS (SELECT 1 FROM evolution.e2_evaluation_runs r WHERE r.run_id=e2_evaluation_run_evidence.run_id AND r.partition=''DEVELOPMENT''))';
   EXECUTE 'CREATE POLICY e2_run_evidence_evaluator_development_insert ON evolution.e2_evaluation_run_evidence FOR INSERT TO agentic_runtime_evaluator WITH CHECK (EXISTS (SELECT 1 FROM evolution.e2_evaluation_runs r WHERE r.run_id=e2_evaluation_run_evidence.run_id AND r.partition=''DEVELOPMENT''))';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_verifier') THEN
   EXECUTE 'CREATE POLICY e2_missions_verifier_read ON evolution.e2_evaluation_missions FOR SELECT TO agentic_runtime_verifier USING (true)';
   EXECUTE 'CREATE POLICY e2_runs_verifier_read ON evolution.e2_evaluation_runs FOR SELECT TO agentic_runtime_verifier USING (true)';
   EXECUTE 'CREATE POLICY e2_runs_verifier_holdout_insert ON evolution.e2_evaluation_runs FOR INSERT TO agentic_runtime_verifier WITH CHECK (partition=''HOLDOUT'')';
   EXECUTE 'CREATE POLICY e2_run_evidence_verifier_read ON evolution.e2_evaluation_run_evidence FOR SELECT TO agentic_runtime_verifier USING (true)';
   EXECUTE 'CREATE POLICY e2_run_evidence_verifier_holdout_insert ON evolution.e2_evaluation_run_evidence FOR INSERT TO agentic_runtime_verifier WITH CHECK (EXISTS (SELECT 1 FROM evolution.e2_evaluation_runs r WHERE r.run_id=e2_evaluation_run_evidence.run_id AND r.partition=''HOLDOUT''))';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_governance') THEN
   EXECUTE 'CREATE POLICY e2_missions_governance_read ON evolution.e2_evaluation_missions FOR SELECT TO agentic_runtime_governance USING (true)';
   EXECUTE 'CREATE POLICY e2_runs_governance_read ON evolution.e2_evaluation_runs FOR SELECT TO agentic_runtime_governance USING (true)';
   EXECUTE 'CREATE POLICY e2_run_evidence_governance_read ON evolution.e2_evaluation_run_evidence FOR SELECT TO agentic_runtime_governance USING (true)';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor') THEN
   EXECUTE 'CREATE POLICY e2_missions_promotion_read ON evolution.e2_evaluation_missions FOR SELECT TO agentic_promotion_executor USING (true)';
   EXECUTE 'CREATE POLICY e2_runs_promotion_read ON evolution.e2_evaluation_runs FOR SELECT TO agentic_promotion_executor USING (true)';
 END IF;
END $$;

CREATE FUNCTION evolution.guard_e2_partitioned_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE m evolution.e2_evaluation_missions%ROWTYPE;
DECLARE s evolution.e2_evaluation_scenarios%ROWTYPE;
BEGIN
 IF NEW.evaluator_id IS DISTINCT FROM session_user THEN
   RAISE EXCEPTION 'E2 evaluator identity must be the authenticated database principal';
 END IF;
 IF NEW.partition='HOLDOUT' AND NOT pg_has_role(session_user,'agentic_runtime_verifier','member') THEN
   RAISE EXCEPTION 'HOLDOUT results require the separate authenticated verifier capability';
 END IF;
 IF NEW.partition='DEVELOPMENT' AND pg_has_role(session_user,'agentic_runtime_verifier','member') THEN
   RAISE EXCEPTION 'holdout verifier cannot write DEVELOPMENT evaluation results';
 END IF;
 SELECT * INTO m FROM evolution.e2_evaluation_missions WHERE run_id=NEW.run_id;
 SELECT * INTO s FROM evolution.e2_evaluation_scenarios
  WHERE pack_id=NEW.pack_id AND scenario_id=NEW.scenario_id;
 IF NOT FOUND OR m.run_id IS NULL OR m.mutation_id<>NEW.mutation_id OR
    m.campaign_id<>NEW.campaign_id OR m.scope_id<>NEW.scope_id OR m.pack_id<>NEW.pack_id OR
    m.genome_id<>(CASE WHEN NEW.side='CHAMPION' THEN NEW.champion_genome_id ELSE NEW.candidate_genome_id END) OR
    m.partition<>NEW.partition OR
    m.scenario_id<>NEW.scenario_id OR m.scenario_hash<>NEW.scenario_hash OR
    m.workflow_hash<>NEW.workflow_hash OR m.plan_version_id<>NEW.plan_version_id OR
    s.partition<>NEW.partition OR s.definition_hash<>NEW.scenario_hash OR
    s.suite_id<>(SELECT suite_id FROM evolution.e2_evaluation_packs WHERE pack_id=NEW.pack_id) OR
    s.suite_version<>(SELECT suite_version FROM evolution.e2_evaluation_packs WHERE pack_id=NEW.pack_id) THEN
   RAISE EXCEPTION 'E2 evaluation result partition, scenario, suite, genome or mission provenance mismatch';
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e2_run_partition_guard BEFORE INSERT ON evolution.e2_evaluation_runs
 FOR EACH ROW EXECUTE FUNCTION evolution.guard_e2_partitioned_result();

CREATE FUNCTION evolution.validate_e2_run_evidence_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE rid text;
DECLARE r evolution.e2_evaluation_runs%ROWTYPE;
DECLARE graph_count integer;
DECLARE covered_count integer;
DECLARE artifact_count integer;
BEGIN
 rid:=CASE WHEN TG_TABLE_NAME='e2_evaluation_runs' THEN NEW.run_id ELSE NEW.run_id END;
 SELECT * INTO r FROM evolution.e2_evaluation_runs WHERE run_id=rid;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT count(*) INTO graph_count FROM runtime.coordination_plan_nodes
  WHERE plan_version_id=r.plan_version_id;
 SELECT count(DISTINCT e.task_id),count(DISTINCT e.artifact_id)
   INTO covered_count,artifact_count
   FROM evolution.e2_evaluation_run_evidence e
   JOIN runtime.coordination_plan_nodes n ON n.plan_version_id=r.plan_version_id AND n.task_id=e.task_id
   JOIN runtime.tasks t ON t.task_id=e.task_id
   JOIN runtime.attempts a ON a.attempt_id=e.attempt_id AND a.task_id=t.task_id
   JOIN runtime.artifacts ar ON ar.artifact_id=e.artifact_id
  WHERE e.run_id=rid AND t.status='ACCEPTED' AND a.status='ACCEPTED'
    AND t.lease_epoch=e.lease_epoch AND a.lease_epoch=e.lease_epoch
    AND a.worker_id=e.worker_id AND a.worker_instance_id=e.worker_instance_id
    AND ar.producer_task_id=e.task_id AND ar.producer_attempt_id=e.attempt_id
    AND ar.verification_status='VERIFIED' AND e.artifact_id=ANY(ARRAY(SELECT jsonb_array_elements_text(r.artifact_refs)));
 IF graph_count=0 OR graph_count<>covered_count OR jsonb_array_length(r.artifact_refs)<>artifact_count OR
    (SELECT status FROM runtime.coordination_plan_versions WHERE plan_version_id=r.plan_version_id)<>'ACCEPTED' OR
    EXISTS(SELECT 1 FROM runtime.coordination_plan_nodes n JOIN runtime.tasks t USING(task_id)
      WHERE n.plan_version_id=r.plan_version_id AND t.status<>'ACCEPTED') THEN
   RAISE EXCEPTION 'E2 evaluation requires complete verified evidence for an accepted M9 plan graph';
 END IF;
 RETURN NULL;
END; $$;
CREATE CONSTRAINT TRIGGER e2_run_complete_after_run
 AFTER INSERT ON evolution.e2_evaluation_runs DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e2_run_evidence_complete();
CREATE CONSTRAINT TRIGGER e2_run_complete_after_evidence
 AFTER INSERT ON evolution.e2_evaluation_run_evidence DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e2_run_evidence_complete();

CREATE TABLE evolution.e2_comparisons (
    comparison_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    mutation_id text NOT NULL UNIQUE REFERENCES evolution.e2_mutation_proposals(mutation_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    champion_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    pack_id text NOT NULL REFERENCES evolution.e2_evaluation_packs(pack_id),
    evaluation_run_ids jsonb NOT NULL CHECK(jsonb_typeof(evaluation_run_ids)='array'),
    hard_gates jsonb NOT NULL,
    comparison jsonb NOT NULL,
    eligible boolean NOT NULL,
    evaluated_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    evidence_hash char(64) NOT NULL CHECK(evidence_hash ~ '^[0-9a-f]{64}$'),
    CHECK(candidate_genome_id<>champion_genome_id)
);

CREATE TABLE evolution.e2_promotion_authorizations (
    authorization_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    mutation_id text NOT NULL REFERENCES evolution.e2_mutation_proposals(mutation_id),
    comparison_id text NOT NULL REFERENCES evolution.e2_comparisons(comparison_id),
    expected_champion_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    rollback_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    policy_hash char(64) NOT NULL CHECK(policy_hash ~ '^[0-9a-f]{64}$'),
    authorized_by text NOT NULL,
    status text NOT NULL CHECK(status IN ('AUTHORIZED','EXECUTED','STALE','REJECTED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(mutation_id),
    CHECK(expected_champion_id<>candidate_genome_id)
);

CREATE TABLE evolution.e2_promotion_decisions (
    decision_id text PRIMARY KEY,
    authorization_id text NOT NULL UNIQUE REFERENCES evolution.e2_promotion_authorizations(authorization_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    prior_champion_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    promoted_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    comparison_id text NOT NULL REFERENCES evolution.e2_comparisons(comparison_id),
    evidence_hash char(64) NOT NULL CHECK(evidence_hash ~ '^[0-9a-f]{64}$'),
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE evolution.e2_postpromotion_checks (
    check_id text PRIMARY KEY,
    decision_id text NOT NULL REFERENCES evolution.e2_promotion_decisions(decision_id),
    plan_version_id text NOT NULL REFERENCES runtime.coordination_plan_versions(plan_version_id),
    recovery_event_id text NOT NULL REFERENCES runtime.events(event_id),
    artifact_refs jsonb NOT NULL CHECK(jsonb_typeof(artifact_refs)='array' AND jsonb_array_length(artifact_refs)>0),
    passed boolean NOT NULL,
    metrics jsonb NOT NULL,
    evidence_hash char(64) NOT NULL CHECK(evidence_hash ~ '^[0-9a-f]{64}$'),
    checked_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(decision_id)
);

CREATE TABLE evolution.e2_postpromotion_check_evidence (
    check_id text NOT NULL REFERENCES evolution.e2_postpromotion_checks(check_id),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    worker_id text NOT NULL REFERENCES runtime.workers(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    lease_epoch bigint NOT NULL CHECK(lease_epoch>0),
    artifact_id text NOT NULL REFERENCES runtime.artifacts(artifact_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(check_id,artifact_id)
);

CREATE TABLE evolution.e2_rollback_records (
    rollback_id text PRIMARY KEY,
    decision_id text NOT NULL UNIQUE REFERENCES evolution.e2_promotion_decisions(decision_id),
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    failed_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    restored_genome_id text NOT NULL REFERENCES evolution.e2_workflow_genomes(genome_id),
    reason text NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE evolution.e2_events (
    event_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e2_scope_policies(scope_id),
    campaign_id text,
    mutation_id text REFERENCES evolution.e2_mutation_proposals(mutation_id),
    event_type text NOT NULL,
    actor_id text NOT NULL,
    payload jsonb NOT NULL,
    payload_hash char(64) NOT NULL CHECK(payload_hash ~ '^[0-9a-f]{64}$'),
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX e2_runs_campaign_idx ON evolution.e2_evaluation_runs(campaign_id,created_at);
CREATE INDEX e2_events_scope_idx ON evolution.e2_events(scope_id,occurred_at);

CREATE TRIGGER e2_genome_immutable BEFORE UPDATE OR DELETE ON evolution.e2_workflow_genomes
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_eval_pack_immutable BEFORE UPDATE OR DELETE ON evolution.e2_evaluation_packs
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_eval_run_immutable BEFORE UPDATE OR DELETE ON evolution.e2_evaluation_runs
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_eval_run_evidence_immutable BEFORE UPDATE OR DELETE ON evolution.e2_evaluation_run_evidence
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_eval_mission_immutable BEFORE UPDATE OR DELETE ON evolution.e2_evaluation_missions
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_comparison_immutable BEFORE UPDATE OR DELETE ON evolution.e2_comparisons
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_decision_immutable BEFORE UPDATE OR DELETE ON evolution.e2_promotion_decisions
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_event_immutable BEFORE UPDATE OR DELETE ON evolution.e2_events
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();

CREATE OR REPLACE FUNCTION evolution.validate_e2_workflow_genome() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent_template jsonb;
DECLARE expected_hash text;
DECLARE actual_paths text[];
DECLARE declared_paths text[];
BEGIN
 expected_hash:=encode(sha256(convert_to(NEW.workflow_template::text,'UTF8')),'hex');
 IF NEW.workflow_hash IS DISTINCT FROM expected_hash THEN
   RAISE EXCEPTION 'E2 workflow hash does not match canonical JSONB';
 END IF;
 IF evolution.e1_contains_credential_material(NEW.workflow_template) THEN
   RAISE EXCEPTION 'E2 workflow template cannot contain credential material';
 END IF;
 IF NEW.parent_genome_id IS NULL THEN
   IF jsonb_array_length(NEW.changed_paths)<>0 THEN
     RAISE EXCEPTION 'baseline E2 genome cannot declare changed paths';
   END IF;
 ELSE
   SELECT workflow_template INTO parent_template FROM evolution.e2_workflow_genomes
    WHERE genome_id=NEW.parent_genome_id AND scope_id=NEW.scope_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'E2 parent must exist in the same governed scope'; END IF;
   SELECT coalesce(array_agg(path ORDER BY path),'{}'::text[]) INTO actual_paths
     FROM evolution.e1_config_diff_paths(parent_template,NEW.workflow_template,'') AS diff(path);
   SELECT coalesce(array_agg(path ORDER BY path),'{}'::text[]) INTO declared_paths
     FROM jsonb_array_elements_text(NEW.changed_paths) AS declared(path);
   IF actual_paths IS DISTINCT FROM declared_paths OR cardinality(actual_paths)=0 THEN
     RAISE EXCEPTION 'E2 changed paths must exactly match canonical template diff';
   END IF;
   IF EXISTS(SELECT 1 FROM unnest(actual_paths) p WHERE p<>'nodes' AND p NOT LIKE 'nodes.%') THEN
     RAISE EXCEPTION 'E2 mutations are restricted to workflow nodes';
   END IF;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e2_genome_validate BEFORE INSERT ON evolution.e2_workflow_genomes
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e2_workflow_genome();

CREATE OR REPLACE FUNCTION evolution.guard_e2_proposal_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'E2 mutation proposals cannot be deleted'; END IF;
 IF OLD.mutation_id<>NEW.mutation_id OR OLD.idempotency_key<>NEW.idempotency_key OR
    OLD.scope_id<>NEW.scope_id OR OLD.parent_genome_id<>NEW.parent_genome_id OR
    OLD.observation_id<>NEW.observation_id OR OLD.parent_hash<>NEW.parent_hash OR OLD.operations<>NEW.operations OR
    OLD.rationale<>NEW.rationale OR OLD.expected_effect<>NEW.expected_effect OR
    OLD.risk<>NEW.risk OR OLD.evaluation_plan_ref<>NEW.evaluation_plan_ref OR
    OLD.rollback_genome_id<>NEW.rollback_genome_id OR OLD.proposal_hash<>NEW.proposal_hash OR
    OLD.created_by<>NEW.created_by OR OLD.created_at<>NEW.created_at THEN
   RAISE EXCEPTION 'E2 mutation proposal content is immutable';
 END IF;
 IF OLD.status<>NEW.status AND NOT (
   (OLD.status='PROPOSED' AND NEW.status IN ('MATERIALIZED','REJECTED')) OR
   (OLD.status='MATERIALIZED' AND NEW.status IN ('EVALUATING','REJECTED')) OR
   (OLD.status='EVALUATING' AND NEW.status IN ('DECIDED','REJECTED'))
 ) THEN RAISE EXCEPTION 'invalid E2 proposal lifecycle transition'; END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e2_proposal_guard BEFORE UPDATE OR DELETE ON evolution.e2_mutation_proposals
 FOR EACH ROW EXECUTE FUNCTION evolution.guard_e2_proposal_transition();

CREATE OR REPLACE FUNCTION evolution.guard_e2_authorization_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'E2 promotion authorization is append-only'; END IF;
 IF OLD.authorization_id<>NEW.authorization_id OR OLD.scope_id<>NEW.scope_id OR
    OLD.mutation_id<>NEW.mutation_id OR OLD.comparison_id<>NEW.comparison_id OR
    OLD.expected_champion_id<>NEW.expected_champion_id OR OLD.candidate_genome_id<>NEW.candidate_genome_id OR
    OLD.rollback_genome_id<>NEW.rollback_genome_id OR OLD.policy_hash<>NEW.policy_hash OR
    OLD.authorized_by<>NEW.authorized_by OR OLD.created_at<>NEW.created_at THEN
   RAISE EXCEPTION 'E2 promotion authorization binding is immutable';
 END IF;
 IF OLD.status<>NEW.status AND NOT (OLD.status='AUTHORIZED' AND NEW.status IN ('EXECUTED','STALE','REJECTED')) THEN
   RAISE EXCEPTION 'invalid E2 authorization lifecycle transition';
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e2_authorization_guard BEFORE UPDATE OR DELETE ON evolution.e2_promotion_authorizations
 FOR EACH ROW EXECUTE FUNCTION evolution.guard_e2_authorization_transition();
CREATE TRIGGER e2_check_no_mutation BEFORE UPDATE OR DELETE ON evolution.e2_postpromotion_checks
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_check_evidence_no_mutation BEFORE UPDATE OR DELETE ON evolution.e2_postpromotion_check_evidence
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER e2_rollback_no_mutation BEFORE UPDATE OR DELETE ON evolution.e2_rollback_records
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();

CREATE OR REPLACE FUNCTION evolution.execute_e2_promotion(
 p_authorization_id text,p_decision_id text,p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,runtime,pg_temp AS $$
DECLARE a evolution.e2_promotion_authorizations%ROWTYPE;
DECLARE c evolution.e2_comparisons%ROWTYPE;
DECLARE m evolution.e2_mutation_proposals%ROWTYPE;
DECLARE pack evolution.e2_evaluation_packs%ROWTYPE;
DECLARE current_champion text;
DECLARE expected_runs integer;
DECLARE actual_runs integer;
DECLARE existing text;
BEGIN
 IF p_actor IS DISTINCT FROM session_user OR
    NOT pg_has_role(session_user,'agentic_runtime_e2_promotion','member') THEN
   RAISE EXCEPTION 'E2 promotion requires the authenticated promotion capability';
 END IF;
 SELECT * INTO a FROM evolution.e2_promotion_authorizations WHERE authorization_id=p_authorization_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'E2 authorization not found'; END IF;
 SELECT decision_id INTO existing FROM evolution.e2_promotion_decisions WHERE authorization_id=p_authorization_id;
 IF FOUND THEN
   IF existing<>p_decision_id THEN RAISE EXCEPTION 'E2 promotion decision identity conflict'; END IF;
   RETURN existing;
 END IF;
 SELECT * INTO c FROM evolution.e2_comparisons WHERE comparison_id=a.comparison_id;
 SELECT * INTO m FROM evolution.e2_mutation_proposals WHERE mutation_id=a.mutation_id;
 SELECT * INTO pack FROM evolution.e2_evaluation_packs WHERE pack_id=c.pack_id;
 SELECT genome_id INTO current_champion FROM evolution.scope_champions WHERE scope_id=a.scope_id FOR UPDATE;
 IF a.status<>'AUTHORIZED' OR
    a.policy_hash<>(SELECT policy_hash FROM evolution.e2_scope_policies WHERE scope_id=a.scope_id) OR
    NOT c.eligible OR c.scope_id<>a.scope_id OR c.mutation_id<>a.mutation_id OR
    c.candidate_genome_id<>a.candidate_genome_id OR c.champion_genome_id<>a.expected_champion_id OR
    current_champion<>a.expected_champion_id OR m.status<>'EVALUATING' OR
    m.rollback_genome_id<>a.rollback_genome_id OR
    m.created_by=c.evaluated_by OR a.authorized_by IN (m.created_by,c.evaluated_by) OR
    session_user IN (m.created_by,c.evaluated_by,a.authorized_by) THEN
   UPDATE evolution.e2_promotion_authorizations SET status='STALE' WHERE authorization_id=p_authorization_id;
   RAISE EXCEPTION 'E2 promotion authorization or champion baseline is stale';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM evolution.e2_workflow_genomes WHERE genome_id=a.candidate_genome_id
               AND scope_id=a.scope_id AND parent_genome_id=a.expected_champion_id) OR
    NOT EXISTS(SELECT 1 FROM evolution.e2_workflow_genomes WHERE genome_id=a.rollback_genome_id
               AND scope_id=a.scope_id) THEN RAISE EXCEPTION 'E2 candidate or rollback lineage is invalid'; END IF;
 expected_runs:=jsonb_array_length(pack.definition->'workload_keys')*
                (pack.definition->>'repetitions')::integer*2;
 SELECT count(*) INTO actual_runs FROM evolution.e2_evaluation_runs
  WHERE mutation_id=a.mutation_id AND pack_id=pack.pack_id AND
        candidate_genome_id=a.candidate_genome_id AND champion_genome_id=a.expected_champion_id
        AND partition='DEVELOPMENT' AND status='COMPLETED';
 IF actual_runs<>expected_runs OR jsonb_array_length(c.evaluation_run_ids)<>expected_runs THEN
   RAISE EXCEPTION 'E2 promotion requires the complete paired frozen evaluation';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(c.evaluation_run_ids) cited(run_id)
      WHERE NOT EXISTS(SELECT 1 FROM evolution.e2_evaluation_runs r
        WHERE r.run_id=cited.run_id AND r.mutation_id=a.mutation_id AND r.pack_id=pack.pack_id
          AND r.candidate_genome_id=a.candidate_genome_id
          AND r.champion_genome_id=a.expected_champion_id AND r.status='COMPLETED')) OR
    EXISTS(SELECT 1 FROM evolution.e2_evaluation_runs r
      WHERE r.mutation_id=a.mutation_id AND r.pack_id=pack.pack_id
        AND r.candidate_genome_id=a.candidate_genome_id
        AND r.champion_genome_id=a.expected_champion_id AND r.partition='DEVELOPMENT' AND r.status='COMPLETED'
        AND NOT (c.evaluation_run_ids ? r.run_id)) THEN
   RAISE EXCEPTION 'E2 promotion run references do not match complete persisted evidence';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM evolution.e2_evaluation_scenarios s
      WHERE s.pack_id=pack.pack_id AND s.partition='HOLDOUT') OR
    EXISTS(SELECT 1 FROM evolution.e2_evaluation_scenarios s
      WHERE s.pack_id=pack.pack_id AND s.partition='HOLDOUT' AND NOT EXISTS(
       SELECT 1 FROM evolution.e2_evaluation_runs r
       JOIN evolution.e2_evaluation_missions mission USING(run_id)
       JOIN evolution.e2_workflow_genomes g ON g.genome_id=r.candidate_genome_id
       WHERE r.mutation_id=a.mutation_id AND r.pack_id=pack.pack_id AND r.side='HOLDOUT'
         AND r.partition='HOLDOUT' AND r.scenario_id=s.scenario_id
         AND r.scenario_hash=s.definition_hash AND r.workflow_hash=g.workflow_hash
         AND r.candidate_genome_id=a.candidate_genome_id
         AND r.status='COMPLETED' AND r.evaluator_id NOT IN (m.created_by,c.evaluated_by,a.authorized_by)
         AND mission.created_at>=g.created_at
         AND r.metrics->>'success_rate'='1.0'
         AND r.metrics->>'verifier_acceptance_rate'='1.0'
         AND r.metrics->>'recovery_pass'='true'
         AND s.suite_id=pack.suite_id AND s.suite_version=pack.suite_version)) OR
    (SELECT count(*) FROM evolution.e2_evaluation_runs r
       WHERE r.mutation_id=a.mutation_id AND r.pack_id=pack.pack_id AND r.side='HOLDOUT') <>
    (SELECT count(*) FROM evolution.e2_evaluation_scenarios s
       WHERE s.pack_id=pack.pack_id AND s.partition='HOLDOUT') THEN
   RAISE EXCEPTION 'E2 promotion requires complete independently evaluated frozen HOLDOUT evidence';
 END IF;
 PERFORM set_config('evolution.governed_promotion','authorized',true);
 UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=a.expected_champion_id;
 UPDATE evolution.system_genomes SET status='CHAMPION' WHERE genome_id=a.candidate_genome_id;
 UPDATE evolution.scope_champions SET genome_id=a.candidate_genome_id,updated_at=now() WHERE scope_id=a.scope_id;
 INSERT INTO evolution.e2_promotion_decisions
  (decision_id,authorization_id,scope_id,prior_champion_id,promoted_genome_id,
   comparison_id,evidence_hash,created_by)
 VALUES (p_decision_id,p_authorization_id,a.scope_id,a.expected_champion_id,
         a.candidate_genome_id,a.comparison_id,c.evidence_hash,p_actor);
 UPDATE evolution.e2_promotion_authorizations SET status='EXECUTED' WHERE authorization_id=p_authorization_id;
 UPDATE evolution.e2_mutation_proposals SET status='DECIDED' WHERE mutation_id=a.mutation_id;
 INSERT INTO evolution.e2_events(event_id,scope_id,campaign_id,mutation_id,event_type,actor_id,payload,payload_hash)
 VALUES ('e2evt_'||md5(p_decision_id),a.scope_id,m.campaign_id,a.mutation_id,'E2_CHAMPION_PROMOTED',p_actor,
   jsonb_build_object('decision_id',p_decision_id,'prior',a.expected_champion_id,'candidate',a.candidate_genome_id,
                      'comparison_id',a.comparison_id),encode(sha256(convert_to(jsonb_build_object(
       'decision_id',p_decision_id,'prior',a.expected_champion_id,'candidate',a.candidate_genome_id,
       'comparison_id',a.comparison_id)::text,'UTF8')),'hex'));
 RETURN p_decision_id;
END; $$;

CREATE OR REPLACE FUNCTION evolution.execute_e2_rollback(p_decision_id text,p_rollback_id text,p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,runtime,pg_temp AS $$
DECLARE d evolution.e2_promotion_decisions%ROWTYPE;
DECLARE chk evolution.e2_postpromotion_checks%ROWTYPE;
DECLARE current_id text;
DECLARE existing text;
BEGIN
 IF p_actor IS DISTINCT FROM session_user OR
    NOT pg_has_role(session_user,'agentic_runtime_e2_promotion','member') THEN
   RAISE EXCEPTION 'E2 rollback requires the authenticated promotion capability';
 END IF;
 SELECT rollback_id INTO existing FROM evolution.e2_rollback_records WHERE decision_id=p_decision_id;
 IF FOUND THEN
   RETURN existing;
 END IF;
 SELECT * INTO d FROM evolution.e2_promotion_decisions WHERE decision_id=p_decision_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'E2 promotion decision not found'; END IF;
 SELECT * INTO chk FROM evolution.e2_postpromotion_checks WHERE decision_id=p_decision_id;
 IF NOT FOUND OR chk.passed THEN RAISE EXCEPTION 'E2 rollback requires a persisted failed post-promotion check'; END IF;
 SELECT genome_id INTO current_id FROM evolution.scope_champions WHERE scope_id=d.scope_id FOR UPDATE;
 IF current_id<>d.promoted_genome_id OR NOT EXISTS(
    SELECT 1 FROM evolution.e2_workflow_genomes WHERE genome_id=d.prior_champion_id AND scope_id=d.scope_id) THEN
   RAISE EXCEPTION 'E2 rollback target is stale or invalid';
 END IF;
 PERFORM set_config('evolution.governed_promotion','authorized',true);
 UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=d.promoted_genome_id;
 UPDATE evolution.system_genomes SET status='CHAMPION' WHERE genome_id=d.prior_champion_id;
 UPDATE evolution.scope_champions SET genome_id=d.prior_champion_id,updated_at=now() WHERE scope_id=d.scope_id;
 INSERT INTO evolution.e2_rollback_records(rollback_id,decision_id,scope_id,failed_genome_id,
     restored_genome_id,reason,created_by)
 VALUES(p_rollback_id,p_decision_id,d.scope_id,d.promoted_genome_id,d.prior_champion_id,
        'post-promotion regression',p_actor);
 INSERT INTO evolution.e2_events(event_id,scope_id,mutation_id,event_type,actor_id,payload,payload_hash)
 SELECT 'e2evt_'||md5(p_rollback_id),d.scope_id,a.mutation_id,'E2_CHAMPION_ROLLED_BACK',p_actor,
   jsonb_build_object('rollback_id',p_rollback_id,'decision_id',p_decision_id,
                      'failed',d.promoted_genome_id,'restored',d.prior_champion_id),
   encode(sha256(convert_to(jsonb_build_object('rollback_id',p_rollback_id,'decision_id',p_decision_id,
       'failed',d.promoted_genome_id,'restored',d.prior_champion_id)::text,'UTF8')),'hex')
 FROM evolution.e2_promotion_authorizations a WHERE a.authorization_id=d.authorization_id;
 RETURN p_rollback_id;
END; $$;

REVOKE ALL ON FUNCTION evolution.execute_e2_promotion(text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION evolution.execute_e2_rollback(text,text,text) FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor') THEN
   -- Keep the migration principal as SECURITY DEFINER owner unless it is
   -- explicitly a member of the non-login executor capability. Fresh and
   -- restricted migration identities must not need that promotion authority.
   IF pg_has_role(current_user,'agentic_promotion_executor','MEMBER') THEN
     ALTER FUNCTION evolution.execute_e2_promotion(text,text,text) OWNER TO agentic_promotion_executor;
     ALTER FUNCTION evolution.execute_e2_rollback(text,text,text) OWNER TO agentic_promotion_executor;
   END IF;
   GRANT USAGE ON SCHEMA evolution,runtime TO agentic_promotion_executor;
   IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_e2_promotion') THEN
     GRANT EXECUTE ON FUNCTION evolution.execute_e2_promotion(text,text,text) TO agentic_runtime_e2_promotion;
     GRANT EXECUTE ON FUNCTION evolution.execute_e2_rollback(text,text,text) TO agentic_runtime_e2_promotion;
   END IF;
   GRANT SELECT ON evolution.e2_promotion_authorizations,evolution.e2_comparisons,
       evolution.e2_mutation_proposals,evolution.e2_evaluation_packs,
       evolution.e2_evaluation_scenarios,evolution.e2_evaluation_runs,
       evolution.e2_evaluation_missions,evolution.e2_workflow_genomes,
       evolution.e2_scope_policies,evolution.scope_champions,
       evolution.e2_postpromotion_checks,evolution.e2_promotion_decisions,
       evolution.e2_rollback_records,evolution.system_genomes TO agentic_promotion_executor;
   GRANT UPDATE ON evolution.scope_champions,evolution.system_genomes,
       evolution.e2_promotion_authorizations,evolution.e2_mutation_proposals,
       runtime.improvement_campaigns,runtime.campaigns,runtime.goals TO agentic_promotion_executor;
   GRANT INSERT ON evolution.e2_promotion_authorizations,evolution.e2_promotion_decisions,evolution.e2_events,
       evolution.e2_rollback_records TO agentic_promotion_executor;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
   GRANT SELECT ON evolution.scopes,evolution.system_genomes,evolution.scope_champions,
       evolution.genome_parents,evolution.e2_scope_policies,evolution.e2_workflow_genomes,
       evolution.e2_mutation_proposals,evolution.e2_evaluation_packs,evolution.e2_evaluation_scenarios,
       evolution.e2_evaluation_runs,evolution.e2_evaluation_missions,evolution.e2_evaluation_run_evidence,
       evolution.e2_comparisons TO agentic_runtime_runtime;
   GRANT INSERT ON evolution.e2_mutation_proposals,evolution.e2_workflow_genomes,
       evolution.e2_evaluation_missions,evolution.e2_events,evolution.genome_parents,
       evolution.system_genomes TO agentic_runtime_runtime;
   GRANT UPDATE(status) ON evolution.e2_mutation_proposals,evolution.system_genomes TO agentic_runtime_runtime;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_evaluator') THEN
 GRANT SELECT ON evolution.e2_scope_policies,evolution.e2_workflow_genomes,
       evolution.e2_mutation_proposals,evolution.e2_evaluation_packs,
       evolution.e2_evaluation_scenarios,
       evolution.e2_evaluation_runs,evolution.e2_evaluation_missions,
       evolution.e2_evaluation_run_evidence,evolution.e2_comparisons,
       evolution.e2_promotion_authorizations,evolution.e2_promotion_decisions,
       evolution.e2_postpromotion_checks,evolution.e2_postpromotion_check_evidence TO agentic_runtime_evaluator;
   -- The evaluator can read execution evidence, but the runtime worker remains
   -- the only principal that writes task, attempt, lease and artifact state.
   GRANT SELECT ON runtime.coordination_plan_versions,runtime.coordination_plan_nodes,
       runtime.tasks,runtime.attempts,runtime.workers,runtime.worker_instances,
       runtime.artifacts,runtime.events,runtime.leases,runtime.outbox TO agentic_runtime_evaluator;
   GRANT INSERT ON evolution.e2_evaluation_runs,evolution.e2_evaluation_run_evidence,evolution.e2_comparisons,
       evolution.e2_events TO agentic_runtime_evaluator;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_verifier') THEN
   GRANT USAGE ON SCHEMA evolution,runtime TO agentic_runtime_verifier;
   GRANT SELECT ON evolution.e2_scope_policies,evolution.e2_workflow_genomes,
       evolution.e2_mutation_proposals,evolution.e2_evaluation_packs,
       evolution.e2_evaluation_scenarios,
       evolution.e2_evaluation_runs,evolution.e2_evaluation_missions,
       evolution.e2_evaluation_run_evidence,evolution.e2_comparisons,
       evolution.e2_promotion_authorizations,evolution.e2_promotion_decisions,
       evolution.e2_postpromotion_checks,evolution.e2_postpromotion_check_evidence,
       evolution.e2_rollback_records TO agentic_runtime_verifier;
   GRANT SELECT ON runtime.coordination_plan_versions,runtime.coordination_plan_nodes,
       runtime.tasks,runtime.attempts,runtime.workers,runtime.worker_instances,
       runtime.artifacts,runtime.events,runtime.leases,runtime.outbox TO agentic_runtime_verifier;
   GRANT INSERT ON evolution.e2_postpromotion_checks,evolution.e2_postpromotion_check_evidence,
       evolution.e2_evaluation_runs,evolution.e2_evaluation_run_evidence,
       evolution.e2_events TO agentic_runtime_verifier;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_governance') THEN
   GRANT SELECT ON evolution.scopes,evolution.eval_suite_versions,
       evolution.system_genomes,evolution.scope_champions TO agentic_runtime_governance;
   GRANT SELECT ON evolution.e2_scope_policies,evolution.e2_workflow_genomes,
       evolution.e2_mutation_proposals,evolution.e2_evaluation_packs,
       evolution.e2_evaluation_runs,evolution.e2_evaluation_missions,evolution.e2_evaluation_run_evidence,
       evolution.e2_comparisons,
       evolution.e2_promotion_authorizations,evolution.e2_promotion_decisions,
       evolution.e2_postpromotion_checks TO agentic_runtime_governance;
   GRANT INSERT ON evolution.scopes,evolution.eval_suite_versions,
       evolution.e2_scope_policies,evolution.e2_workflow_genomes,
       evolution.system_genomes,evolution.scope_champions,evolution.e2_evaluation_packs,
       evolution.e2_evaluation_scenarios,evolution.e2_promotion_authorizations,
       evolution.e2_events TO agentic_runtime_governance;
 END IF;
END $$;
