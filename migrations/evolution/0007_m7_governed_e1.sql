-- M7: bounded E1 autonomous promotion. Existing M1-M6 records remain intact.
CREATE TABLE evolution.e1_scope_policies (
    scope_id text PRIMARY KEY REFERENCES evolution.scopes(scope_id),
    tier text NOT NULL DEFAULT 'E1' CHECK (tier='E1'),
    policy_version text NOT NULL,
    allowed_paths jsonb NOT NULL CHECK (jsonb_typeof(allowed_paths)='array'),
    bounds jsonb NOT NULL DEFAULT '{}'::jsonb,
    mode text NOT NULL DEFAULT 'SHADOW' CHECK (mode IN ('SHADOW','ACTIVE_E1')),
    promotion_policy jsonb NOT NULL,
    policy_hash text NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    CHECK (promotion_policy ? 'required_improvement')
);

CREATE TABLE evolution.e1_genome_versions (
    genome_id text PRIMARY KEY REFERENCES evolution.system_genomes(genome_id),
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    tier text NOT NULL CHECK (tier='E1'),
    parent_genome_id text REFERENCES evolution.system_genomes(genome_id),
    canonical_config jsonb NOT NULL,
    config_hash text NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
    changed_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
    provenance jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(genome_id,scope_id),
    CHECK (jsonb_typeof(canonical_config)='object')
);

CREATE OR REPLACE FUNCTION evolution.e1_config_diff_paths(old_config jsonb,new_config jsonb,prefix text DEFAULT '')
RETURNS SETOF text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE k text;
DECLARE child_path text;
BEGIN
 IF jsonb_typeof(old_config)='object' AND jsonb_typeof(new_config)='object' THEN
   FOR k IN SELECT key FROM (
       SELECT jsonb_object_keys(old_config) AS key
       UNION SELECT jsonb_object_keys(new_config) AS key
   ) keys ORDER BY key LOOP
     child_path:=CASE WHEN prefix='' THEN k ELSE prefix||'.'||k END;
     IF NOT (old_config ? k) OR NOT (new_config ? k) THEN
       RETURN NEXT child_path;
     ELSE
       RETURN QUERY SELECT * FROM evolution.e1_config_diff_paths(old_config->k,new_config->k,child_path);
     END IF;
   END LOOP;
 ELSIF old_config IS DISTINCT FROM new_config THEN
   RETURN NEXT prefix;
 END IF;
END; $$;

CREATE OR REPLACE FUNCTION evolution.e1_contains_credential_material(config jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
WITH RECURSIVE nodes(value) AS (
 SELECT config
 UNION ALL
 SELECT child.value
 FROM nodes n
 CROSS JOIN LATERAL (
   SELECT item.value FROM jsonb_each(
       CASE WHEN jsonb_typeof(n.value)='object' THEN n.value ELSE '{}'::jsonb END) AS item
   UNION ALL
   SELECT item.value FROM jsonb_array_elements(
       CASE WHEN jsonb_typeof(n.value)='array' THEN n.value ELSE '[]'::jsonb END) AS item(value)
 ) AS child
)
SELECT EXISTS(
 SELECT 1 FROM nodes n
 CROSS JOIN LATERAL jsonb_each(
   CASE WHEN jsonb_typeof(n.value)='object' THEN n.value ELSE '{}'::jsonb END) AS field
 WHERE field.key ~* '(^|[_-])(password|passwd|secret|api[_-]?key|access[_-]?token|private[_-]?key|credential)([_-]|$)'
) OR config::text ~* '(sk-[A-Za-z0-9]{20,}|-----BEGIN (RSA |OPENSSH |EC )?PRIVATE KEY-----|Bearer [A-Za-z0-9._~-]{20,})'
$$;

CREATE OR REPLACE FUNCTION evolution.validate_e1_genome_hash() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected text;
DECLARE parent_config jsonb;
DECLARE parent_scope text;
DECLARE actual_paths text[];
DECLARE declared_paths text[];
BEGIN
 IF evolution.e1_contains_credential_material(NEW.canonical_config) THEN
   RAISE EXCEPTION 'E1 genome cannot contain credential material; store only external secret references';
 END IF;
 expected:=encode(sha256(convert_to(NEW.canonical_config::text,'UTF8')),'hex');
 IF NEW.config_hash IS DISTINCT FROM expected THEN
   RAISE EXCEPTION 'E1 genome hash does not match canonical JSONB configuration';
 END IF;
 IF NEW.parent_genome_id IS NULL THEN
   IF jsonb_array_length(NEW.changed_paths)<>0 THEN
     RAISE EXCEPTION 'baseline E1 genome cannot declare changed paths';
   END IF;
 ELSE
   SELECT v.canonical_config,v.scope_id INTO parent_config,parent_scope
     FROM evolution.e1_genome_versions v WHERE v.genome_id=NEW.parent_genome_id;
   IF NOT FOUND OR parent_scope<>NEW.scope_id THEN
     RAISE EXCEPTION 'E1 genome parent must exist in the same controlled scope';
   END IF;
   SELECT coalesce(array_agg(path ORDER BY path),'{}'::text[]) INTO actual_paths
     FROM evolution.e1_config_diff_paths(parent_config,NEW.canonical_config,'') AS diff(path);
   SELECT coalesce(array_agg(path ORDER BY path),'{}'::text[]) INTO declared_paths
     FROM jsonb_array_elements_text(NEW.changed_paths) AS declared(path);
   IF actual_paths IS DISTINCT FROM declared_paths OR cardinality(actual_paths)=0 THEN
     RAISE EXCEPTION 'E1 declared mutation paths must exactly match the canonical configuration diff';
   END IF;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e1_genome_hash_valid BEFORE INSERT ON evolution.e1_genome_versions
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e1_genome_hash();

CREATE TABLE evolution.e1_evaluation_pack_versions (
    pack_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    version integer NOT NULL CHECK (version > 0),
    suite_id text NOT NULL,
    suite_version text NOT NULL,
    definition jsonb NOT NULL,
    definition_hash text NOT NULL CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    evaluator_id text NOT NULL,
    evaluator_version text NOT NULL,
    code_revision text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    UNIQUE(scope_id,version)
);

CREATE TABLE evolution.e1_evaluation_runs (
    run_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    genome_id text NOT NULL REFERENCES evolution.e1_genome_versions(genome_id),
    baseline_genome_id text NOT NULL REFERENCES evolution.e1_genome_versions(genome_id),
    pack_id text NOT NULL REFERENCES evolution.e1_evaluation_pack_versions(pack_id),
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    evaluator_id text NOT NULL,
    evaluator_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('COMPLETED','FAILED','QUARANTINED')),
    metrics jsonb NOT NULL,
    result_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    reproducible boolean NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    UNIQUE(run_id,scope_id)
);

CREATE TABLE evolution.e1_comparisons (
    comparison_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.e1_genome_versions(genome_id),
    champion_genome_id text NOT NULL REFERENCES evolution.e1_genome_versions(genome_id),
    candidate_run_id text NOT NULL REFERENCES evolution.e1_evaluation_runs(run_id),
    champion_run_id text NOT NULL REFERENCES evolution.e1_evaluation_runs(run_id),
    eligible boolean NOT NULL,
    disposition text NOT NULL CHECK (disposition IN ('ELIGIBLE','REJECTED','RETAINED')),
    rationale jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(candidate_genome_id,candidate_run_id)
);

CREATE TABLE evolution.e1_promotion_authorizations (
    authorization_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    expected_champion_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    expected_champion_hash text NOT NULL,
    candidate_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    candidate_hash text NOT NULL,
    comparison_id text NOT NULL REFERENCES evolution.e1_comparisons(comparison_id),
    pack_id text NOT NULL REFERENCES evolution.e1_evaluation_pack_versions(pack_id),
    pack_hash text NOT NULL,
    evidence_hash text NOT NULL,
    policy_hash text NOT NULL,
    authorized_by text NOT NULL,
    authorized_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL CHECK (status IN ('AUTHORIZED','EXECUTED','STALE','REJECTED')),
    decision_id text UNIQUE,
    UNIQUE(campaign_id,candidate_genome_id)
);

CREATE TABLE evolution.e1_promotion_decisions (
    decision_id text PRIMARY KEY,
    authorization_id text NOT NULL UNIQUE REFERENCES evolution.e1_promotion_authorizations(authorization_id),
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    prior_champion_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    promoted_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    comparison_id text NOT NULL REFERENCES evolution.e1_comparisons(comparison_id),
    pack_id text NOT NULL REFERENCES evolution.e1_evaluation_pack_versions(pack_id),
    evidence_hash text NOT NULL,
    decision text NOT NULL CHECK (decision IN ('PROMOTED','WOULD_PROMOTE')),
    mode text NOT NULL CHECK (mode IN ('ACTIVE_E1','SHADOW')),
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL
);

CREATE TABLE evolution.e1_postpromotion_checks (
    check_id text PRIMARY KEY,
    decision_id text NOT NULL REFERENCES evolution.e1_promotion_decisions(decision_id),
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    passed boolean NOT NULL,
    failure_class text,
    metrics jsonb NOT NULL,
    evidence_hash text NOT NULL CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
    checked_at timestamptz NOT NULL DEFAULT now(),
    CHECK (passed OR failure_class IS NOT NULL)
);

CREATE TABLE evolution.e1_rollback_records (
    rollback_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    decision_id text NOT NULL UNIQUE REFERENCES evolution.e1_promotion_decisions(decision_id),
    failed_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    restored_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    check_id text NOT NULL REFERENCES evolution.e1_postpromotion_checks(check_id),
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL
);

CREATE TABLE evolution.e1_events (
    event_id text PRIMARY KEY,
    scope_id text REFERENCES evolution.e1_scope_policies(scope_id),
    campaign_id text REFERENCES runtime.improvement_campaigns(campaign_id),
    event_type text NOT NULL,
    actor_id text NOT NULL,
    causation_id text,
    payload jsonb NOT NULL,
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE evolution.e1_executor_measurements (
    measurement_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.e1_scope_policies(scope_id),
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    task_class text NOT NULL,
    executor_id text NOT NULL,
    capability text NOT NULL,
    verified boolean NOT NULL,
    quality numeric,
    latency_ms numeric CHECK (latency_ms >= 0),
    cost_units numeric CHECK (cost_units >= 0),
    artifact_ref text NOT NULL REFERENCES runtime.artifacts(artifact_id),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    model_run_id text NOT NULL REFERENCES runtime.model_runs(model_run_id),
    measured_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (verified AND quality BETWEEN 0 AND 1)
);
CREATE INDEX e1_executor_measurement_lookup ON evolution.e1_executor_measurements(scope_id,task_class,executor_id,measured_at DESC);

-- Measurement snapshots are admissible only when derived from an accepted
-- runtime task, its accepted attempt, a verified result artifact, and the
-- matching persisted executor run. Callers cannot invent benchmark rows.
CREATE OR REPLACE FUNCTION evolution.validate_e1_measurement() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE t runtime.tasks%ROWTYPE;
DECLARE a runtime.attempts%ROWTYPE;
DECLARE ar runtime.artifacts%ROWTYPE;
DECLARE mr runtime.model_runs%ROWTYPE;
BEGIN
 SELECT * INTO t FROM runtime.tasks WHERE task_id=NEW.task_id;
 SELECT * INTO a FROM runtime.attempts WHERE attempt_id=NEW.attempt_id;
 SELECT * INTO ar FROM runtime.artifacts WHERE artifact_id=NEW.artifact_ref;
 SELECT * INTO mr FROM runtime.model_runs WHERE model_run_id=NEW.model_run_id;
 IF t.status<>'ACCEPTED' OR a.status<>'ACCEPTED' OR a.task_id<>t.task_id
    OR NEW.campaign_id<>t.campaign_id OR NOT (t.result_refs ? NEW.artifact_ref)
    OR ar.verification_status<>'VERIFIED' OR ar.producer_task_id<>t.task_id
    OR ar.producer_attempt_id<>a.attempt_id OR mr.task_id<>t.task_id OR mr.attempt_id<>a.attempt_id
    OR mr.executor_id<>NEW.executor_id OR mr.status NOT IN ('SUCCEEDED','COMPLETED')
    OR mr.schema_valid IS DISTINCT FROM true OR a.completed_at IS NULL
    OR NOT (t.required_capabilities ? NEW.capability) THEN
   RAISE EXCEPTION 'E1 measurement must derive from matching accepted task, attempt, verified artifact and executor run';
 END IF;
 NEW.task_class:=t.task_type;
 NEW.quality:=1;
 NEW.verified:=true;
 -- Wall time comes from the coordinator-owned attempt clock, not a worker
 -- supplied latency field. Acceptance plus artifact verification supplies
 -- the deterministic binary quality observation for this neutral task class.
 NEW.latency_ms:=ceil(extract(epoch FROM (a.completed_at-a.started_at))*1000);
 NEW.cost_units:=mr.estimated_cost;
 RETURN NEW;
END; $$;
CREATE TRIGGER e1_measurement_runtime_evidence BEFORE INSERT ON evolution.e1_executor_measurements
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e1_measurement();

CREATE OR REPLACE FUNCTION evolution.prevent_genome_champion_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='CHAMPION' AND NEW.status<>'CHAMPION'
    AND (current_user<>'agentic_promotion_executor'
      OR current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized') THEN
   RAISE EXCEPTION 'active champion status requires the E1 promotion capability';
 END IF;
 IF NEW.status='CHAMPION' AND OLD.status<>'CHAMPION'
    AND (current_user<>'agentic_promotion_executor'
      OR current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized') THEN
   RAISE EXCEPTION 'only the governed E1 transaction may activate a champion';
 END IF;
 IF OLD.genome_id<>NEW.genome_id OR OLD.scope_id<>NEW.scope_id OR OLD.config_ref<>NEW.config_ref
    OR OLD.config_hash<>NEW.config_hash OR OLD.version<>NEW.version
    OR OLD.mutation_description<>NEW.mutation_description OR OLD.mutation_rationale<>NEW.mutation_rationale
    OR OLD.created_at<>NEW.created_at OR OLD.created_by<>NEW.created_by
    OR OLD.exploration_budget<>NEW.exploration_budget OR OLD.exploitation_budget<>NEW.exploitation_budget
    OR OLD.metadata<>NEW.metadata THEN RAISE EXCEPTION 'genome identity and configuration are immutable'; END IF;
 IF OLD.rollback_ref IS DISTINCT FROM NEW.rollback_ref
    AND (current_user<>'agentic_promotion_executor'
      OR current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized') THEN
   RAISE EXCEPTION 'rollback lineage may only be attached by governed promotion';
 END IF;
 RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION evolution.reject_e1_immutable_change() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION '% is immutable after insertion',TG_TABLE_NAME; END; $$;
CREATE TRIGGER e1_policy_immutable BEFORE UPDATE OR DELETE ON evolution.e1_scope_policies FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_genome_immutable BEFORE UPDATE OR DELETE ON evolution.e1_genome_versions FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_pack_immutable BEFORE UPDATE OR DELETE ON evolution.e1_evaluation_pack_versions FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_run_immutable BEFORE UPDATE OR DELETE ON evolution.e1_evaluation_runs FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_comparison_immutable BEFORE UPDATE OR DELETE ON evolution.e1_comparisons FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_decision_immutable BEFORE UPDATE OR DELETE ON evolution.e1_promotion_decisions FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_rollback_immutable BEFORE UPDATE OR DELETE ON evolution.e1_rollback_records FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_event_immutable BEFORE UPDATE OR DELETE ON evolution.e1_events FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_postcheck_immutable BEFORE UPDATE OR DELETE ON evolution.e1_postpromotion_checks FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE TRIGGER e1_measurements_immutable BEFORE UPDATE OR DELETE ON evolution.e1_executor_measurements FOR EACH ROW EXECUTE FUNCTION evolution.reject_e1_immutable_change();
CREATE OR REPLACE FUNCTION evolution.validate_e1_event_payload_hash() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected text;
BEGIN
 expected:=encode(sha256(convert_to(NEW.payload::text,'UTF8')),'hex');
 IF NEW.payload_hash IS DISTINCT FROM expected THEN
   RAISE EXCEPTION 'E1 event payload hash does not match canonical JSONB payload';
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER e1_event_hash_valid BEFORE INSERT ON evolution.e1_events
 FOR EACH ROW EXECUTE FUNCTION evolution.validate_e1_event_payload_hash();

CREATE OR REPLACE FUNCTION runtime.reject_e1_promotion_campaign_write() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF current_user='agentic_promotion_executor' THEN RAISE EXCEPTION 'promotion capability may lock but cannot mutate campaign state'; END IF;
 RETURN COALESCE(NEW,OLD);
END; $$;
CREATE TRIGGER e1_promotion_campaign_write_guard BEFORE UPDATE OR DELETE ON runtime.improvement_campaigns
 FOR EACH ROW EXECUTE FUNCTION runtime.reject_e1_promotion_campaign_write();

CREATE OR REPLACE FUNCTION evolution.authorize_and_execute_e1(
 p_authorization_id text,p_comparison_id text,p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,runtime,pg_temp AS $$
DECLARE c evolution.e1_comparisons%ROWTYPE;
DECLARE cp evolution.scope_champions%ROWTYPE;
DECLARE candidate evolution.e1_genome_versions%ROWTYPE;
DECLARE champion evolution.e1_genome_versions%ROWTYPE;
DECLARE baseline_run evolution.e1_evaluation_runs%ROWTYPE;
DECLARE pack evolution.e1_evaluation_pack_versions%ROWTYPE;
DECLARE policy evolution.e1_scope_policies%ROWTYPE;
DECLARE campaign runtime.improvement_campaigns%ROWTYPE;
DECLARE er evolution.e1_evaluation_runs%ROWTYPE;
DECLARE auth_hash text;
DECLARE decision_key text;
DECLARE ev_id text;
DECLARE event_payload jsonb;
DECLARE challenger_metrics jsonb;
DECLARE baseline_metrics jsonb;
DECLARE metric_key text;
DECLARE required_pct numeric;
DECLARE quality_key text;
DECLARE candidate_metric numeric;
DECLARE champion_metric numeric;
DECLARE candidate_quality numeric;
DECLARE champion_quality numeric;
DECLARE candidate_cost numeric;
DECLARE prior_auth evolution.e1_promotion_authorizations%ROWTYPE;
DECLARE accounted numeric;
DECLARE dimension_key text;
DECLARE limit_key text;
BEGIN
 IF p_actor IS DISTINCT FROM session_user THEN
   RAISE EXCEPTION 'promotion actor must match the authenticated database login';
 END IF;
 SELECT * INTO c FROM evolution.e1_comparisons WHERE comparison_id=p_comparison_id;
 IF NOT FOUND OR NOT c.eligible OR c.disposition<>'ELIGIBLE' THEN RAISE EXCEPTION 'comparison is not eligible'; END IF;
 SELECT * INTO prior_auth FROM evolution.e1_promotion_authorizations
   WHERE comparison_id=p_comparison_id OR (campaign_id=c.campaign_id AND candidate_genome_id=c.candidate_genome_id)
   ORDER BY authorized_at LIMIT 1 FOR UPDATE;
 IF FOUND THEN
   IF prior_auth.status='EXECUTED' THEN RETURN prior_auth.decision_id; END IF;
   IF prior_auth.status='STALE' THEN RETURN 'STALE'; END IF;
   IF prior_auth.status='REJECTED' AND prior_auth.decision_id IS NOT NULL THEN RETURN prior_auth.decision_id; END IF;
   RAISE EXCEPTION 'promotion authorization already exists';
 END IF;
 SELECT * INTO policy FROM evolution.e1_scope_policies WHERE scope_id=c.scope_id;
 SELECT * INTO cp FROM evolution.scope_champions WHERE scope_id=c.scope_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'champion pointer missing'; END IF;
 SELECT * INTO prior_auth FROM evolution.e1_promotion_authorizations
   WHERE comparison_id=p_comparison_id OR (campaign_id=c.campaign_id AND candidate_genome_id=c.candidate_genome_id)
   ORDER BY authorized_at LIMIT 1 FOR UPDATE;
 IF FOUND THEN
   IF prior_auth.status='EXECUTED' THEN RETURN prior_auth.decision_id; END IF;
   IF prior_auth.status='STALE' THEN RETURN 'STALE'; END IF;
   IF prior_auth.status='REJECTED' AND prior_auth.decision_id IS NOT NULL THEN RETURN prior_auth.decision_id; END IF;
   RAISE EXCEPTION 'promotion authorization already exists';
 END IF;
 SELECT * INTO candidate FROM evolution.e1_genome_versions WHERE genome_id=c.candidate_genome_id;
 SELECT * INTO champion FROM evolution.e1_genome_versions WHERE genome_id=c.champion_genome_id;
   SELECT * INTO er FROM evolution.e1_evaluation_runs WHERE run_id=c.candidate_run_id;
   SELECT * INTO pack FROM evolution.e1_evaluation_pack_versions WHERE pack_id=er.pack_id;
 IF cp.genome_id<>c.champion_genome_id THEN
   auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text)::bytea),'hex');
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,session_user,'STALE');
   RETURN 'STALE';
 END IF;
 SELECT metrics INTO challenger_metrics FROM evolution.e1_evaluation_runs WHERE run_id=c.candidate_run_id;
 SELECT metrics INTO baseline_metrics FROM evolution.e1_evaluation_runs WHERE run_id=c.champion_run_id;
 SELECT * INTO baseline_run FROM evolution.e1_evaluation_runs WHERE run_id=c.champion_run_id;
 SELECT * INTO campaign FROM runtime.improvement_campaigns WHERE campaign_id=c.campaign_id FOR UPDATE;
 IF NOT FOUND OR campaign.status IN ('STOPPED','COMPLETED') THEN RAISE EXCEPTION 'campaign is not active'; END IF;
 IF NOT EXISTS(SELECT 1 FROM evolution.eval_suite_versions s
       WHERE s.eval_suite_id=pack.suite_id AND s.version=pack.suite_version
         AND s.scope_id=pack.scope_id
         AND s.integrity_hash=pack.definition->'suite_binding'->>'integrity_hash') THEN
   RAISE EXCEPTION 'frozen evaluation pack is not bound to the recorded immutable suite version';
 END IF;
   metric_key:=policy.promotion_policy->>'improvement_metric';
   quality_key:=policy.promotion_policy->>'protected_quality_metric';
   required_pct:=coalesce((policy.promotion_policy->>'required_improvement')::numeric,0);
   IF metric_key IS NULL OR quality_key IS NULL OR required_pct<0 THEN
     RAISE EXCEPTION 'E1 promotion policy is incomplete';
   END IF;
   candidate_metric:=(challenger_metrics->'aggregate'->>metric_key)::numeric;
   champion_metric:=(baseline_metrics->'aggregate'->>metric_key)::numeric;
   candidate_quality:=(challenger_metrics->'aggregate'->>quality_key)::numeric;
   champion_quality:=(baseline_metrics->'aggregate'->>quality_key)::numeric;
   candidate_cost:=(challenger_metrics->'aggregate'->>'cost_units')::numeric;
   IF candidate_metric IS NULL OR champion_metric IS NULL OR candidate_quality IS NULL
      OR champion_quality IS NULL OR candidate_cost IS NULL THEN RAISE EXCEPTION 'required evaluation metric missing'; END IF;
   IF candidate.tier<>'E1' OR candidate.parent_genome_id<>champion.genome_id
      OR jsonb_typeof(candidate.changed_paths)<>'array'
      OR jsonb_array_length(candidate.changed_paths)=0 THEN
     RAISE EXCEPTION 'candidate is not a bounded child E1 genome';
   END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(candidate.changed_paths) AS changed(path)
       WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(policy.allowed_paths) AS allowed(pattern)
         WHERE changed.path LIKE replace(allowed.pattern,'*','%'))) THEN
     RAISE EXCEPTION 'candidate changed a field outside the registered E1 allowlist';
   END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(er.result_refs || baseline_run.result_refs) AS result(ref)
       WHERE NOT EXISTS(SELECT 1 FROM evolution.e1_executor_measurements m
         JOIN runtime.artifacts a ON a.artifact_id=m.artifact_ref
         JOIN runtime.tasks t ON t.task_id=m.task_id
         JOIN runtime.attempts at ON at.attempt_id=m.attempt_id
         WHERE m.measurement_id=result.ref AND m.scope_id=c.scope_id AND m.verified
           AND a.verification_status='VERIFIED' AND t.status='ACCEPTED' AND at.status='ACCEPTED')) THEN
     RAISE EXCEPTION 'evaluation cites missing or unverified measurement evidence';
   END IF;
   IF policy.promotion_policy->>'improvement_direction'='MIN' THEN
     IF candidate_metric > champion_metric*(1-required_pct) THEN RAISE EXCEPTION 'required improvement threshold not met'; END IF;
   ELSIF policy.promotion_policy->>'improvement_direction'='MAX' THEN
     IF candidate_metric < champion_metric*(1+required_pct) THEN RAISE EXCEPTION 'required improvement threshold not met'; END IF;
   ELSE RAISE EXCEPTION 'unknown improvement direction'; END IF;
   IF candidate_quality < champion_quality THEN
     RAISE EXCEPTION 'protected quality regression';
   END IF;
   IF candidate_cost >
      coalesce((policy.promotion_policy->>'maximum_cost_units')::numeric,1e100) THEN
     RAISE EXCEPTION 'cost gate failed';
   END IF;
 IF session_user IN (candidate.provenance->>'created_by',er.evaluator_id) THEN
   RAISE EXCEPTION 'promotion controller must be independent from proposer and evaluator';
 END IF;
 IF policy.mode='ACTIVE_E1' THEN
   IF NOT er.reproducible OR er.status<>'COMPLETED' OR er.evaluator_id=candidate.provenance->>'created_by'
      OR er.pack_id IS DISTINCT FROM pack.pack_id OR pack.scope_id<>c.scope_id
      OR er.genome_id<>candidate.genome_id OR er.baseline_genome_id<>champion.genome_id THEN
     RAISE EXCEPTION 'evaluation integrity or lineage gate failed';
   END IF;
   IF NOT EXISTS(SELECT 1 FROM evolution.e1_evaluation_runs cr WHERE cr.run_id=c.champion_run_id
      AND cr.status='COMPLETED' AND cr.reproducible AND cr.pack_id=pack.pack_id
      AND cr.genome_id=champion.genome_id) THEN RAISE EXCEPTION 'champion baseline was not evaluated under same pack'; END IF;
   IF EXISTS(SELECT 1 FROM runtime.improvement_reservations r WHERE r.campaign_id=c.campaign_id AND r.status='RESERVED') THEN
     RAISE EXCEPTION 'campaign has unsettled resource reservations';
   END IF;
   SELECT coalesce(sum(d.consumed),0) INTO accounted
     FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
     WHERE r.campaign_id=c.campaign_id AND d.dimension='experiment_units';
   IF campaign.budget ? 'max_experiment_units' AND accounted>(campaign.budget->>'max_experiment_units')::numeric THEN
     RAISE EXCEPTION 'campaign experiment budget exceeded';
   END IF;
   FOREACH dimension_key IN ARRAY ARRAY['monetary_cost','tokens_input','tokens_output'] LOOP
     limit_key:=CASE dimension_key WHEN 'monetary_cost' THEN 'max_cost'
       WHEN 'tokens_input' THEN 'max_tokens_input' WHEN 'tokens_output' THEN 'max_tokens_output' END;
     IF campaign.budget ? limit_key THEN
       IF EXISTS(SELECT 1 FROM runtime.improvement_reservations r
           JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
           WHERE r.campaign_id=c.campaign_id AND d.dimension=dimension_key AND d.unknown) THEN
         RAISE EXCEPTION 'unknown usage cannot satisfy hard campaign ceiling %',limit_key;
       END IF;
       SELECT coalesce(sum(d.consumed),0) INTO accounted
         FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
         WHERE r.campaign_id=c.campaign_id AND d.dimension=dimension_key;
       IF accounted>(campaign.budget->>limit_key)::numeric THEN RAISE EXCEPTION 'campaign hard budget exceeded for %',dimension_key; END IF;
     END IF;
   END LOOP;
   IF champion.config_hash<> (SELECT config_hash FROM evolution.system_genomes WHERE genome_id=cp.genome_id)
      OR candidate.config_hash<>(SELECT config_hash FROM evolution.system_genomes WHERE genome_id=candidate.genome_id) THEN
     auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text)::bytea),'hex');
     INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
     VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,p_actor,'STALE');
     RETURN 'STALE';
   END IF;
   auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text||
       er.result_refs::text||baseline_run.result_refs::text||policy.policy_hash)::bytea),'hex');
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,champion.genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,session_user,'AUTHORIZED');
   decision_key:='e1decision_'||replace(p_authorization_id,'e1auth_','');
   PERFORM set_config('evolution.governed_promotion','authorized',true);
   UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=champion.genome_id;
   UPDATE evolution.system_genomes SET status='CHAMPION',rollback_ref=champion.genome_id WHERE genome_id=candidate.genome_id;
   UPDATE evolution.scope_champions SET genome_id=candidate.genome_id,updated_at=now() WHERE scope_id=c.scope_id;
   INSERT INTO evolution.e1_promotion_decisions(decision_id,authorization_id,scope_id,campaign_id,
       prior_champion_id,promoted_genome_id,comparison_id,pack_id,evidence_hash,decision,mode,created_by)
   VALUES(decision_key,p_authorization_id,c.scope_id,c.campaign_id,champion.genome_id,candidate.genome_id,
       c.comparison_id,pack.pack_id,auth_hash,'PROMOTED','ACTIVE_E1',session_user);
   UPDATE evolution.e1_promotion_authorizations SET status='EXECUTED',decision_id=decision_key
       WHERE authorization_id=p_authorization_id;
   ev_id:='e1evt_'||replace(p_authorization_id,'e1auth_','');
   event_payload:=jsonb_build_object('authorization_id',p_authorization_id,'decision_id',decision_key,
       'from',champion.genome_id,'to',candidate.genome_id,'pack_hash',pack.definition_hash);
   INSERT INTO evolution.e1_events(event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
   VALUES(ev_id,c.scope_id,c.campaign_id,'E1_CHAMPION_PROMOTED',session_user,event_payload,
       encode(sha256(convert_to(event_payload::text,'UTF8')),'hex'));
   RETURN decision_key;
 END IF;
 IF policy.mode='SHADOW' THEN
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       encode(sha256((candidate.config_hash||pack.definition_hash)::bytea),'hex'),policy.policy_hash,p_actor,'REJECTED');
   decision_key:='e1decision_'||replace(p_authorization_id,'e1auth_','');
   INSERT INTO evolution.e1_promotion_decisions(decision_id,authorization_id,scope_id,campaign_id,
       prior_champion_id,promoted_genome_id,comparison_id,pack_id,evidence_hash,decision,mode,created_by)
   VALUES(decision_key,p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,c.candidate_genome_id,
       c.comparison_id,pack.pack_id,encode(sha256((candidate.config_hash||pack.definition_hash)::bytea),'hex'),
       'WOULD_PROMOTE','SHADOW',session_user);
   UPDATE evolution.e1_promotion_authorizations SET decision_id=decision_key WHERE authorization_id=p_authorization_id;
   event_payload:=jsonb_build_object('authorization_id',p_authorization_id,'decision_id',decision_key,
       'candidate_genome_id',candidate.genome_id,'comparison_id',c.comparison_id,
       'pack_hash',pack.definition_hash,'mode','SHADOW','decision','WOULD_PROMOTE');
   ev_id:='e1evt_'||replace(p_authorization_id,'e1auth_','');
   INSERT INTO evolution.e1_events(event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
   VALUES(ev_id,c.scope_id,c.campaign_id,'E1_SHADOW_DECISION_RECORDED',session_user,event_payload,
       encode(sha256(convert_to(event_payload::text,'UTF8')),'hex'));
   RETURN 'WOULD_PROMOTE';
 END IF;
 RAISE EXCEPTION 'candidate, baseline or mode is stale/unauthorized';
END; $$;

CREATE OR REPLACE FUNCTION evolution.rollback_e1_on_failed_check(p_check_id text,p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,runtime,pg_temp AS $$
DECLARE chk evolution.e1_postpromotion_checks%ROWTYPE;
DECLARE dec evolution.e1_promotion_decisions%ROWTYPE;
DECLARE current_id text;
DECLARE restored text;
DECLARE failed text;
DECLARE rollback_id text;
DECLARE event_payload jsonb;
BEGIN
 IF p_actor IS DISTINCT FROM session_user THEN
   RAISE EXCEPTION 'rollback actor must match the authenticated database login';
 END IF;
 SELECT * INTO chk FROM evolution.e1_postpromotion_checks WHERE check_id=p_check_id;
 IF NOT FOUND OR chk.passed THEN RAISE EXCEPTION 'failed post-promotion check required'; END IF;
 SELECT * INTO dec FROM evolution.e1_promotion_decisions WHERE decision_id=chk.decision_id;
 SELECT genome_id INTO current_id FROM evolution.scope_champions WHERE scope_id=chk.scope_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM evolution.e1_rollback_records WHERE decision_id=dec.decision_id) THEN
   SELECT restored_genome_id INTO restored FROM evolution.e1_rollback_records WHERE decision_id=dec.decision_id;
   RETURN restored;
 END IF;
 IF current_id IS DISTINCT FROM dec.promoted_genome_id THEN RAISE EXCEPTION 'active genome changed since promotion'; END IF;
 SELECT rollback_ref INTO restored FROM evolution.system_genomes WHERE genome_id=current_id;
 IF restored IS NULL OR NOT EXISTS(SELECT 1 FROM evolution.e1_genome_versions WHERE genome_id=restored) THEN
   RAISE EXCEPTION 'rollback target is missing or not a valid E1 genome';
 END IF;
 failed:=current_id;
 PERFORM set_config('evolution.governed_promotion','authorized',true);
 UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=failed;
 UPDATE evolution.system_genomes SET status='CHAMPION' WHERE genome_id=restored;
 UPDATE evolution.scope_champions SET genome_id=restored,updated_at=now() WHERE scope_id=chk.scope_id;
 rollback_id:='e1rollback_'||substr(encode(sha256((dec.decision_id||p_check_id)::bytea),'hex'),1,24);
 INSERT INTO evolution.e1_rollback_records(rollback_id,scope_id,decision_id,failed_genome_id,
     restored_genome_id,check_id,reason,created_by)
 VALUES(rollback_id,chk.scope_id,dec.decision_id,failed,restored,p_check_id,chk.failure_class,p_actor);
 event_payload:=jsonb_build_object('failed',failed,'restored',restored,'failure_class',chk.failure_class,
     'decision_id',dec.decision_id,'check_id',p_check_id);
 INSERT INTO evolution.e1_events(event_id,scope_id,campaign_id,event_type,actor_id,causation_id,payload,payload_hash)
 VALUES(rollback_id,chk.scope_id,dec.campaign_id,'E1_CHAMPION_ROLLED_BACK',p_actor,dec.decision_id,
     event_payload,encode(sha256(convert_to(event_payload::text,'UTF8')),'hex'));
 RETURN restored;
END; $$;

REVOKE ALL ON FUNCTION evolution.authorize_and_execute_e1(text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION evolution.rollback_e1_on_failed_check(text,text) FROM PUBLIC;
