CREATE SCHEMA IF NOT EXISTS evolution;

CREATE TABLE evolution.scopes (
    scope_id text PRIMARY KEY,
    description text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE evolution.system_genomes (
    genome_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    version text NOT NULL,
    status text NOT NULL CHECK (status IN ('DRAFT','CHALLENGER','CHAMPION','REJECTED','RETAINED_FOR_DIVERSITY','SUPERSEDED')),
    config_ref text NOT NULL,
    config_hash text NOT NULL CHECK (config_hash ~ '^[0-9a-f]{64}$'),
    mutation_description text NOT NULL,
    mutation_rationale text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    rollback_ref text,
    exploration_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    exploitation_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (scope_id, version),
    UNIQUE (genome_id, scope_id)
);
CREATE UNIQUE INDEX one_champion_per_scope ON evolution.system_genomes(scope_id) WHERE status='CHAMPION';

CREATE TABLE evolution.genome_parents (
    genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    parent_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    PRIMARY KEY (genome_id, parent_genome_id),
    CHECK (genome_id <> parent_genome_id)
);

CREATE TABLE evolution.scope_champions (
    scope_id text PRIMARY KEY REFERENCES evolution.scopes(scope_id),
    genome_id text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (genome_id, scope_id) REFERENCES evolution.system_genomes(genome_id, scope_id)
);
CREATE OR REPLACE FUNCTION evolution.require_champion_pointer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM evolution.system_genomes g
                   WHERE g.genome_id=NEW.genome_id AND g.scope_id=NEW.scope_id AND g.status='CHAMPION') THEN
        RAISE EXCEPTION 'scope champion pointer must reference a CHAMPION genome in the same scope';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER scope_champion_valid BEFORE INSERT OR UPDATE ON evolution.scope_champions
    FOR EACH ROW EXECUTE FUNCTION evolution.require_champion_pointer();

CREATE TABLE evolution.observations (
    observation_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    source_refs jsonb NOT NULL,
    summary text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE evolution.mutation_proposals (
    mutation_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    observation_refs jsonb NOT NULL,
    parent_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    candidate_config_ref text NOT NULL,
    candidate_config_hash text NOT NULL CHECK (candidate_config_hash ~ '^[0-9a-f]{64}$'),
    hypothesis text NOT NULL,
    expected_effect jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by text NOT NULL,
    status text NOT NULL CHECK (status IN ('PROPOSED','REGISTERED','EVALUATING','DECIDED','REJECTED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE evolution.eval_suite_versions (
    eval_suite_id text NOT NULL,
    version text NOT NULL,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    status text NOT NULL CHECK (status IN ('DRAFT','AUTHORITATIVE','SUPERSEDED','REJECTED')),
    definition_ref text NOT NULL,
    integrity_hash text NOT NULL CHECK (integrity_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    supersedes_eval_suite_id text,
    supersedes_version text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (eval_suite_id, version),
    FOREIGN KEY (supersedes_eval_suite_id, supersedes_version)
        REFERENCES evolution.eval_suite_versions(eval_suite_id, version),
    CHECK ((supersedes_eval_suite_id IS NULL) = (supersedes_version IS NULL))
);
CREATE UNIQUE INDEX one_authoritative_eval_suite_per_scope ON evolution.eval_suite_versions(scope_id)
    WHERE status='AUTHORITATIVE';

CREATE TABLE evolution.eval_suite_change_proposals (
    change_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    observation_refs jsonb NOT NULL,
    current_eval_suite_id text NOT NULL,
    current_eval_suite_version text NOT NULL,
    proposed_definition_ref text NOT NULL,
    proposed_integrity_hash text NOT NULL CHECK (proposed_integrity_hash ~ '^[0-9a-f]{64}$'),
    rationale text NOT NULL,
    status text NOT NULL CHECK (status IN ('PROPOSED','INDEPENDENT_REVIEW','VALIDATED','REJECTED','AUTHORIZED')),
    created_by text NOT NULL,
    independent_reviewer text,
    authorized_by text,
    validation_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (current_eval_suite_id, current_eval_suite_version)
        REFERENCES evolution.eval_suite_versions(eval_suite_id, version)
);

CREATE TABLE evolution.evaluation_records (
    evaluation_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    champion_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    eval_suite_id text NOT NULL,
    eval_suite_version text NOT NULL,
    conditions_ref text NOT NULL,
    evaluator_version text NOT NULL,
    evaluator_id text NOT NULL,
    result_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL CHECK (status IN ('BOUND','RUNNING','COMPLETED','FAILED','QUARANTINED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    FOREIGN KEY (eval_suite_id, eval_suite_version)
        REFERENCES evolution.eval_suite_versions(eval_suite_id, version),
    CHECK (candidate_genome_id <> champion_genome_id)
);
CREATE INDEX evaluations_candidate_idx ON evolution.evaluation_records(candidate_genome_id, created_at);

CREATE TABLE evolution.promotion_decisions (
    decision_id text PRIMARY KEY,
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    candidate_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    champion_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
    evaluation_refs jsonb NOT NULL,
    decision text NOT NULL CHECK (decision IN ('WOULD_PROMOTE','REJECT','RETAIN_FOR_DIVERSITY')),
    mode text NOT NULL CHECK (mode='SHADOW'),
    rationale_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    CHECK (candidate_genome_id <> champion_genome_id)
);

CREATE TABLE evolution.events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    scope_id text REFERENCES evolution.scopes(scope_id),
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    causation_id text,
    correlation_id text NOT NULL,
    schema_version text NOT NULL,
    payload jsonb NOT NULL,
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

CREATE OR REPLACE FUNCTION evolution.prevent_genome_champion_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status='CHAMPION' AND NEW.status <> 'CHAMPION' THEN
        RAISE EXCEPTION 'shadow evolution cannot alter an active champion';
    END IF;
    IF OLD.genome_id <> NEW.genome_id OR OLD.scope_id <> NEW.scope_id
       OR OLD.config_ref <> NEW.config_ref OR OLD.config_hash <> NEW.config_hash
       OR OLD.version <> NEW.version OR OLD.mutation_description <> NEW.mutation_description
       OR OLD.mutation_rationale <> NEW.mutation_rationale OR OLD.created_at <> NEW.created_at
       OR OLD.created_by <> NEW.created_by OR OLD.rollback_ref IS DISTINCT FROM NEW.rollback_ref
       OR OLD.exploration_budget <> NEW.exploration_budget
       OR OLD.exploitation_budget <> NEW.exploitation_budget
       OR OLD.metadata <> NEW.metadata THEN
        RAISE EXCEPTION 'genome identity and configuration are immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER genome_guard BEFORE UPDATE ON evolution.system_genomes
    FOR EACH ROW EXECUTE FUNCTION evolution.prevent_genome_champion_mutation();
CREATE TRIGGER genome_no_delete BEFORE DELETE ON evolution.system_genomes
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
CREATE TRIGGER genome_parents_no_update BEFORE UPDATE OR DELETE ON evolution.genome_parents
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();

CREATE OR REPLACE FUNCTION evolution.prevent_eval_suite_definition_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.eval_suite_id <> NEW.eval_suite_id OR OLD.version <> NEW.version
       OR OLD.scope_id <> NEW.scope_id OR OLD.definition_ref <> NEW.definition_ref
       OR OLD.integrity_hash <> NEW.integrity_hash OR OLD.created_at <> NEW.created_at
       OR OLD.created_by <> NEW.created_by OR OLD.supersedes_eval_suite_id IS DISTINCT FROM NEW.supersedes_eval_suite_id
       OR OLD.supersedes_version IS DISTINCT FROM NEW.supersedes_version
       OR OLD.metadata <> NEW.metadata THEN
        RAISE EXCEPTION 'evaluation suite definition is immutable';
    END IF;
    IF OLD.status <> NEW.status AND NOT (
        (OLD.status='DRAFT' AND NEW.status IN ('AUTHORITATIVE','REJECTED')) OR
        (OLD.status='AUTHORITATIVE' AND NEW.status='SUPERSEDED')) THEN
        RAISE EXCEPTION 'invalid evaluation suite lifecycle transition';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER eval_suite_guard BEFORE UPDATE ON evolution.eval_suite_versions
    FOR EACH ROW EXECUTE FUNCTION evolution.prevent_eval_suite_definition_mutation();
CREATE TRIGGER eval_suite_no_delete BEFORE DELETE ON evolution.eval_suite_versions
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();

CREATE OR REPLACE FUNCTION evolution.guard_eval_suite_change_proposal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.change_id <> NEW.change_id OR OLD.scope_id <> NEW.scope_id
       OR OLD.observation_refs <> NEW.observation_refs
       OR OLD.current_eval_suite_id <> NEW.current_eval_suite_id
       OR OLD.current_eval_suite_version <> NEW.current_eval_suite_version
       OR OLD.proposed_definition_ref <> NEW.proposed_definition_ref
       OR OLD.proposed_integrity_hash <> NEW.proposed_integrity_hash
       OR OLD.rationale <> NEW.rationale OR OLD.created_by <> NEW.created_by
       OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'evaluation suite change proposal definition is immutable';
    END IF;
    IF OLD.status <> NEW.status AND NOT (
        (OLD.status='PROPOSED' AND NEW.status='INDEPENDENT_REVIEW') OR
        (OLD.status='INDEPENDENT_REVIEW' AND NEW.status IN ('VALIDATED','REJECTED')) OR
        (OLD.status='VALIDATED' AND NEW.status='AUTHORIZED')) THEN
        RAISE EXCEPTION 'invalid evaluation suite proposal transition';
    END IF;
    IF OLD.status IN ('VALIDATED','AUTHORIZED','REJECTED') AND
       (OLD.independent_reviewer IS DISTINCT FROM NEW.independent_reviewer
        OR OLD.validation_refs IS DISTINCT FROM NEW.validation_refs) THEN
        RAISE EXCEPTION 'completed suite review record is immutable';
    END IF;
    IF OLD.status IN ('AUTHORIZED','REJECTED') AND OLD.authorized_by IS DISTINCT FROM NEW.authorized_by THEN
        RAISE EXCEPTION 'suite authorization identity is immutable';
    END IF;
    IF NEW.status IN ('VALIDATED','AUTHORIZED') AND
       (NEW.independent_reviewer IS NULL OR NEW.independent_reviewer=NEW.created_by
        OR jsonb_array_length(NEW.validation_refs)=0) THEN
        RAISE EXCEPTION 'suite change requires independent review and validation evidence';
    END IF;
    IF NEW.status='AUTHORIZED' AND
       (NEW.authorized_by IS NULL OR NEW.authorized_by=NEW.created_by
        OR NEW.authorized_by=NEW.independent_reviewer) THEN
        RAISE EXCEPTION 'suite change authorization requires a distinct governance authority';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER eval_suite_change_guard BEFORE UPDATE ON evolution.eval_suite_change_proposals
    FOR EACH ROW EXECUTE FUNCTION evolution.guard_eval_suite_change_proposal();
CREATE TRIGGER eval_suite_change_no_delete BEFORE DELETE ON evolution.eval_suite_change_proposals
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();

CREATE OR REPLACE FUNCTION evolution.prevent_evaluation_rebind() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.evaluation_id <> NEW.evaluation_id OR OLD.scope_id <> NEW.scope_id
       OR OLD.candidate_genome_id <> NEW.candidate_genome_id
       OR OLD.champion_genome_id <> NEW.champion_genome_id
       OR OLD.eval_suite_id <> NEW.eval_suite_id OR OLD.eval_suite_version <> NEW.eval_suite_version
       OR OLD.conditions_ref <> NEW.conditions_ref OR OLD.evaluator_version <> NEW.evaluator_version
       OR OLD.created_at <> NEW.created_at THEN
        RAISE EXCEPTION 'evaluation binding is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER evaluation_binding_guard BEFORE UPDATE ON evolution.evaluation_records
    FOR EACH ROW EXECUTE FUNCTION evolution.prevent_evaluation_rebind();

CREATE OR REPLACE FUNCTION evolution.reject_immutable_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is immutable in shadow evolution', TG_TABLE_NAME;
END;
$$;
CREATE TRIGGER evaluation_record_immutable BEFORE UPDATE OR DELETE ON evolution.evaluation_records
    FOR EACH ROW EXECUTE FUNCTION evolution.reject_immutable_change();
CREATE TRIGGER promotion_decision_immutable BEFORE UPDATE OR DELETE ON evolution.promotion_decisions
    FOR EACH ROW EXECUTE FUNCTION evolution.reject_immutable_change();
CREATE TRIGGER scope_champion_shadow_lock BEFORE UPDATE OR DELETE ON evolution.scope_champions
    FOR EACH ROW EXECUTE FUNCTION evolution.reject_immutable_change();
CREATE TRIGGER evolution_events_immutable BEFORE UPDATE OR DELETE ON evolution.events
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
