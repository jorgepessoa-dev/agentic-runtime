CREATE TABLE evolution.evaluation_packs (
 pack_id text PRIMARY KEY,
 scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
 candidate_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 champion_genome_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 eval_suite_id text NOT NULL,
 eval_suite_version text NOT NULL,
 pack_hash text NOT NULL CHECK(pack_hash ~ '^[0-9a-f]{64}$'),
 definition jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 created_by text NOT NULL,
 FOREIGN KEY(eval_suite_id,eval_suite_version) REFERENCES evolution.eval_suite_versions(eval_suite_id,version),
 CHECK(candidate_genome_id<>champion_genome_id)
);
CREATE TABLE evolution.evaluator_runs (
 evaluator_run_id text PRIMARY KEY,
 evaluation_id text NOT NULL REFERENCES evolution.evaluation_records(evaluation_id),
 evaluator_id text NOT NULL,
 evaluator_type text NOT NULL,
 implementation_version text NOT NULL,
 independence_relation text NOT NULL CHECK(independence_relation IN ('DETERMINISTIC','INDEPENDENT_FAMILY','SAME_FAMILY','UNKNOWN')),
 execution_ref text,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE evolution.promotion_authorizations (
 authorization_id text PRIMARY KEY,
 scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
 expected_champion_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 challenger_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 shadow_decision_id text NOT NULL REFERENCES evolution.promotion_decisions(decision_id),
 evaluation_refs jsonb NOT NULL,
 authorized_by text NOT NULL,
 authorized_at timestamptz NOT NULL DEFAULT now(),
 action text NOT NULL CHECK(action IN ('PROMOTE','ROLLBACK')),
 status text NOT NULL CHECK(status IN ('AUTHORIZED','EXECUTED','STALE','REJECTED'))
);
CREATE TABLE evolution.champion_history (
 history_id text PRIMARY KEY,
 scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
 prior_champion_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 resulting_champion_id text NOT NULL REFERENCES evolution.system_genomes(genome_id),
 authorization_id text NOT NULL REFERENCES evolution.promotion_authorizations(authorization_id),
 action text NOT NULL CHECK(action IN ('PROMOTE','ROLLBACK')),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(authorization_id)
);
CREATE TABLE evolution.evolution_events (
 event_id text PRIMARY KEY,
 event_type text NOT NULL,
 scope_id text REFERENCES evolution.scopes(scope_id),
 actor_id text NOT NULL,
 causation_ref text,
 payload jsonb NOT NULL,
 payload_hash text NOT NULL CHECK(payload_hash ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER scope_champion_shadow_lock ON evolution.scope_champions;
CREATE OR REPLACE FUNCTION evolution.guard_governed_champion_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized' THEN
   RAISE EXCEPTION 'champion pointer changes require governed promotion service';
 END IF;
 RETURN COALESCE(NEW,OLD);
END; $$;
CREATE TRIGGER scope_champion_governed BEFORE UPDATE OR DELETE ON evolution.scope_champions
 FOR EACH ROW EXECUTE FUNCTION evolution.guard_governed_champion_change();
CREATE OR REPLACE FUNCTION evolution.prevent_genome_champion_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='CHAMPION' AND NEW.status <> 'CHAMPION'
   AND current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized' THEN
   RAISE EXCEPTION 'active champion status requires governed promotion service';
 END IF;
 IF OLD.genome_id <> NEW.genome_id OR OLD.scope_id <> NEW.scope_id
   OR OLD.config_ref <> NEW.config_ref OR OLD.config_hash <> NEW.config_hash
   OR OLD.version <> NEW.version OR OLD.mutation_description <> NEW.mutation_description
   OR OLD.mutation_rationale <> NEW.mutation_rationale OR OLD.created_at <> NEW.created_at
   OR OLD.created_by <> NEW.created_by OR OLD.rollback_ref IS DISTINCT FROM NEW.rollback_ref
   OR OLD.exploration_budget <> NEW.exploration_budget OR OLD.exploitation_budget <> NEW.exploitation_budget
   OR OLD.metadata <> NEW.metadata THEN
   RAISE EXCEPTION 'genome identity and configuration are immutable';
 END IF;
 RETURN NEW;
END; $$;
CREATE OR REPLACE FUNCTION evolution.immutable_m4_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION '% is append-only',TG_TABLE_NAME; END; $$;
CREATE TRIGGER evaluation_packs_immutable BEFORE UPDATE OR DELETE ON evolution.evaluation_packs FOR EACH ROW EXECUTE FUNCTION evolution.immutable_m4_row();
CREATE TRIGGER evaluator_runs_immutable BEFORE UPDATE OR DELETE ON evolution.evaluator_runs FOR EACH ROW EXECUTE FUNCTION evolution.immutable_m4_row();
CREATE TRIGGER champion_history_immutable BEFORE UPDATE OR DELETE ON evolution.champion_history FOR EACH ROW EXECUTE FUNCTION evolution.immutable_m4_row();
CREATE TRIGGER evolution_events_immutable_m4 BEFORE UPDATE OR DELETE ON evolution.evolution_events FOR EACH ROW EXECUTE FUNCTION evolution.immutable_m4_row();
