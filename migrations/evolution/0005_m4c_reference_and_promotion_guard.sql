DO $$
DECLARE target text;
BEGIN
 FOREACH target IN ARRAY ARRAY[
  'evolution.eval_suite_versions','evolution.evaluation_packs','evolution.evaluation_records',
  'evolution.eval_suite_change_proposals','evolution.evaluator_runs',
  'evolution.system_genomes','evolution.mutation_proposals','evolution.promotion_decisions',
  'evolution.promotion_authorizations','evolution.evolution_events','evolution.governance_events'
 ] LOOP
  EXECUTE format('CREATE TRIGGER artifact_reference_lock BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references()',target);
 END LOOP;
END $$;

CREATE OR REPLACE FUNCTION evolution.guard_governed_champion_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF current_user <> 'agentic_promotion_executor' THEN
   RAISE EXCEPTION 'champion pointer changes require controlled promotion procedure';
 END IF;
 RETURN COALESCE(NEW,OLD);
END; $$;
CREATE OR REPLACE FUNCTION evolution.prevent_genome_champion_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='CHAMPION' AND NEW.status <> 'CHAMPION'
   AND current_user <> 'agentic_promotion_executor' THEN
   RAISE EXCEPTION 'active champion status requires controlled promotion procedure';
 END IF;
 IF OLD.genome_id <> NEW.genome_id OR OLD.scope_id <> NEW.scope_id
   OR OLD.config_ref <> NEW.config_ref OR OLD.config_hash <> NEW.config_hash
   OR OLD.version <> NEW.version OR OLD.mutation_description <> NEW.mutation_description
   OR OLD.mutation_rationale <> NEW.mutation_rationale OR OLD.created_at <> NEW.created_at
   OR OLD.created_by <> NEW.created_by
   OR (OLD.rollback_ref IS DISTINCT FROM NEW.rollback_ref AND current_user <> 'agentic_promotion_executor')
   OR OLD.exploration_budget <> NEW.exploration_budget OR OLD.exploitation_budget <> NEW.exploitation_budget
   OR OLD.metadata <> NEW.metadata THEN
   RAISE EXCEPTION 'genome identity and configuration are immutable';
 END IF;
 RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION evolution.authorize_promotion(
 p_authorization_id text,p_scope_id text,p_expected_champion text,p_challenger text,
 p_decision_id text,p_evaluation_refs jsonb,p_authorized_by text,p_action text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,pg_temp AS $$
DECLARE d evolution.promotion_decisions%ROWTYPE;
DECLARE active_id text;
BEGIN
 IF p_action NOT IN ('PROMOTE','ROLLBACK') OR jsonb_typeof(p_evaluation_refs) IS DISTINCT FROM 'array' THEN
   RAISE EXCEPTION 'invalid promotion authorization request';
 END IF;
 SELECT * INTO d FROM evolution.promotion_decisions WHERE decision_id=p_decision_id;
 IF NOT FOUND OR d.scope_id<>p_scope_id OR d.decision<>'WOULD_PROMOTE'
    OR d.evaluation_refs<>p_evaluation_refs THEN
   RAISE EXCEPTION 'authorization requires the complete positive shadow decision';
 END IF;
 IF p_action='PROMOTE' AND (d.candidate_genome_id<>p_challenger OR d.champion_genome_id<>p_expected_champion) THEN
   RAISE EXCEPTION 'promotion decision does not match the requested challenger and champion';
 END IF;
 IF p_action='ROLLBACK' AND (d.candidate_genome_id<>p_expected_champion OR d.champion_genome_id<>p_challenger
      OR NOT EXISTS(SELECT 1 FROM evolution.system_genomes WHERE genome_id=p_expected_champion AND rollback_ref=p_challenger)) THEN
   RAISE EXCEPTION 'rollback target is not the evaluated prior champion';
 END IF;
 SELECT genome_id INTO active_id FROM evolution.scope_champions WHERE scope_id=p_scope_id FOR UPDATE;
 IF active_id IS DISTINCT FROM p_expected_champion THEN RAISE EXCEPTION 'authorization expected champion is stale'; END IF;
 IF p_authorized_by IN (d.created_by,(SELECT created_by FROM evolution.system_genomes WHERE genome_id=d.candidate_genome_id))
    OR EXISTS(SELECT 1 FROM evolution.evaluation_records WHERE evaluation_id IN
        (SELECT jsonb_array_elements_text(p_evaluation_refs)) AND evaluator_id=p_authorized_by) THEN
   RAISE EXCEPTION 'authorization identity must be independent of proposer and evaluator';
 END IF;
 INSERT INTO evolution.promotion_authorizations
  (authorization_id,scope_id,expected_champion_id,challenger_id,shadow_decision_id,evaluation_refs,authorized_by,action,status)
 VALUES (p_authorization_id,p_scope_id,p_expected_champion,p_challenger,p_decision_id,p_evaluation_refs,p_authorized_by,p_action,'AUTHORIZED');
END; $$;
REVOKE ALL ON FUNCTION evolution.authorize_promotion(text,text,text,text,text,jsonb,text,text) FROM PUBLIC;
