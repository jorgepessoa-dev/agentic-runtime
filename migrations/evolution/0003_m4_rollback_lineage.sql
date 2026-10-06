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
   OR OLD.created_by <> NEW.created_by OR OLD.exploration_budget <> NEW.exploration_budget
   OR OLD.exploitation_budget <> NEW.exploitation_budget OR OLD.metadata <> NEW.metadata
   OR (OLD.rollback_ref IS DISTINCT FROM NEW.rollback_ref
       AND current_setting('evolution.governed_promotion',true) IS DISTINCT FROM 'authorized') THEN
   RAISE EXCEPTION 'genome identity and configuration are immutable';
 END IF;
 RETURN NEW;
END; $$;
