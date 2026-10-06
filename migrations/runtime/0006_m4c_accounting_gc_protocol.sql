CREATE TABLE runtime.improvement_reservations (
 reservation_id text PRIMARY KEY,
 campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
 idempotency_key text NOT NULL,
 budget_class text NOT NULL CHECK (budget_class IN ('EXPLOIT','EXPLORE')),
 stage text NOT NULL,
 status text NOT NULL CHECK (status IN ('RESERVED','SETTLED','RELEASED','UNKNOWN')),
 dispatch_state text NOT NULL CHECK (dispatch_state IN ('NOT_DISPATCHED','DISPATCHED','TERMINAL')),
 usage_status text NOT NULL CHECK (usage_status IN ('KNOWN','UNKNOWN')),
 task_id text REFERENCES runtime.tasks(task_id),
 attempt_id text REFERENCES runtime.attempts(attempt_id),
 created_at timestamptz NOT NULL DEFAULT now(),
 settled_at timestamptz,
 metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
 UNIQUE(campaign_id,idempotency_key)
);
CREATE TABLE runtime.improvement_reservation_dimensions (
 reservation_id text NOT NULL REFERENCES runtime.improvement_reservations(reservation_id),
 dimension text NOT NULL CHECK (dimension IN ('experiment_units','child_tasks','hypotheses','challengers','evaluations','attempts','wall_time','tokens_input','tokens_output','monetary_cost')),
 reserved numeric,
 consumed numeric,
 released numeric,
 unknown boolean NOT NULL DEFAULT false,
 PRIMARY KEY(reservation_id,dimension),
 CHECK (reserved IS NULL OR reserved >= 0),
 CHECK (consumed IS NULL OR consumed >= 0),
 CHECK (released IS NULL OR released >= 0)
);
CREATE INDEX improvement_reservations_recovery_idx ON runtime.improvement_reservations(status,dispatch_state,created_at);

CREATE OR REPLACE FUNCTION runtime.lock_artifact_references() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,runtime,evolution AS $$
DECLARE artifact_ref text;
BEGIN
 IF TG_TABLE_SCHEMA='evolution' AND TG_TABLE_NAME='evolution_events'
    AND to_jsonb(NEW)->>'event_type'='ARTIFACT_GC_SWEPT' THEN
   RETURN NEW;
 END IF;
 FOR artifact_ref IN
   SELECT DISTINCT captures[1]
   FROM regexp_matches(to_jsonb(NEW)::text, '(sha256:[0-9a-f]{64})', 'g') AS matches(captures)
   ORDER BY captures[1]
 LOOP
   PERFORM pg_advisory_xact_lock(hashtextextended(artifact_ref,0));
   IF EXISTS (SELECT 1 FROM runtime.artifact_gc_candidates WHERE artifact_id=artifact_ref AND status='SWEPT') THEN
     RAISE EXCEPTION 'artifact % has been swept and cannot be referenced', artifact_ref;
   END IF;
 END LOOP;
 RETURN NEW;
END; $$;

DO $$
DECLARE target text;
BEGIN
 FOREACH target IN ARRAY ARRAY['runtime.tasks','runtime.attempts','runtime.model_runs','runtime.artifacts',
  'runtime.knowledge_objects','runtime.events','runtime.outbox'] LOOP
  EXECUTE format('CREATE TRIGGER artifact_reference_lock BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references()',target);
 END LOOP;
END $$;
