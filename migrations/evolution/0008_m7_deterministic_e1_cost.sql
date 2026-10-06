-- A generic deterministic remote sandbox performs no provider call. When its
-- persisted ModelRun has no provider/model and no explicit cost, its known
-- billable cost is zero. Model-backed or otherwise unknown usage stays NULL.
CREATE OR REPLACE FUNCTION evolution.normalize_deterministic_e1_cost()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.cost_units IS NULL AND EXISTS (
   SELECT 1 FROM runtime.model_runs mr
   WHERE mr.model_run_id=NEW.model_run_id
     AND mr.adapter_type='remote_worker'
     AND mr.provider IS NULL
     AND mr.requested_model IS NULL
     AND mr.resolved_model IS NULL
     AND mr.estimated_cost IS NULL
 ) THEN
   NEW.cost_units:=0;
 END IF;
 RETURN NEW;
END; $$;

-- PostgreSQL fires same-event triggers alphabetically. This runs after the
-- runtime-evidence trigger has validated and derived the measurement fields.
CREATE TRIGGER e1_measurement_runtime_zcost
BEFORE INSERT ON evolution.e1_executor_measurements
FOR EACH ROW EXECUTE FUNCTION evolution.normalize_deterministic_e1_cost();
