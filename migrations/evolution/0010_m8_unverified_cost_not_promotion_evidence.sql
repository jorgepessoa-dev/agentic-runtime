-- Remote workers may return token claims and a rate-card planning estimate.
-- Those claims are not billing evidence and must not satisfy E1's hard
-- monetary eligibility gate. Preserve the estimate in runtime.model_runs for
-- planning/audit, but leave evolution.e1_executor_measurements.cost_units
-- NULL unless the run is deterministic compute with no provider/model/cost.
CREATE OR REPLACE FUNCTION evolution.exclude_unverified_e1_cost()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE unverified boolean;
BEGIN
 SELECT COALESCE(
     mr.telemetry->>'worker_usage_unverified'='true'
     OR mr.telemetry->>'cost_estimate_status'='UNVERIFIED_WORKER_USAGE',
     false)
   INTO unverified
   FROM runtime.model_runs mr
  WHERE mr.model_run_id=NEW.model_run_id;

 IF unverified THEN
   NEW.cost_units:=NULL;
 END IF;
 RETURN NEW;
END; $$;

-- PostgreSQL orders same-event triggers by name. Run after 0008's deterministic
-- zero-cost normalization so only deterministic, non-provider work may keep
-- that known-zero value; provider estimates are cleared.
CREATE TRIGGER e1_measurement_runtime_zzcost
BEFORE INSERT ON evolution.e1_executor_measurements
FOR EACH ROW EXECUTE FUNCTION evolution.exclude_unverified_e1_cost();
