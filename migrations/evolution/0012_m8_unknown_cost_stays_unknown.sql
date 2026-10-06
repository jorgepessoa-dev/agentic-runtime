-- UNKNOWN provider/worker usage is not known-zero merely because a worker row
-- lacks provider labels. Known zero is admitted only for deterministic compute.
CREATE OR REPLACE FUNCTION evolution.normalize_deterministic_e1_cost()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.cost_units IS NULL AND EXISTS (
   SELECT 1 FROM runtime.model_runs mr
   WHERE mr.model_run_id=NEW.model_run_id
     AND mr.adapter_type='DETERMINISTIC'
     AND mr.provider IS NULL
     AND mr.requested_model IS NULL
     AND mr.resolved_model IS NULL
     AND mr.estimated_cost=0
 ) THEN
   NEW.cost_units:=0;
 END IF;
 RETURN NEW;
END; $$;
