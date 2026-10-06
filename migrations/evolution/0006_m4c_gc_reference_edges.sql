CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON evolution.events
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();

CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON evolution.eval_artifact_quarantine
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();
