-- E1 campaign/evaluation events can carry frozen artifact references. Make
-- those writes participate in the same per-artifact GC advisory-lock protocol.
CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON evolution.e1_events
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();
