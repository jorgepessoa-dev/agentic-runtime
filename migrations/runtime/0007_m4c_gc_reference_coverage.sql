CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON runtime.sandboxes
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();
