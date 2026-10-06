CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON runtime.routing_decisions
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();

CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON runtime.dispatch_receipts
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();
