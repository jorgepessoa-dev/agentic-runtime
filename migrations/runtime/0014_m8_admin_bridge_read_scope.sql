-- The M8 governance identity needs to validate a fixed route/campaign before
-- inserting its immutable qualification acceptance. It receives read-only
-- access to these two runtime tables; route writes and task writes remain in
-- agentic_runtime_runtime, while acceptance INSERT remains governance-only.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_governance') THEN
  GRANT SELECT ON runtime.cognitive_routes,runtime.campaigns TO agentic_runtime_governance;
 END IF;
END $$;
