-- Promote stable scheduling and coordination identity out of extensible JSON.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM runtime.tasks
    WHERE budget ? 'max_attempts' AND jsonb_typeof(budget->'max_attempts') <> 'null'
      AND CASE WHEN budget->>'max_attempts' ~ '^[0-9]+$'
               THEN (budget->>'max_attempts')::numeric > 2147483647
               ELSE true END
  ) THEN
    RAISE EXCEPTION 'cannot promote malformed task budget max_attempts values';
  END IF;
  IF EXISTS (
    SELECT 1 FROM runtime.tasks t
    WHERE metadata ? 'plan_version_id'
      AND NOT EXISTS (SELECT 1 FROM runtime.coordination_plan_versions p
                      WHERE p.plan_version_id=t.metadata->>'plan_version_id')
  ) THEN
    RAISE EXCEPTION 'cannot promote task plan_version_id without a matching coordination plan';
  END IF;
END $$;

ALTER TABLE runtime.tasks
  ADD COLUMN plan_version_id text REFERENCES runtime.coordination_plan_versions(plan_version_id),
  ADD COLUMN max_attempts integer CHECK (max_attempts IS NULL OR max_attempts >= 0);

UPDATE runtime.tasks
SET plan_version_id = metadata->>'plan_version_id',
    max_attempts = CASE WHEN NOT budget ? 'max_attempts'
                              OR jsonb_typeof(budget->'max_attempts')='null' THEN NULL
                        ELSE (budget->>'max_attempts')::integer END;

UPDATE runtime.tasks SET metadata=metadata-'plan_version_id' WHERE metadata ? 'plan_version_id';
UPDATE runtime.tasks SET budget=budget-'max_attempts' WHERE budget ? 'max_attempts';

-- Flush deferred task guard events before creating an index in this transaction.
SET CONSTRAINTS runtime.task_state_has_event IMMEDIATE;

CREATE INDEX tasks_plan_version_status_idx
  ON runtime.tasks(plan_version_id,status) WHERE plan_version_id IS NOT NULL;
