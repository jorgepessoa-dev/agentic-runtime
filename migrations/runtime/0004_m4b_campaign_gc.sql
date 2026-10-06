ALTER TABLE runtime.improvement_campaigns
 ADD COLUMN reserved_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
 ADD COLUMN consumed_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
 ADD COLUMN stopped_at timestamptz,
 ADD COLUMN stop_evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE runtime.improvement_campaigns ADD CONSTRAINT campaign_terminal_state_fields CHECK (
 (status NOT IN ('STOPPED','COMPLETED') OR (ended_at IS NOT NULL))
);
CREATE TABLE runtime.improvement_budget_ledger (
 entry_id text PRIMARY KEY,
 campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
 idempotency_key text NOT NULL,
 budget_class text NOT NULL CHECK(budget_class IN ('EXPLOIT','EXPLORE')),
 action text NOT NULL CHECK(action IN ('RESERVE','CONSUME','RELEASE')),
 dimensions jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(campaign_id,idempotency_key)
);
ALTER TABLE runtime.opportunity_decisions ADD COLUMN budget_class text NOT NULL DEFAULT 'EXPLOIT'
 CHECK(budget_class IN ('EXPLOIT','EXPLORE'));
CREATE TABLE runtime.artifact_gc_candidates (
 artifact_id text PRIMARY KEY,
 retention_class text NOT NULL CHECK(retention_class IN ('TEMPORARY','WORKING','EVIDENCE','ACCEPTED','GOVERNANCE','ROLLBACK_CRITICAL')),
 status text NOT NULL CHECK(status IN ('MARKED','RETAINED','SWEPT')),
 marked_at timestamptz NOT NULL DEFAULT now(),
 delete_after timestamptz NOT NULL,
 swept_at timestamptz,
 last_check jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE runtime.sandbox_cleanup_attempts (
 cleanup_id text PRIMARY KEY,
 sandbox_id text NOT NULL REFERENCES runtime.sandboxes(sandbox_id),
 attempt_number integer NOT NULL CHECK(attempt_number > 0),
 started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz,
 outcome text NOT NULL CHECK(outcome IN ('STARTED','DESTROYED','RETAINED_ACTIVE','FAILED','ORPHANED')),
 detail jsonb NOT NULL DEFAULT '{}'::jsonb,
 UNIQUE(sandbox_id,attempt_number)
);
ALTER TABLE runtime.sandboxes ADD COLUMN cleanup_attempts integer NOT NULL DEFAULT 0 CHECK(cleanup_attempts >= 0);
ALTER TABLE runtime.sandboxes ADD COLUMN orphaned_at timestamptz;
