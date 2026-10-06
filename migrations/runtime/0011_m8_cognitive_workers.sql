-- M8: provider-neutral route capability inventory and immutable invocation
-- provenance. No provider credentials or prompt/output bodies live in SQL.
CREATE TABLE runtime.cognitive_routes (
    route_id text PRIMARY KEY,
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    provider_name text NOT NULL,
    model_family text NOT NULL,
    model_label text,
    adapter_version text NOT NULL,
    supported_roles jsonb NOT NULL CHECK (jsonb_typeof(supported_roles)='array'),
    declared_capabilities jsonb NOT NULL CHECK (jsonb_typeof(declared_capabilities)='array'),
    observed_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(observed_capabilities)='array'),
    structured_output boolean NOT NULL,
    tool_use boolean NOT NULL,
    max_input_tokens integer CHECK (max_input_tokens IS NULL OR max_input_tokens>0),
    concurrency_limit integer NOT NULL CHECK (concurrency_limit>0),
    supports_cancel boolean NOT NULL,
    cost_state text NOT NULL CHECK (cost_state IN
      ('MEASURED','PROVIDER_REPORTED','SUBSCRIPTION_UNPRICED','UNKNOWN','NOT_APPLICABLE')),
    health text NOT NULL CHECK (health IN ('UNKNOWN','HEALTHY','DEGRADED','UNAVAILABLE')),
    last_probe timestamptz,
    resource_class text NOT NULL,
    config_ref text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX cognitive_routes_health_idx ON runtime.cognitive_routes(health,provider_name,model_family);

CREATE TABLE runtime.cognitive_invocations (
    invocation_id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    campaign_id text REFERENCES runtime.campaigns(campaign_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    lease_epoch bigint NOT NULL CHECK (lease_epoch>0),
    route_id text NOT NULL REFERENCES runtime.cognitive_routes(route_id),
    logical_role text NOT NULL,
    response_schema jsonb NOT NULL CHECK (jsonb_typeof(response_schema)='object'),
    response_schema_hash text NOT NULL CHECK (response_schema_hash ~ '^[0-9a-f]{64}$'),
    provider_name text NOT NULL,
    requested_route text NOT NULL,
    resolved_route text,
    resolution_state text NOT NULL CHECK (resolution_state IN ('VERIFIED','UNVERIFIED','UNKNOWN')),
    model_version text,
    adapter_version text NOT NULL,
    status text NOT NULL CHECK (status IN
      ('PENDING','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED','UNAVAILABLE','MALFORMED','STALE','QUARANTINED')),
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    latency_ms bigint CHECK (latency_ms IS NULL OR latency_ms>=0),
    raw_artifact_id text,
    raw_hash text CHECK (raw_hash IS NULL OR raw_hash ~ '^[0-9a-f]{64}$'),
    normalized_artifact_id text,
    normalized_hash text CHECK (normalized_hash IS NULL OR normalized_hash ~ '^[0-9a-f]{64}$'),
    input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens>=0),
    output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens>=0),
    cached_tokens bigint CHECK (cached_tokens IS NULL OR cached_tokens>=0),
    reasoning_tokens bigint CHECK (reasoning_tokens IS NULL OR reasoning_tokens>=0),
    provider_cost numeric CHECK (provider_cost IS NULL OR provider_cost>=0),
    usage_state text NOT NULL CHECK (usage_state IN
      ('MEASURED','PROVIDER_REPORTED','SUBSCRIPTION_UNPRICED','UNKNOWN','NOT_APPLICABLE')),
    cost_state text NOT NULL CHECK (cost_state IN
      ('MEASURED','PROVIDER_REPORTED','SUBSCRIPTION_UNPRICED','UNKNOWN','NOT_APPLICABLE')),
    finish_reason text,
    error_class text,
    stderr_class text,
    tool_summary jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tool_summary)='array'),
    telemetry jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK ((status='PENDING' AND completed_at IS NULL) OR (status<>'PENDING' AND completed_at IS NOT NULL)),
    CHECK (status<>'SUCCEEDED' OR (completed_at IS NOT NULL AND raw_artifact_id IS NOT NULL
      AND normalized_artifact_id IS NOT NULL AND raw_hash IS NOT NULL AND normalized_hash IS NOT NULL)),
    CHECK ((provider_cost IS NULL) OR cost_state IN ('MEASURED','PROVIDER_REPORTED'))
);
CREATE INDEX cognitive_invocation_campaign_idx ON runtime.cognitive_invocations(campaign_id,started_at);
CREATE INDEX cognitive_invocation_route_idx ON runtime.cognitive_invocations(route_id,status,started_at DESC);

-- One SHA-256 blob may be emitted by many invocations. The generic runtime
-- artifact manifest intentionally has one producer; this relation preserves
-- the per-invocation producer lineage while reusing immutable content bytes.
CREATE TABLE runtime.cognitive_artifacts (
    cognitive_artifact_id text PRIMARY KEY,
    invocation_id text NOT NULL REFERENCES runtime.cognitive_invocations(invocation_id) ON DELETE RESTRICT,
    kind text NOT NULL CHECK (kind IN ('RAW_OUTPUT','NORMALIZED_OUTPUT')),
    blob_artifact_id text NOT NULL CHECK (blob_artifact_id ~ '^sha256:[0-9a-f]{64}$'),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    location text NOT NULL,
    verification_status text NOT NULL CHECK (verification_status='VERIFIED'),
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(invocation_id,kind),
    CHECK (blob_artifact_id='sha256:'||content_hash)
);
CREATE INDEX cognitive_artifacts_blob_idx ON runtime.cognitive_artifacts(blob_artifact_id);

CREATE OR REPLACE FUNCTION runtime.freeze_cognitive_artifact()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'cognitive artifact lineage is immutable';
END; $$;
CREATE TRIGGER cognitive_artifact_immutable
BEFORE UPDATE OR DELETE ON runtime.cognitive_artifacts
FOR EACH ROW EXECUTE FUNCTION runtime.freeze_cognitive_artifact();
CREATE TRIGGER cognitive_artifact_reference_lock
BEFORE INSERT ON runtime.cognitive_artifacts
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();

CREATE OR REPLACE FUNCTION runtime.validate_cognitive_invocation_artifacts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE raw_row runtime.cognitive_artifacts%ROWTYPE;
DECLARE normalized_row runtime.cognitive_artifacts%ROWTYPE;
DECLARE active_lease boolean;
BEGIN
  IF NEW.status='SUCCEEDED' THEN
    SELECT EXISTS (
      SELECT 1 FROM runtime.leases l JOIN runtime.attempts a USING(attempt_id)
      WHERE l.task_id=NEW.task_id AND l.attempt_id=NEW.attempt_id
        AND l.lease_epoch=NEW.lease_epoch AND l.worker_id=NEW.worker_id
        AND l.status='ACTIVE' AND l.lease_until>now()
        AND a.worker_instance_id=NEW.worker_instance_id
    ) INTO active_lease;
    IF NOT active_lease THEN
      RAISE EXCEPTION 'stale cognitive invocation cannot commit success';
    END IF;
    SELECT * INTO raw_row FROM runtime.cognitive_artifacts
      WHERE invocation_id=NEW.invocation_id AND kind='RAW_OUTPUT'
        AND blob_artifact_id=NEW.raw_artifact_id;
    SELECT * INTO normalized_row FROM runtime.cognitive_artifacts
      WHERE invocation_id=NEW.invocation_id AND kind='NORMALIZED_OUTPUT'
        AND blob_artifact_id=NEW.normalized_artifact_id;
    IF raw_row.cognitive_artifact_id IS NULL OR raw_row.verification_status<>'VERIFIED'
       OR raw_row.content_hash<>NEW.raw_hash OR raw_row.task_id<>NEW.task_id
       OR raw_row.attempt_id<>NEW.attempt_id THEN
      RAISE EXCEPTION 'cognitive raw output artifact is not verified or lineage-matched';
    END IF;
    IF normalized_row.cognitive_artifact_id IS NULL OR normalized_row.verification_status<>'VERIFIED'
       OR normalized_row.content_hash<>NEW.normalized_hash
       OR normalized_row.task_id<>NEW.task_id
       OR normalized_row.attempt_id<>NEW.attempt_id THEN
      RAISE EXCEPTION 'cognitive normalized output artifact is not verified or lineage-matched';
    END IF;
  END IF;
  RETURN NEW;
END; $$;
CREATE TRIGGER cognitive_invocation_artifact_guard
BEFORE INSERT OR UPDATE ON runtime.cognitive_invocations
FOR EACH ROW EXECUTE FUNCTION runtime.validate_cognitive_invocation_artifacts();

CREATE OR REPLACE FUNCTION runtime.freeze_terminal_cognitive_invocation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'cognitive invocation history is immutable';
  END IF;
  IF OLD.status<>'PENDING' THEN
    RAISE EXCEPTION 'terminal cognitive invocation is immutable';
  END IF;
  IF OLD.invocation_id<>NEW.invocation_id OR OLD.request_hash<>NEW.request_hash
     OR OLD.idempotency_key<>NEW.idempotency_key OR OLD.task_id<>NEW.task_id
     OR OLD.attempt_id<>NEW.attempt_id OR OLD.route_id<>NEW.route_id
     OR OLD.worker_id<>NEW.worker_id OR OLD.worker_instance_id<>NEW.worker_instance_id THEN
    RAISE EXCEPTION 'cognitive invocation identity and provenance are immutable';
  END IF;
  RETURN NEW;
END; $$;
CREATE TRIGGER cognitive_invocation_immutable
BEFORE UPDATE OR DELETE ON runtime.cognitive_invocations
FOR EACH ROW EXECUTE FUNCTION runtime.freeze_terminal_cognitive_invocation();

-- All raw/normalized artifact-reference writers join M4C's artifact-key lock
-- protocol; GC also checks this relation before sweeping a blob.
CREATE TRIGGER artifact_reference_lock
BEFORE INSERT OR UPDATE ON runtime.cognitive_invocations
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
    GRANT SELECT,INSERT,UPDATE ON runtime.cognitive_routes TO agentic_runtime_runtime;
    GRANT SELECT,INSERT,UPDATE ON runtime.cognitive_invocations TO agentic_runtime_runtime;
    GRANT SELECT,INSERT ON runtime.cognitive_artifacts TO agentic_runtime_runtime;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_evaluator') THEN
    GRANT SELECT ON runtime.cognitive_routes,runtime.cognitive_invocations TO agentic_runtime_evaluator;
    GRANT SELECT ON runtime.cognitive_artifacts TO agentic_runtime_evaluator;
  END IF;
END $$;
