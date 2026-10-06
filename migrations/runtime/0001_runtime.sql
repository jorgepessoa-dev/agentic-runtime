CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE runtime.campaigns (
    campaign_id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash text NOT NULL,
    description text NOT NULL,
    status text NOT NULL CHECK (status IN ('ACTIVE','PAUSED','COMPLETED','CANCELLED')),
    budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    reserved_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    spent_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    max_children integer NOT NULL DEFAULT 6 CHECK (max_children >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE runtime.goals (
    goal_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES runtime.campaigns(campaign_id),
    parent_goal_id text REFERENCES runtime.goals(goal_id),
    status text NOT NULL CHECK (status IN ('DRAFT','ACTIVE','BLOCKED','ACHIEVED','ABANDONED')),
    description text NOT NULL,
    artifact_ref text,
    priority integer NOT NULL DEFAULT 0,
    mission_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE runtime.opportunities (
    opportunity_id text PRIMARY KEY,
    goal_id text NOT NULL REFERENCES runtime.goals(goal_id),
    kind text NOT NULL CHECK (kind IN ('OPPORTUNITY','WEAKNESS','UNCERTAINTY','STAGNATION')),
    observation_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    description text NOT NULL,
    artifact_ref text,
    status text NOT NULL CHECK (status IN ('OPEN','INVESTIGATING','ACTIONABLE','CLOSED','REJECTED')),
    estimated_value jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.hypotheses (
    hypothesis_id text PRIMARY KEY,
    opportunity_id text NOT NULL REFERENCES runtime.opportunities(opportunity_id),
    statement text NOT NULL,
    artifact_ref text,
    falsification_ref text,
    status text NOT NULL CHECK (status IN ('PROPOSED','TESTING','SUPPORTED','REFUTED','INCONCLUSIVE')),
    created_by text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.plan_proposals (
    plan_id text PRIMARY KEY,
    goal_id text NOT NULL REFERENCES runtime.goals(goal_id),
    proposed_tasks jsonb NOT NULL,
    dependencies jsonb NOT NULL DEFAULT '[]'::jsonb,
    rationale_ref text,
    estimated_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_by text NOT NULL,
    status text NOT NULL CHECK (status IN ('PROPOSED','VALIDATED','REJECTED','MATERIALIZED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE runtime.tasks (
    task_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES runtime.campaigns(campaign_id),
    goal_id text REFERENCES runtime.goals(goal_id),
    plan_id text REFERENCES runtime.plan_proposals(plan_id),
    parent_task_id text REFERENCES runtime.tasks(task_id),
    task_type text NOT NULL,
    status text NOT NULL CHECK (status IN (
        'DRAFT','QUEUED','LEASED','CONTEXT_VALIDATED','RUNNING',
        'WAITING_CHILDREN','WAITING_TOOL','WAITING_IO','BLOCKED','CHECKPOINTED',
        'RESULT_COMMITTED','VERIFIED','ACCEPTED','REJECTED','NEEDS_REVIEW',
        'FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','QUARANTINED',
        'BUDGET_EXCEEDED','RETRY_PENDING')),
    priority integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    run_after timestamptz NOT NULL DEFAULT now(),
    required_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    input_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    output_contract jsonb NOT NULL DEFAULT '{}'::jsonb,
    budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    reserved_budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    max_children integer NOT NULL DEFAULT 0 CHECK (max_children >= 0),
    depth_remaining integer NOT NULL DEFAULT 0 CHECK (depth_remaining >= 0),
    idempotency_key text NOT NULL UNIQUE,
    request_hash text NOT NULL,
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    result_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    result_hash text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX tasks_runnable_idx ON runtime.tasks(priority DESC, run_after, created_at)
    WHERE status IN ('QUEUED','RETRY_PENDING');
CREATE INDEX tasks_campaign_idx ON runtime.tasks(campaign_id, status);

CREATE OR REPLACE FUNCTION runtime.validate_task_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ok boolean := false;
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.status NOT IN ('DRAFT','QUEUED') THEN
            RAISE EXCEPTION 'tasks must be created in DRAFT or QUEUED';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status=NEW.status THEN RETURN NEW; END IF;
    ok := CASE OLD.status
        WHEN 'DRAFT' THEN NEW.status IN ('QUEUED','CANCELLED')
        WHEN 'QUEUED' THEN NEW.status IN ('LEASED','CANCELLED','BUDGET_EXCEEDED')
        WHEN 'RETRY_PENDING' THEN NEW.status IN ('LEASED','CANCELLED','BUDGET_EXCEEDED')
        WHEN 'LEASED' THEN NEW.status IN ('CONTEXT_VALIDATED','RUNNING','CANCELLED','FAILED_TRANSIENT','FAILED_PERMANENT','RETRY_PENDING')
        WHEN 'CONTEXT_VALIDATED' THEN NEW.status IN ('RUNNING','CANCELLED','FAILED_TRANSIENT','FAILED_PERMANENT')
        WHEN 'RUNNING' THEN NEW.status IN ('WAITING_CHILDREN','WAITING_TOOL','WAITING_IO','BLOCKED','CHECKPOINTED','RESULT_COMMITTED','NEEDS_REVIEW','FAILED_TRANSIENT','FAILED_PERMANENT','CANCELLED','BUDGET_EXCEEDED','QUARANTINED')
        WHEN 'WAITING_CHILDREN' THEN NEW.status IN ('QUEUED','RUNNING','BLOCKED','CANCELLED')
        WHEN 'WAITING_TOOL' THEN NEW.status IN ('RUNNING','BLOCKED','CANCELLED')
        WHEN 'WAITING_IO' THEN NEW.status IN ('RUNNING','BLOCKED','CANCELLED')
        WHEN 'BLOCKED' THEN NEW.status IN ('QUEUED','CANCELLED')
        WHEN 'CHECKPOINTED' THEN NEW.status IN ('QUEUED','RUNNING','CANCELLED')
        WHEN 'RESULT_COMMITTED' THEN NEW.status IN ('VERIFIED','QUARANTINED','NEEDS_REVIEW')
        WHEN 'VERIFIED' THEN NEW.status IN ('ACCEPTED','REJECTED','NEEDS_REVIEW')
        WHEN 'NEEDS_REVIEW' THEN NEW.status IN ('ACCEPTED','REJECTED','QUARANTINED')
        ELSE false END;
    IF NOT ok THEN RAISE EXCEPTION 'invalid task transition: % -> %', OLD.status, NEW.status; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER task_transition_guard BEFORE INSERT OR UPDATE OF status ON runtime.tasks
    FOR EACH ROW EXECUTE FUNCTION runtime.validate_task_transition();

CREATE OR REPLACE FUNCTION runtime.require_task_transition_event() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE task_key text; target_state text; task_status text;
BEGIN
    task_key := NEW.task_id;
    target_state := NEW.status;
    SELECT status INTO task_status FROM runtime.tasks WHERE task_id=task_key;
    IF task_status=target_state AND NOT EXISTS (
        SELECT 1 FROM runtime.events e WHERE e.task_id=task_key AND e.event_type='TASK_'||target_state
    ) THEN
        RAISE EXCEPTION 'task state % lacks matching event', target_state;
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER task_state_has_event AFTER INSERT OR UPDATE ON runtime.tasks
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION runtime.require_task_transition_event();

CREATE TABLE runtime.task_dependencies (
    task_id text NOT NULL REFERENCES runtime.tasks(task_id) ON DELETE CASCADE,
    depends_on_task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    dependency_type text NOT NULL CHECK (dependency_type IN ('REQUIRES_ACCEPTED','REQUIRES_ARTIFACT','ORDER_ONLY')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, depends_on_task_id),
    CHECK (task_id <> depends_on_task_id)
);
CREATE INDEX task_dependencies_parent_idx ON runtime.task_dependencies(depends_on_task_id);

CREATE TABLE runtime.workers (
    worker_id text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('IDLE','BUSY','DRAINING','UNHEALTHY','STOPPED')),
    capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    last_heartbeat timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.attempts (
    attempt_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    worker_id text NOT NULL REFERENCES runtime.workers(worker_id),
    lease_epoch bigint NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN (
        'LEASED','CONTEXT_VALIDATED','RUNNING','WAITING_CHILDREN','WAITING_TOOL','WAITING_IO',
        'BLOCKED','CHECKPOINTED','RESULT_COMMITTED','VERIFIED','ACCEPTED','REJECTED',
        'NEEDS_REVIEW','FAILED_TRANSIENT','FAILED_PERMANENT','FAILED','ABANDONED',
        'CANCELLED','QUARANTINED','BUDGET_EXCEEDED','RETRY_PENDING')),
    executor_ref text,
    model_run_id text,
    sandbox_id text,
    error_class text,
    telemetry jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (task_id, lease_epoch)
);
CREATE INDEX attempts_task_idx ON runtime.attempts(task_id, started_at DESC);

CREATE TABLE runtime.leases (
    task_id text PRIMARY KEY REFERENCES runtime.tasks(task_id),
    worker_id text NOT NULL REFERENCES runtime.workers(worker_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    lease_epoch bigint NOT NULL,
    lease_until timestamptz NOT NULL,
    last_heartbeat timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('ACTIVE','EXPIRED','RELEASED')),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (task_id, lease_epoch)
);
CREATE INDEX leases_expiry_idx ON runtime.leases(lease_until) WHERE status='ACTIVE';

CREATE TABLE runtime.sandboxes (
    sandbox_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL UNIQUE REFERENCES runtime.attempts(attempt_id),
    implementation text NOT NULL,
    base_revision text,
    workspace_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    destroyed_at timestamptz,
    status text NOT NULL CHECK (status IN ('CREATED','READY','ACTIVE','CLEANUP_PENDING','DESTROYED','CLEANUP_FAILED')),
    cleanup_status text NOT NULL,
    policy_ref text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE runtime.executors (
    executor_id text PRIMARY KEY,
    adapter_type text NOT NULL,
    backend text NOT NULL,
    version text NOT NULL,
    configuration_ref text NOT NULL,
    capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    location text NOT NULL CHECK (location IN ('LOCAL','REMOTE','API','EPHEMERAL')),
    enabled boolean NOT NULL DEFAULT true,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.model_runs (
    model_run_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    executor_id text NOT NULL REFERENCES runtime.executors(executor_id),
    adapter_type text NOT NULL,
    provider text,
    requested_model text,
    resolved_model text,
    role text,
    input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0),
    estimated_cost numeric CHECK (estimated_cost IS NULL OR estimated_cost >= 0),
    latency_ms bigint CHECK (latency_ms IS NULL OR latency_ms >= 0),
    status text NOT NULL,
    schema_valid boolean,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    telemetry jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX model_runs_capability_history_idx ON runtime.model_runs(executor_id, status, completed_at);

CREATE TABLE runtime.artifacts (
    artifact_id text PRIMARY KEY,
    kind text NOT NULL,
    schema_version text NOT NULL,
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    producer_task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    producer_attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    input_manifest_hash text NOT NULL CHECK (input_manifest_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    location text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    verification_status text NOT NULL CHECK (verification_status IN ('UNVERIFIED','VERIFIED','QUARANTINED')),
    UNIQUE (content_hash, producer_attempt_id)
);

CREATE TABLE runtime.knowledge_objects (
    knowledge_id text PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('CLAIM','EVIDENCE','EXPERIMENT','DECISION','FACT','CONTRADICTION','QUESTION','OBSERVATION')),
    content_ref text,
    structured_payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    source_task_id text REFERENCES runtime.tasks(task_id),
    source_artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL CHECK (status IN ('PROPOSED','ACTIVE','DISPUTED','SUPERSEDED','REJECTED')),
    evidence_quality text CHECK (evidence_quality IS NULL OR evidence_quality IN ('LOW','MODERATE','HIGH')),
    reproducibility text CHECK (reproducibility IS NULL OR reproducibility IN ('UNKNOWN','REPRODUCIBLE','NOT_REPRODUCIBLE')),
    review_status text CHECK (review_status IS NULL OR review_status IN ('UNREVIEWED','REVIEWED','INDEPENDENTLY_REVIEWED')),
    independence text CHECK (independence IS NULL OR independence IN ('UNKNOWN','DEPENDENT','INDEPENDENT')),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (content_ref IS NOT NULL OR structured_payload IS NOT NULL)
);

CREATE TABLE runtime.knowledge_edges (
    from_knowledge_id text NOT NULL REFERENCES runtime.knowledge_objects(knowledge_id),
    to_knowledge_id text NOT NULL REFERENCES runtime.knowledge_objects(knowledge_id),
    edge_type text NOT NULL CHECK (edge_type IN ('SUPPORTS','CONTRADICTS','TESTS','DERIVED_FROM','AFFECTS','SUPERSEDES','DEPENDS_ON')),
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (from_knowledge_id, to_knowledge_id, edge_type),
    CHECK (from_knowledge_id <> to_knowledge_id)
);

CREATE TABLE runtime.events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    campaign_id text REFERENCES runtime.campaigns(campaign_id),
    goal_id text REFERENCES runtime.goals(goal_id),
    task_id text REFERENCES runtime.tasks(task_id),
    attempt_id text REFERENCES runtime.attempts(attempt_id),
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    causation_id text REFERENCES runtime.events(event_id),
    correlation_id text NOT NULL,
    schema_version text NOT NULL,
    payload jsonb NOT NULL,
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);
CREATE INDEX events_task_causality_idx ON runtime.events(task_id, occurred_at);
CREATE INDEX events_correlation_idx ON runtime.events(correlation_id, occurred_at);
CREATE INDEX events_causation_idx ON runtime.events(causation_id);

CREATE TABLE runtime.outbox (
    outbox_id text PRIMARY KEY,
    event_id text NOT NULL REFERENCES runtime.events(event_id),
    topic text NOT NULL,
    idempotency_key text NOT NULL UNIQUE,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_by text,
    locked_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    delivered_at timestamptz,
    last_error text
);
CREATE INDEX outbox_ready_idx ON runtime.outbox(available_at, created_at) WHERE delivered_at IS NULL;

CREATE TABLE runtime.dispatch_receipts (
    receipt_id text PRIMARY KEY,
    outbox_id text NOT NULL UNIQUE REFERENCES runtime.outbox(outbox_id),
    logical_effect_ref text NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.routing_decisions (
    decision_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    required_capabilities jsonb NOT NULL,
    eligible_executor_refs jsonb NOT NULL,
    excluded_executor_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    selected_executor_ref text NOT NULL REFERENCES runtime.executors(executor_id),
    policy_ref text NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION runtime.reject_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'runtime events are append-only';
END;
$$;
CREATE TRIGGER runtime_events_immutable BEFORE UPDATE OR DELETE ON runtime.events
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_event_mutation();
