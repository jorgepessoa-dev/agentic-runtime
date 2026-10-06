ALTER TABLE runtime.attempts
    ADD COLUMN worker_instance_id text;

ALTER TABLE runtime.artifacts
    ADD COLUMN worker_id text,
    ADD COLUMN worker_instance_id text,
    ADD COLUMN lease_epoch bigint;

ALTER TABLE runtime.sandboxes
    ADD COLUMN worker_id text,
    ADD COLUMN worker_instance_id text,
    ADD COLUMN lease_epoch bigint;

CREATE TABLE runtime.worker_identities (
    worker_id text PRIMARY KEY,
    token_sha256 text NOT NULL CHECK (token_sha256 ~ '^[0-9a-f]{64}$'),
    granted_capabilities jsonb NOT NULL,
    allowed_task_types jsonb NOT NULL,
    resource_classes jsonb NOT NULL,
    max_resource_policy jsonb NOT NULL DEFAULT '{}'::jsonb,
    allowed_tools jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL CHECK (status IN ('ACTIVE','REVOKED')),
    protocol_major integer NOT NULL,
    min_protocol_minor integer NOT NULL DEFAULT 0,
    token_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (protocol_major > 0 AND min_protocol_minor >= 0)
);

CREATE TABLE runtime.worker_instances (
    worker_instance_id text PRIMARY KEY,
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    software_version text NOT NULL,
    protocol_major integer NOT NULL,
    protocol_minor integer NOT NULL,
    capabilities jsonb NOT NULL,
    allowed_tools jsonb NOT NULL DEFAULT '[]'::jsonb,
    task_types jsonb NOT NULL,
    resource_classes jsonb NOT NULL,
    resource_policy jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('REGISTERING','READY','BUSY','SUSPECT','OFFLINE','DRAINING','REJECTED')),
    registered_at timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX worker_one_live_instance_idx ON runtime.worker_instances(worker_id)
    WHERE status IN ('REGISTERING','READY','BUSY','SUSPECT','DRAINING');
CREATE INDEX worker_instance_liveness_idx ON runtime.worker_instances(status,last_seen);

ALTER TABLE runtime.attempts ADD CONSTRAINT attempts_worker_instance_fk
    FOREIGN KEY (worker_instance_id) REFERENCES runtime.worker_instances(worker_instance_id);
ALTER TABLE runtime.artifacts ADD CONSTRAINT artifacts_worker_identity_fk
    FOREIGN KEY (worker_id) REFERENCES runtime.worker_identities(worker_id);
ALTER TABLE runtime.artifacts ADD CONSTRAINT artifacts_worker_instance_fk
    FOREIGN KEY (worker_instance_id) REFERENCES runtime.worker_instances(worker_instance_id);
ALTER TABLE runtime.sandboxes ADD CONSTRAINT sandboxes_worker_identity_fk
    FOREIGN KEY (worker_id) REFERENCES runtime.worker_identities(worker_id);
ALTER TABLE runtime.sandboxes ADD CONSTRAINT sandboxes_worker_instance_fk
    FOREIGN KEY (worker_instance_id) REFERENCES runtime.worker_instances(worker_instance_id);

CREATE TABLE runtime.worker_heartbeats (
    heartbeat_id text PRIMARY KEY,
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    observed_at timestamptz NOT NULL DEFAULT now(),
    active_attempts jsonb NOT NULL DEFAULT '[]'::jsonb,
    protocol_major integer NOT NULL,
    protocol_minor integer NOT NULL,
    request_id text NOT NULL UNIQUE,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX worker_heartbeats_instance_idx ON runtime.worker_heartbeats(worker_instance_id,observed_at DESC);

CREATE TABLE runtime.remote_claim_receipts (
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    idempotency_key text NOT NULL,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    task_id text REFERENCES runtime.tasks(task_id),
    attempt_id text REFERENCES runtime.attempts(attempt_id),
    lease_epoch bigint,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(worker_instance_id,idempotency_key)
);

CREATE TABLE runtime.remote_result_receipts (
    idempotency_key text PRIMARY KEY,
    request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    lease_epoch bigint NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.remote_transfer_rejections (
    rejection_id text PRIMARY KEY,
    worker_id text NOT NULL REFERENCES runtime.worker_identities(worker_id),
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    lease_epoch bigint NOT NULL,
    expected_hash text NOT NULL,
    received_size bigint NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime.worker_commands (
    command_id text PRIMARY KEY,
    worker_instance_id text NOT NULL REFERENCES runtime.worker_instances(worker_instance_id),
    task_id text REFERENCES runtime.tasks(task_id),
    attempt_id text REFERENCES runtime.attempts(attempt_id),
    lease_epoch bigint,
    command text NOT NULL CHECK (command IN ('CANCEL_ATTEMPT','DRAIN')),
    status text NOT NULL CHECK (status IN ('PENDING','DELIVERED','ACKNOWLEDGED','REJECTED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX worker_commands_pending_idx ON runtime.worker_commands(worker_instance_id,created_at)
    WHERE status='PENDING';

CREATE OR REPLACE FUNCTION runtime.reject_worker_heartbeat_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'worker heartbeats are append-only'; END; $$;
CREATE TRIGGER worker_heartbeats_immutable BEFORE UPDATE OR DELETE ON runtime.worker_heartbeats
    FOR EACH ROW EXECUTE FUNCTION runtime.reject_worker_heartbeat_mutation();

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON runtime.worker_identities,runtime.worker_instances,
   runtime.worker_heartbeats,runtime.remote_claim_receipts,runtime.remote_result_receipts,
   runtime.remote_transfer_rejections,runtime.worker_commands TO agentic_runtime_runtime;
  GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA runtime TO agentic_runtime_runtime;
 END IF;
END $$;
