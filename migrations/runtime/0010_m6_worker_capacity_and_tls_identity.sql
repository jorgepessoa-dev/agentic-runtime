ALTER TABLE runtime.worker_identities
    ADD COLUMN allowed_worker_classes jsonb NOT NULL DEFAULT '["REMOTE_PERSISTENT"]'::jsonb,
    ADD COLUMN max_concurrency integer NOT NULL DEFAULT 1 CHECK (max_concurrency BETWEEN 1 AND 64),
    ADD COLUMN client_cert_sha256 text CHECK (
        client_cert_sha256 IS NULL OR client_cert_sha256 ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE runtime.worker_instances
    ADD COLUMN worker_class text NOT NULL DEFAULT 'REMOTE_PERSISTENT'
        CHECK (worker_class IN ('LOCAL','REMOTE_PERSISTENT','REMOTE_EPHEMERAL')),
    ADD COLUMN max_concurrency integer NOT NULL DEFAULT 1 CHECK (max_concurrency BETWEEN 1 AND 64);

CREATE INDEX worker_instance_class_state_idx
    ON runtime.worker_instances(worker_class,status,last_seen);
