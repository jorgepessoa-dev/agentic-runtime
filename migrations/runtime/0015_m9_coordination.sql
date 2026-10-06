-- M9: immutable plan versions, durable graph contracts, delegation and verification.
CREATE TABLE runtime.coordination_plan_versions (
    plan_version_id text PRIMARY KEY,
    plan_id text NOT NULL,
    goal_id text NOT NULL REFERENCES runtime.goals(goal_id),
    version integer NOT NULL CHECK (version > 0),
    parent_plan_version_id text REFERENCES runtime.coordination_plan_versions(plan_version_id),
    status text NOT NULL CHECK (status IN ('PROPOSED','ACCEPTED','REJECTED','SUPERSEDED')),
    proposer text NOT NULL,
    canonical_sha256 char(64) NOT NULL,
    proposal jsonb NOT NULL,
    limits jsonb NOT NULL,
    accepted_by text,
    accepted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(plan_id,version),
    CHECK ((status='ACCEPTED') = (accepted_by IS NOT NULL AND accepted_at IS NOT NULL))
);

CREATE TABLE runtime.coordination_plan_nodes (
    plan_version_id text NOT NULL REFERENCES runtime.coordination_plan_versions(plan_version_id),
    node_key text NOT NULL,
    parent_node_key text,
    task_id text REFERENCES runtime.tasks(task_id),
    task_type text NOT NULL,
    objective text NOT NULL,
    required_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    input_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    output_contract jsonb NOT NULL DEFAULT '{}'::jsonb,
    acceptance_criteria jsonb NOT NULL DEFAULT '{}'::jsonb,
    verifier_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    budget jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(plan_version_id,node_key),
    FOREIGN KEY(plan_version_id,parent_node_key)
      REFERENCES runtime.coordination_plan_nodes(plan_version_id,node_key)
      DEFERRABLE INITIALLY DEFERRED,
    UNIQUE(task_id)
);

CREATE TABLE runtime.coordination_plan_edges (
    plan_version_id text NOT NULL,
    predecessor_key text NOT NULL,
    successor_key text NOT NULL,
    requirement text NOT NULL CHECK (requirement IN ('ACCEPTED','ARTIFACT','ORDER_ONLY')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(plan_version_id,predecessor_key,successor_key),
    CHECK (predecessor_key<>successor_key),
    FOREIGN KEY(plan_version_id,predecessor_key)
      REFERENCES runtime.coordination_plan_nodes(plan_version_id,node_key),
    FOREIGN KEY(plan_version_id,successor_key)
      REFERENCES runtime.coordination_plan_nodes(plan_version_id,node_key)
);

CREATE TABLE runtime.coordination_delegations (
    delegation_id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_sha256 char(64) NOT NULL,
    plan_version_id text NOT NULL REFERENCES runtime.coordination_plan_versions(plan_version_id),
    parent_task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    parent_attempt_id text REFERENCES runtime.attempts(attempt_id),
    child_task_id text NOT NULL UNIQUE REFERENCES runtime.tasks(task_id),
    delegator text NOT NULL,
    depth integer NOT NULL CHECK(depth >= 1),
    capability_contract jsonb NOT NULL,
    input_refs jsonb NOT NULL,
    output_contract jsonb NOT NULL,
    acceptance_criteria jsonb NOT NULL,
    budget jsonb NOT NULL,
    deadline timestamptz,
    status text NOT NULL CHECK(status IN ('PLAN_ACCEPTED','AUTHORIZED','DELIVERED','VERIFIED','ACCEPTED','REJECTED','CANCELLED')),
    verifier_task_id text REFERENCES runtime.tasks(task_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX coordination_delegations_parent_idx ON runtime.coordination_delegations(parent_task_id,status);

CREATE TABLE runtime.task_verifications (
    verification_id text PRIMARY KEY,
    task_id text NOT NULL REFERENCES runtime.tasks(task_id),
    attempt_id text NOT NULL REFERENCES runtime.attempts(attempt_id),
    verifier_ref text NOT NULL,
    verifier_task_id text REFERENCES runtime.tasks(task_id),
    result_hash char(64),
    status text NOT NULL CHECK(status IN ('PENDING','ACCEPTED','REJECTED','REPAIR_REQUIRED','ESCALATED')),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CHECK (verifier_task_id IS NULL OR verifier_task_id<>task_id)
);
CREATE UNIQUE INDEX task_verifications_once_idx
  ON runtime.task_verifications(task_id,attempt_id,verifier_task_id,result_hash)
  WHERE verifier_task_id IS NOT NULL;
CREATE INDEX task_verifications_task_idx ON runtime.task_verifications(task_id,created_at);

CREATE OR REPLACE FUNCTION runtime.prevent_task_verification_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'task verification evidence is append-only';
END $$;
CREATE TRIGGER task_verification_append_only
  BEFORE UPDATE OR DELETE ON runtime.task_verifications
  FOR EACH ROW EXECUTE FUNCTION runtime.prevent_task_verification_mutation();

CREATE OR REPLACE FUNCTION runtime.guard_coordination_plan_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'coordination plan versions are immutable'; END IF;
  IF ROW(NEW.plan_id,NEW.goal_id,NEW.version,NEW.parent_plan_version_id,NEW.proposer,
         NEW.canonical_sha256,NEW.proposal,NEW.limits,NEW.created_at)
     IS DISTINCT FROM
     ROW(OLD.plan_id,OLD.goal_id,OLD.version,OLD.parent_plan_version_id,OLD.proposer,
         OLD.canonical_sha256,OLD.proposal,OLD.limits,OLD.created_at) THEN
    RAISE EXCEPTION 'coordination plan version content is immutable';
  END IF;
  IF OLD.status<>'PROPOSED' OR NEW.status NOT IN ('ACCEPTED','REJECTED') THEN
    RAISE EXCEPTION 'invalid coordination plan version transition';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER coordination_plan_version_immutable
  BEFORE UPDATE OR DELETE ON runtime.coordination_plan_versions
  FOR EACH ROW EXECUTE FUNCTION runtime.guard_coordination_plan_version();

CREATE OR REPLACE FUNCTION runtime.guard_coordination_graph_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v_status text;
BEGIN
  SELECT status INTO v_status FROM runtime.coordination_plan_versions
   WHERE plan_version_id=COALESCE(NEW.plan_version_id,OLD.plan_version_id);
  IF v_status<>'PROPOSED' THEN RAISE EXCEPTION 'accepted plan graph is immutable'; END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER coordination_plan_nodes_immutable
  BEFORE INSERT OR UPDATE OR DELETE ON runtime.coordination_plan_nodes
  FOR EACH ROW EXECUTE FUNCTION runtime.guard_coordination_graph_immutable();
CREATE TRIGGER coordination_plan_edges_immutable
  BEFORE INSERT OR UPDATE OR DELETE ON runtime.coordination_plan_edges
  FOR EACH ROW EXECUTE FUNCTION runtime.guard_coordination_graph_immutable();

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
    GRANT SELECT,INSERT,UPDATE ON runtime.coordination_plan_versions,
      runtime.coordination_plan_nodes,runtime.coordination_delegations
      TO agentic_runtime_runtime;
    GRANT SELECT,INSERT ON runtime.coordination_plan_edges,runtime.task_verifications
      TO agentic_runtime_runtime;
  END IF;
END $$;
