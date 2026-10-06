-- M8: bind validated model proposals to their durable challenger and preserve
-- the raw, normalized, and proposal artifact lineage independently of workers.
CREATE TABLE evolution.e1_cognitive_proposal_lineage (
    lineage_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES runtime.improvement_campaigns(campaign_id),
    scope_id text NOT NULL REFERENCES evolution.scopes(scope_id),
    mutation_id text NOT NULL UNIQUE REFERENCES evolution.mutation_proposals(mutation_id),
    genome_id text NOT NULL UNIQUE REFERENCES evolution.system_genomes(genome_id),
    invocation_id text NOT NULL REFERENCES runtime.cognitive_invocations(invocation_id),
    raw_cognitive_artifact_id text NOT NULL REFERENCES runtime.cognitive_artifacts(cognitive_artifact_id),
    normalized_cognitive_artifact_id text NOT NULL REFERENCES runtime.cognitive_artifacts(cognitive_artifact_id),
    raw_artifact_ref text NOT NULL CHECK (raw_artifact_ref ~ '^sha256:[0-9a-f]{64}$'),
    normalized_artifact_ref text NOT NULL CHECK (normalized_artifact_ref ~ '^sha256:[0-9a-f]{64}$'),
    proposal_artifact_ref text NOT NULL CHECK (proposal_artifact_ref ~ '^sha256:[0-9a-f]{64}$'),
    raw_hash text NOT NULL CHECK (raw_hash ~ '^[0-9a-f]{64}$'),
    normalized_hash text NOT NULL CHECK (normalized_hash ~ '^[0-9a-f]{64}$'),
    proposal_hash text NOT NULL CHECK (proposal_hash ~ '^[0-9a-f]{64}$'),
    proposal jsonb NOT NULL CHECK (jsonb_typeof(proposal)='object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(invocation_id,proposal_hash),
    CHECK (raw_artifact_ref='sha256:'||raw_hash),
    CHECK (normalized_artifact_ref='sha256:'||normalized_hash),
    CHECK (proposal_artifact_ref='sha256:'||proposal_hash)
);

CREATE INDEX e1_cognitive_proposal_campaign_idx
    ON evolution.e1_cognitive_proposal_lineage(campaign_id,created_at);

CREATE OR REPLACE FUNCTION evolution.validate_e1_cognitive_proposal_lineage()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE inv runtime.cognitive_invocations%ROWTYPE;
DECLARE raw_row runtime.cognitive_artifacts%ROWTYPE;
DECLARE normalized_row runtime.cognitive_artifacts%ROWTYPE;
DECLARE mut evolution.mutation_proposals%ROWTYPE;
DECLARE candidate evolution.system_genomes%ROWTYPE;
DECLARE campaign runtime.improvement_campaigns%ROWTYPE;
BEGIN
  SELECT * INTO inv FROM runtime.cognitive_invocations WHERE invocation_id=NEW.invocation_id;
  SELECT * INTO raw_row FROM runtime.cognitive_artifacts WHERE cognitive_artifact_id=NEW.raw_cognitive_artifact_id;
  SELECT * INTO normalized_row FROM runtime.cognitive_artifacts WHERE cognitive_artifact_id=NEW.normalized_cognitive_artifact_id;
  SELECT * INTO mut FROM evolution.mutation_proposals WHERE mutation_id=NEW.mutation_id;
  SELECT * INTO candidate FROM evolution.system_genomes WHERE genome_id=NEW.genome_id;
  SELECT * INTO campaign FROM runtime.improvement_campaigns WHERE campaign_id=NEW.campaign_id;

  IF inv.invocation_id IS NULL OR inv.status<>'SUCCEEDED' OR inv.campaign_id IS DISTINCT FROM NEW.campaign_id
     OR inv.raw_artifact_id IS DISTINCT FROM NEW.raw_artifact_ref
     OR inv.normalized_artifact_id IS DISTINCT FROM NEW.normalized_artifact_ref
     OR inv.raw_hash IS DISTINCT FROM NEW.raw_hash OR inv.normalized_hash IS DISTINCT FROM NEW.normalized_hash THEN
    RAISE EXCEPTION 'proposal lineage must bind a successful campaign invocation and its verified output hashes';
  END IF;
  IF raw_row.invocation_id<>NEW.invocation_id OR raw_row.kind<>'RAW_OUTPUT'
     OR raw_row.blob_artifact_id<>NEW.raw_artifact_ref OR raw_row.content_hash<>NEW.raw_hash
     OR raw_row.verification_status<>'VERIFIED' THEN
    RAISE EXCEPTION 'proposal lineage raw artifact does not match its invocation';
  END IF;
  IF normalized_row.invocation_id<>NEW.invocation_id OR normalized_row.kind<>'NORMALIZED_OUTPUT'
     OR normalized_row.blob_artifact_id<>NEW.normalized_artifact_ref OR normalized_row.content_hash<>NEW.normalized_hash
     OR normalized_row.verification_status<>'VERIFIED' THEN
    RAISE EXCEPTION 'proposal lineage normalized artifact does not match its invocation';
  END IF;
  IF mut.mutation_id IS NULL OR candidate.genome_id IS NULL OR mut.scope_id<>NEW.scope_id
     OR candidate.scope_id<>NEW.scope_id OR candidate.status NOT IN ('CHALLENGER','CHAMPION','RETAINED_FOR_DIVERSITY')
     OR candidate.metadata->>'mutation_id'<>NEW.mutation_id
     OR campaign.scope_id<>NEW.scope_id OR campaign.status NOT IN
       ('CHALLENGERS_CREATED','EVALUATING','COMPLETED') THEN
    RAISE EXCEPTION 'proposal lineage mutation, challenger, or campaign scope does not match';
  END IF;
  IF NEW.proposal->>'scope_id'<>NEW.scope_id OR NEW.proposal->>'tier'<>'E1'
     OR NEW.proposal->'mutation'->>'value' IS NULL
     OR candidate.metadata#>>'{model_proposal,proposal,proposal_id}'<>NEW.proposal->>'proposal_id'
     OR mut.metadata#>>'{model_proposal,proposal,proposal_id}'<>NEW.proposal->>'proposal_id' THEN
    RAISE EXCEPTION 'durable challenger does not retain the linked normalized proposal';
  END IF;
  RETURN NEW;
END; $$;

CREATE TRIGGER e1_cognitive_proposal_lineage_valid
BEFORE INSERT ON evolution.e1_cognitive_proposal_lineage
FOR EACH ROW EXECUTE FUNCTION evolution.validate_e1_cognitive_proposal_lineage();

CREATE OR REPLACE FUNCTION evolution.freeze_e1_cognitive_proposal_lineage()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'cognitive proposal lineage is immutable';
END; $$;
CREATE TRIGGER e1_cognitive_proposal_lineage_immutable
BEFORE UPDATE OR DELETE ON evolution.e1_cognitive_proposal_lineage
FOR EACH ROW EXECUTE FUNCTION evolution.freeze_e1_cognitive_proposal_lineage();

CREATE TRIGGER artifact_reference_lock
BEFORE INSERT ON evolution.e1_cognitive_proposal_lineage
FOR EACH ROW EXECUTE FUNCTION runtime.lock_artifact_references();

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
    GRANT SELECT,INSERT ON evolution.e1_cognitive_proposal_lineage TO agentic_runtime_runtime;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_evaluator') THEN
    GRANT SELECT ON evolution.e1_cognitive_proposal_lineage TO agentic_runtime_evaluator;
  END IF;
END $$;
