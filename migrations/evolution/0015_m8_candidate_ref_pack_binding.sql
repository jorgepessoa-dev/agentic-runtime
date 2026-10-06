-- A reused immutable proposal is admissible only when the new campaign binds
-- it to its own frozen evaluator pack and versioned comparison policy.
ALTER TABLE evolution.e1_campaign_candidate_refs
  ADD COLUMN evaluation_pack_id text NOT NULL REFERENCES evolution.e1_evaluation_pack_versions(pack_id),
  ADD COLUMN evaluation_pack_hash text NOT NULL CHECK(evaluation_pack_hash ~ '^[0-9a-f]{64}$'),
  ADD COLUMN policy_hash text NOT NULL CHECK(policy_hash ~ '^[0-9a-f]{64}$');

CREATE OR REPLACE FUNCTION evolution.validate_e1_campaign_candidate_ref()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE src evolution.e1_cognitive_proposal_lineage%ROWTYPE;
DECLARE genome evolution.e1_genome_versions%ROWTYPE;
DECLARE campaign runtime.improvement_campaigns%ROWTYPE;
DECLARE policy evolution.e1_campaign_comparison_policies%ROWTYPE;
DECLARE pack evolution.e1_evaluation_pack_versions%ROWTYPE;
DECLARE champion_id text;
BEGIN
  SELECT * INTO src FROM evolution.e1_cognitive_proposal_lineage WHERE lineage_id=NEW.source_lineage_id;
  SELECT * INTO genome FROM evolution.e1_genome_versions WHERE genome_id=NEW.source_genome_id;
  SELECT * INTO campaign FROM runtime.improvement_campaigns WHERE campaign_id=NEW.campaign_id FOR UPDATE;
  SELECT * INTO policy FROM evolution.e1_campaign_comparison_policies WHERE campaign_id=NEW.campaign_id;
  SELECT * INTO pack FROM evolution.e1_evaluation_pack_versions WHERE pack_id=NEW.evaluation_pack_id;
  SELECT genome_id INTO champion_id FROM evolution.scope_champions WHERE scope_id=NEW.scope_id;
  IF src.lineage_id IS NULL OR genome.genome_id IS NULL OR campaign.campaign_id IS NULL
     OR campaign.status<>'CREATED' OR campaign.scope_id<>NEW.scope_id
     OR src.campaign_id<>NEW.source_campaign_id OR src.genome_id<>NEW.source_genome_id
     OR src.invocation_id<>NEW.source_invocation_id OR genome.scope_id<>NEW.scope_id
     OR genome.config_hash<>NEW.source_config_hash OR genome.parent_genome_id<>champion_id
     OR src.raw_artifact_ref<>NEW.raw_artifact_ref
     OR src.normalized_artifact_ref<>NEW.normalized_artifact_ref
     OR src.proposal_artifact_ref<>NEW.proposal_artifact_ref
     OR src.raw_hash<>NEW.raw_hash OR src.normalized_hash<>NEW.normalized_hash
     OR src.proposal_hash<>NEW.proposal_hash THEN
    RAISE EXCEPTION 'campaign candidate reference does not match immutable source challenger provenance';
  END IF;
  IF genome.canonical_config#>>'{routing,preference,default}'<>NEW.route_id THEN
    RAISE EXCEPTION 'source challenger configuration does not select the referenced route';
  END IF;
  IF policy.campaign_id IS NULL OR policy.scope_id<>NEW.scope_id OR policy.policy_hash<>NEW.policy_hash
     OR pack.pack_id IS NULL OR pack.scope_id<>NEW.scope_id
     OR pack.definition_hash<>NEW.evaluation_pack_hash
     OR pack.definition->>'campaign_id'<>NEW.campaign_id
     OR pack.definition->>'comparison_policy_hash'<>NEW.policy_hash THEN
    RAISE EXCEPTION 'campaign candidate reference is not bound to its frozen evaluation and comparison policy';
  END IF;
  RETURN NEW;
END; $$;
