-- Harden the newly introduced campaign policy freeze with an on-database
-- content hash check. Earlier applied migration files remain unchanged.
CREATE OR REPLACE FUNCTION evolution.freeze_e1_campaign_policy()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE campaign runtime.improvement_campaigns%ROWTYPE;
DECLARE expected_hash text;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION 'campaign comparison policy is immutable';
  END IF;
  SELECT * INTO campaign FROM runtime.improvement_campaigns
   WHERE campaign_id=NEW.campaign_id FOR UPDATE;
  IF campaign.campaign_id IS NULL OR campaign.status<>'CREATED'
     OR campaign.scope_id<>NEW.scope_id THEN
    RAISE EXCEPTION 'comparison policy must be frozen for its scope before campaign dispatch';
  END IF;
  IF EXISTS (SELECT 1 FROM evolution.e1_evaluation_runs WHERE campaign_id=NEW.campaign_id)
     OR EXISTS (SELECT 1 FROM runtime.cognitive_invocations WHERE campaign_id=NEW.campaign_id) THEN
    RAISE EXCEPTION 'comparison policy cannot be bound after execution starts';
  END IF;
  IF NEW.comparison_policy->>'improvement_metric'<>'latency_ms'
     OR NEW.comparison_policy->>'protected_quality_metric'<>'quality'
     OR (NEW.comparison_policy->>'minimum_improvement_fraction')::numeric<>0.05
     OR NEW.comparison_policy->>'monetary_evidence_mode'<>'CAMPAIGN_SAFETY_ONLY' THEN
    RAISE EXCEPTION 'M8 campaign comparison policy does not match the frozen quality/latency-only policy';
  END IF;
  expected_hash:=encode(sha256(convert_to(jsonb_build_object(
    'scope_id',NEW.scope_id,'policy_version',NEW.policy_version,
    'comparison_policy',NEW.comparison_policy)::text,'UTF8')),'hex');
  IF NEW.policy_hash IS DISTINCT FROM expected_hash THEN
    RAISE EXCEPTION 'campaign comparison policy hash does not match its frozen contents';
  END IF;
  RETURN NEW;
END; $$;
