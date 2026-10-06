-- The SECURITY DEFINER E1 promotion procedure is owned by this narrowly
-- privileged execution role in the deployed/test role topology. It needs
-- read-only access to the new immutable campaign policy binding.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor') THEN
    GRANT SELECT ON evolution.e1_campaign_comparison_policies
      TO agentic_promotion_executor;
  END IF;
END $$;
