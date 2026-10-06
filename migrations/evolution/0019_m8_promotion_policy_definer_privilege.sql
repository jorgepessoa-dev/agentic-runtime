-- The E1 authorization procedure is SECURITY DEFINER and owned by
-- agentic_promotion_executor. That execution identity must be able to read
-- the frozen campaign policy it validates; the login identity invoking the
-- function does not supply this authority.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor') THEN
    GRANT SELECT ON evolution.e1_campaign_comparison_policies
      TO agentic_promotion_executor;
  END IF;
END $$;
