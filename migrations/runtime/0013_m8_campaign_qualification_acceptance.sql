-- A human/governance acceptance reuses existing qualification evidence for
-- one bounded campaign. It never rewrites last_probe or extends other scopes.
CREATE TABLE runtime.cognitive_qualification_acceptances (
    campaign_id text NOT NULL REFERENCES runtime.campaigns(campaign_id),
    route_id text NOT NULL REFERENCES runtime.cognitive_routes(route_id),
    evidence_ref text NOT NULL,
    qualified_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    accepted_by text NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(campaign_id,route_id),
    CHECK(qualified_at <= accepted_at AND expires_at > accepted_at)
);
CREATE FUNCTION runtime.freeze_qualification_acceptance() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'campaign qualification acceptance is immutable';
END; $$;
CREATE TRIGGER qualification_acceptance_immutable BEFORE UPDATE OR DELETE
ON runtime.cognitive_qualification_acceptances
FOR EACH ROW EXECUTE FUNCTION runtime.freeze_qualification_acceptance();
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN
  GRANT SELECT ON runtime.cognitive_qualification_acceptances TO agentic_runtime_runtime;
 END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_governance') THEN
  GRANT SELECT,INSERT ON runtime.cognitive_qualification_acceptances TO agentic_runtime_governance;
 END IF;
END $$;
