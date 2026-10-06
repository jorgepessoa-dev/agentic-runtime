-- M8: trusted route configuration may retain a public, versioned token-price
-- schedule. Worker-reported token counts remain explicitly unverified; the
-- resulting estimate is never treated as an invoice or settlement receipt.
ALTER TABLE runtime.cognitive_routes
    ADD COLUMN pricing jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(pricing)='object');

COMMENT ON COLUMN runtime.cognitive_routes.pricing IS
  'Optional public versioned input/output price schedule; never a credential. Estimates derived from worker-reported usage remain unverified.';
