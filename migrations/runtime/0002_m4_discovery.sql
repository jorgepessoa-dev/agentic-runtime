ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS scope_id text;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS fingerprint text;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS estimated_cost jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS uncertainty text;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS novelty text;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS risk text;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE runtime.opportunities ADD COLUMN IF NOT EXISTS triage_rationale text;
ALTER TABLE runtime.opportunities DROP CONSTRAINT IF EXISTS opportunities_status_check;
ALTER TABLE runtime.opportunities ADD CONSTRAINT opportunities_status_check CHECK
 (status IN ('OPEN','INVESTIGATING','ACTIONABLE','CLOSED','REJECTED','DETECTED','TRIAGED','SELECTED','DEFERRED','DUPLICATE','INVALIDATED','UNDER_INVESTIGATION','RESOLVED','EXPIRED'));
CREATE UNIQUE INDEX IF NOT EXISTS opportunity_scope_fingerprint_open
 ON runtime.opportunities(scope_id,fingerprint)
 WHERE fingerprint IS NOT NULL AND status IN ('OPEN','INVESTIGATING','ACTIONABLE');

CREATE TABLE runtime.improvement_campaigns (
 campaign_id text PRIMARY KEY,
 scope_id text NOT NULL,
 goal_id text REFERENCES runtime.goals(goal_id),
 status text NOT NULL CHECK(status IN ('CREATED','OBSERVING','OPPORTUNITIES_IDENTIFIED','SELECTED','HYPOTHESES_CREATED','CHALLENGERS_CREATED','EVALUATING','COMPLETED','STOPPED')),
 budget jsonb NOT NULL,
 exploration_allocation jsonb NOT NULL DEFAULT '{}'::jsonb,
 exploitation_allocation jsonb NOT NULL DEFAULT '{}'::jsonb,
 limits jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 ended_at timestamptz,
 stop_reason text,
 created_by text NOT NULL
);
CREATE TABLE runtime.opportunity_decisions (
 decision_id text PRIMARY KEY,
 opportunity_id text NOT NULL REFERENCES runtime.opportunities(opportunity_id),
 selected boolean NOT NULL,
 priority_class text NOT NULL CHECK(priority_class IN ('P0','P1','P2','DEFER')),
 rationale jsonb NOT NULL,
 campaign_id text REFERENCES runtime.improvement_campaigns(campaign_id),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE runtime.hypothesis_evidence (
 hypothesis_id text NOT NULL REFERENCES runtime.hypotheses(hypothesis_id),
 evidence_ref text NOT NULL,
 relation text NOT NULL CHECK(relation IN ('SUPPORTS','CONTRADICTS','SEARCHED_NO_RESULT')),
 search_scope text,
 PRIMARY KEY(hypothesis_id,evidence_ref,relation)
);
CREATE TABLE runtime.mutation_policy (
 path_pattern text PRIMARY KEY,
 tier text NOT NULL CHECK(tier IN ('E0','E1','E2','E3','E4')),
 autonomous_proposal boolean NOT NULL,
 autonomous_evaluation boolean NOT NULL,
 active_promotion boolean NOT NULL DEFAULT false CHECK(active_promotion=false),
 risk_class text NOT NULL
);
INSERT INTO runtime.mutation_policy(path_pattern,tier,autonomous_proposal,autonomous_evaluation,risk_class) VALUES
 ('routing.preference.*','E1',true,true,'LOW'),
 ('routing.eligibility.*','E1',true,true,'LOW'),
 ('context.policy.*','E1',true,true,'LOW'),
 ('retry.policy.*','E1',true,true,'LOW'),
 ('exploration_allocation.*','E1',true,true,'LOW'),
 ('review.policy.*','E2',true,true,'MODERATE'),
 ('escalation.policy.*','E2',true,true,'MODERATE'),
 ('workflow.topology.*','E2',true,true,'MODERATE');
