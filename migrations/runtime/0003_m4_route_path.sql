INSERT INTO runtime.mutation_policy(path_pattern,tier,autonomous_proposal,autonomous_evaluation,risk_class)
VALUES ('routes.*','E1',true,true,'LOW') ON CONFLICT(path_pattern) DO NOTHING;
