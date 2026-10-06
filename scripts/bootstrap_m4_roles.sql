-- Run once as a local PostgreSQL administrator before M4B migrations/tests.
-- Roles are NOLOGIN capability groups; deployment identities inherit only one group.
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_migration') THEN CREATE ROLE agentic_runtime_migration NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_runtime') THEN CREATE ROLE agentic_runtime_runtime NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_evaluator') THEN CREATE ROLE agentic_runtime_evaluator NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_verifier') THEN CREATE ROLE agentic_runtime_verifier NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_governance') THEN CREATE ROLE agentic_runtime_governance NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor') THEN CREATE ROLE agentic_promotion_executor NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_e1_promotion') THEN CREATE ROLE agentic_runtime_e1_promotion NOLOGIN; END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_e2_promotion') THEN CREATE ROLE agentic_runtime_e2_promotion NOLOGIN; END IF;
END $$;

-- Procedure owners are non-login capability identities. Deployment/test
-- LOGIN identities inherit one capability group each.
GRANT SELECT,INSERT,UPDATE ON evolution.promotion_authorizations TO agentic_promotion_executor;
GRANT SELECT,UPDATE ON evolution.system_genomes,evolution.scope_champions TO agentic_promotion_executor;
GRANT SELECT ON evolution.promotion_decisions,evolution.evaluation_records TO agentic_promotion_executor;
GRANT SELECT,INSERT ON evolution.champion_history,evolution.evolution_events,evolution.governance_events TO agentic_promotion_executor;
GRANT CREATE ON SCHEMA evolution TO agentic_promotion_executor;
GRANT USAGE ON SCHEMA evolution TO agentic_promotion_executor;
ALTER FUNCTION evolution.execute_authorized_promotion(text) OWNER TO agentic_promotion_executor;
ALTER FUNCTION evolution.authorize_promotion(text,text,text,text,text,jsonb,text,text) OWNER TO agentic_promotion_executor;
REVOKE CREATE ON SCHEMA evolution FROM agentic_promotion_executor;

-- M7 E1 promotion is a separate capability. The ordinary runtime and
-- evaluator identities cannot invoke these deterministic governance functions.
GRANT SELECT ON evolution.e1_scope_policies,evolution.e1_genome_versions,
 evolution.e1_evaluation_pack_versions,evolution.e1_evaluation_runs,
 evolution.e1_comparisons,evolution.e1_promotion_authorizations,
 evolution.e1_promotion_decisions,evolution.e1_postpromotion_checks,
 evolution.e1_rollback_records,evolution.e1_executor_measurements TO agentic_promotion_executor;
-- The SECURITY DEFINER promotion function validates the frozen pack against
-- the authoritative M3 suite version. Its non-login owner needs read-only
-- access to that binding table.
GRANT SELECT ON evolution.eval_suite_versions TO agentic_promotion_executor;
-- M8 frozen comparison policy is read by the SECURITY DEFINER E1 promotion
-- procedure, not by its caller's login identity.
DO $$ BEGIN
 IF to_regclass('evolution.e1_campaign_comparison_policies') IS NOT NULL THEN
  GRANT SELECT ON evolution.e1_campaign_comparison_policies TO agentic_promotion_executor;
 END IF;
END $$;
-- Promotion revalidates that every cited measurement still resolves to
-- accepted runtime evidence. The executor can inspect these immutable/result
-- fields but cannot change runtime tasks, attempts, artifacts or model runs.
GRANT SELECT ON runtime.artifacts,runtime.tasks,runtime.attempts,runtime.model_runs
 TO agentic_promotion_executor;
GRANT SELECT ON runtime.improvement_campaigns,runtime.improvement_reservations TO agentic_promotion_executor;
GRANT SELECT ON runtime.improvement_reservation_dimensions TO agentic_promotion_executor;
GRANT UPDATE ON runtime.improvement_campaigns TO agentic_promotion_executor;
GRANT USAGE ON SCHEMA runtime TO agentic_promotion_executor;
GRANT INSERT,UPDATE ON evolution.e1_promotion_authorizations TO agentic_promotion_executor;
GRANT INSERT ON evolution.e1_promotion_decisions,evolution.e1_events,
 evolution.e1_rollback_records TO agentic_promotion_executor;
GRANT UPDATE ON evolution.system_genomes,evolution.scope_champions TO agentic_promotion_executor;
GRANT EXECUTE ON FUNCTION evolution.authorize_and_execute_e1(text,text,text),
 evolution.rollback_e1_on_failed_check(text,text) TO agentic_runtime_e1_promotion;
REVOKE agentic_promotion_executor FROM agentic_runtime_e1_promotion;

-- M10 E2 promotion is a separate authority capability; it is not inherited by
-- E1 callers. Deployment may grant it only to its explicitly governed login.
GRANT USAGE ON SCHEMA evolution TO agentic_runtime_e2_promotion;

GRANT SELECT ON evolution.e1_scope_policies,evolution.e1_genome_versions,
 evolution.e1_evaluation_pack_versions,evolution.e1_executor_measurements TO agentic_runtime_runtime;
GRANT INSERT ON evolution.e1_genome_versions,evolution.e1_executor_measurements,
 evolution.e1_comparisons,evolution.e1_events TO agentic_runtime_runtime;
GRANT INSERT ON evolution.e1_evaluation_runs TO agentic_runtime_evaluator;
GRANT INSERT ON evolution.e1_evaluation_pack_versions,evolution.e1_evaluation_runs,
 evolution.e1_postpromotion_checks TO agentic_runtime_evaluator;
GRANT UPDATE(status) ON evolution.system_genomes TO agentic_runtime_evaluator;
GRANT UPDATE(status) ON evolution.mutation_proposals TO agentic_runtime_runtime;
GRANT INSERT ON evolution.scopes,evolution.scope_champions,evolution.system_genomes,
 evolution.e1_scope_policies,evolution.e1_genome_versions,evolution.eval_suite_versions,
 evolution.e1_events
 TO agentic_runtime_governance;
ALTER FUNCTION evolution.authorize_and_execute_e1(text,text,text) OWNER TO agentic_promotion_executor;
ALTER FUNCTION evolution.rollback_e1_on_failed_check(text,text) OWNER TO agentic_promotion_executor;

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_migrator') THEN GRANT agentic_runtime_migration TO agentic_migrator; END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime') THEN GRANT agentic_runtime_runtime TO agentic_runtime; END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_evaluator') THEN GRANT agentic_runtime_evaluator TO agentic_evaluator; END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_governance') THEN GRANT agentic_runtime_governance TO agentic_governance; END IF;
END $$;

GRANT USAGE ON SCHEMA runtime,evolution TO agentic_runtime_runtime,agentic_runtime_evaluator,agentic_runtime_governance;
GRANT USAGE ON SCHEMA evolution TO agentic_runtime_e1_promotion;
GRANT USAGE,CREATE ON SCHEMA runtime,evolution TO agentic_runtime_migration;
-- E2's SECURITY DEFINER promotion functions must execute as the same
-- non-login authority recognized by the champion state guard. Migration
-- ownership is deliberately independent; the administrative bootstrap sets
-- the permanent procedure owner after the functions have been installed.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_promotion_executor')
    AND to_regprocedure('evolution.execute_e2_promotion(text,text,text)') IS NOT NULL THEN
  GRANT CREATE ON SCHEMA evolution TO agentic_promotion_executor;
  ALTER FUNCTION evolution.execute_e2_promotion(text,text,text) OWNER TO agentic_promotion_executor;
  ALTER FUNCTION evolution.execute_e2_rollback(text,text,text) OWNER TO agentic_promotion_executor;
  REVOKE CREATE ON SCHEMA evolution FROM agentic_promotion_executor;
 END IF;
END $$;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA runtime TO agentic_runtime_runtime;
REVOKE ALL ON runtime.mutation_policy FROM agentic_runtime_runtime;
DO $$ BEGIN
 IF to_regclass('runtime.cognitive_qualification_acceptances') IS NOT NULL THEN
  REVOKE INSERT,UPDATE,DELETE ON runtime.cognitive_qualification_acceptances FROM agentic_runtime_runtime;
 END IF;
END $$;
GRANT SELECT ON runtime.mutation_policy TO agentic_runtime_runtime;
GRANT SELECT ON ALL TABLES IN SCHEMA evolution TO agentic_runtime_runtime,agentic_runtime_evaluator,agentic_runtime_governance;
-- E2 hidden holdout definitions are row-filtered: candidate execution and the
-- development evaluator see DEVELOPMENT only; verifier/governance capabilities
-- retain the separate HOLDOUT partition.
DO $$ BEGIN
 IF to_regclass('evolution.e2_evaluation_scenarios') IS NOT NULL THEN
  REVOKE ALL ON evolution.e2_evaluation_scenarios FROM agentic_runtime_runtime,agentic_runtime_evaluator;
  GRANT SELECT ON evolution.e2_evaluation_scenarios TO agentic_runtime_runtime,agentic_runtime_evaluator;
  GRANT SELECT ON evolution.e2_evaluation_scenarios TO agentic_runtime_verifier,agentic_runtime_governance;
  GRANT INSERT ON evolution.e2_evaluation_scenarios TO agentic_runtime_governance;
 END IF;
END $$;
GRANT INSERT ON evolution.observations,evolution.mutation_proposals,evolution.system_genomes,
 evolution.scopes,evolution.genome_parents,evolution.evaluation_packs,evolution.promotion_decisions,evolution.evolution_events
 TO agentic_runtime_runtime;
GRANT EXECUTE ON FUNCTION evolution.record_challenger_outcome(text,text) TO agentic_runtime_runtime;
GRANT INSERT ON evolution.evaluation_records,evolution.evaluator_runs,evolution.eval_artifact_quarantine
 TO agentic_runtime_evaluator;
GRANT INSERT ON evolution.evolution_events TO agentic_runtime_evaluator;
GRANT EXECUTE ON FUNCTION evolution.execute_authorized_promotion(text) TO agentic_runtime_governance;
GRANT EXECUTE ON FUNCTION evolution.authorize_promotion(text,text,text,text,text,jsonb,text,text) TO agentic_runtime_governance;
REVOKE INSERT,UPDATE,DELETE ON evolution.promotion_authorizations,evolution.scope_champions,evolution.system_genomes FROM agentic_runtime_governance;
-- Governance may seed a new E1 scope and its initial champion during
-- controlled registration, but cannot update an existing champion pointer.
GRANT INSERT ON evolution.scope_champions,evolution.system_genomes TO agentic_runtime_governance;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA runtime,evolution TO agentic_runtime_runtime,agentic_runtime_evaluator,agentic_runtime_governance;

-- Integration-test supervisor only. Production workers must not be members of these groups.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='agentic_runtime_test') THEN
  GRANT agentic_runtime_runtime,agentic_runtime_evaluator,agentic_runtime_verifier,agentic_runtime_governance TO agentic_runtime_test;
  GRANT agentic_runtime_e1_promotion TO agentic_runtime_test;
  GRANT agentic_runtime_e2_promotion TO agentic_runtime_test;
 END IF;
END $$;
