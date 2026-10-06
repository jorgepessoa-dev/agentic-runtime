-- Preserve legacy recorded-cost behavior for pre-versioned scope policies; new campaign policies remain explicit.
CREATE OR REPLACE FUNCTION evolution.authorize_and_execute_e1(
 p_authorization_id text,p_comparison_id text,p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,runtime,pg_temp AS $$
DECLARE c evolution.e1_comparisons%ROWTYPE;
DECLARE cp evolution.scope_champions%ROWTYPE;
DECLARE candidate evolution.e1_genome_versions%ROWTYPE;
DECLARE champion evolution.e1_genome_versions%ROWTYPE;
DECLARE baseline_run evolution.e1_evaluation_runs%ROWTYPE;
DECLARE pack evolution.e1_evaluation_pack_versions%ROWTYPE;
DECLARE policy evolution.e1_scope_policies%ROWTYPE;
DECLARE campaign_policy evolution.e1_campaign_comparison_policies%ROWTYPE;
DECLARE campaign runtime.improvement_campaigns%ROWTYPE;
DECLARE er evolution.e1_evaluation_runs%ROWTYPE;
DECLARE auth_hash text;
DECLARE decision_key text;
DECLARE ev_id text;
DECLARE event_payload jsonb;
DECLARE challenger_metrics jsonb;
DECLARE baseline_metrics jsonb;
DECLARE metric_key text;
DECLARE required_pct numeric;
DECLARE quality_key text;
DECLARE candidate_metric numeric;
DECLARE champion_metric numeric;
DECLARE candidate_quality numeric;
DECLARE champion_quality numeric;
DECLARE candidate_cost numeric;
DECLARE prior_auth evolution.e1_promotion_authorizations%ROWTYPE;
DECLARE accounted numeric;
DECLARE dimension_key text;
DECLARE limit_key text;
BEGIN
 IF p_actor IS DISTINCT FROM session_user THEN
   RAISE EXCEPTION 'promotion actor must match the authenticated database login';
 END IF;
 SELECT * INTO c FROM evolution.e1_comparisons WHERE comparison_id=p_comparison_id;
 IF NOT FOUND OR NOT c.eligible OR c.disposition<>'ELIGIBLE' THEN RAISE EXCEPTION 'comparison is not eligible'; END IF;
 SELECT * INTO prior_auth FROM evolution.e1_promotion_authorizations
   WHERE comparison_id=p_comparison_id OR (campaign_id=c.campaign_id AND candidate_genome_id=c.candidate_genome_id)
   ORDER BY authorized_at LIMIT 1 FOR UPDATE;
 IF FOUND THEN
   IF prior_auth.status='EXECUTED' THEN RETURN prior_auth.decision_id; END IF;
   IF prior_auth.status='STALE' THEN RETURN 'STALE'; END IF;
   IF prior_auth.status='REJECTED' AND prior_auth.decision_id IS NOT NULL THEN RETURN prior_auth.decision_id; END IF;
   RAISE EXCEPTION 'promotion authorization already exists';
 END IF;
 SELECT * INTO policy FROM evolution.e1_scope_policies WHERE scope_id=c.scope_id;
 SELECT * INTO campaign_policy FROM evolution.e1_campaign_comparison_policies WHERE campaign_id=c.campaign_id;
 IF FOUND THEN
   IF campaign_policy.scope_id<>c.scope_id OR c.policy_version<>campaign_policy.policy_version
      OR c.policy_hash<>campaign_policy.policy_hash THEN
     RAISE EXCEPTION 'comparison is not bound to the campaign immutable policy version';
   END IF;
   policy.promotion_policy:=campaign_policy.comparison_policy;
   policy.policy_hash:=campaign_policy.policy_hash;
 ELSE
   IF c.policy_version<>policy.policy_version OR c.policy_hash<>policy.policy_hash THEN
     RAISE EXCEPTION 'comparison is not bound to the immutable scope policy version';
   END IF;
 END IF;
 SELECT * INTO cp FROM evolution.scope_champions WHERE scope_id=c.scope_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'champion pointer missing'; END IF;
 SELECT * INTO prior_auth FROM evolution.e1_promotion_authorizations
   WHERE comparison_id=p_comparison_id OR (campaign_id=c.campaign_id AND candidate_genome_id=c.candidate_genome_id)
   ORDER BY authorized_at LIMIT 1 FOR UPDATE;
 IF FOUND THEN
   IF prior_auth.status='EXECUTED' THEN RETURN prior_auth.decision_id; END IF;
   IF prior_auth.status='STALE' THEN RETURN 'STALE'; END IF;
   IF prior_auth.status='REJECTED' AND prior_auth.decision_id IS NOT NULL THEN RETURN prior_auth.decision_id; END IF;
   RAISE EXCEPTION 'promotion authorization already exists';
 END IF;
 SELECT * INTO candidate FROM evolution.e1_genome_versions WHERE genome_id=c.candidate_genome_id;
 SELECT * INTO champion FROM evolution.e1_genome_versions WHERE genome_id=c.champion_genome_id;
   SELECT * INTO er FROM evolution.e1_evaluation_runs WHERE run_id=c.candidate_run_id;
   SELECT * INTO pack FROM evolution.e1_evaluation_pack_versions WHERE pack_id=er.pack_id;
 IF cp.genome_id<>c.champion_genome_id THEN
   auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text)::bytea),'hex');
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,session_user,'STALE');
   RETURN 'STALE';
 END IF;
 SELECT metrics INTO challenger_metrics FROM evolution.e1_evaluation_runs WHERE run_id=c.candidate_run_id;
 SELECT metrics INTO baseline_metrics FROM evolution.e1_evaluation_runs WHERE run_id=c.champion_run_id;
 SELECT * INTO baseline_run FROM evolution.e1_evaluation_runs WHERE run_id=c.champion_run_id;
 SELECT * INTO campaign FROM runtime.improvement_campaigns WHERE campaign_id=c.campaign_id FOR UPDATE;
 IF NOT FOUND OR campaign.status IN ('STOPPED','COMPLETED') THEN RAISE EXCEPTION 'campaign is not active'; END IF;
 IF NOT EXISTS(SELECT 1 FROM evolution.eval_suite_versions s
       WHERE s.eval_suite_id=pack.suite_id AND s.version=pack.suite_version
         AND s.scope_id=pack.scope_id
         AND s.integrity_hash=pack.definition->'suite_binding'->>'integrity_hash') THEN
   RAISE EXCEPTION 'frozen evaluation pack is not bound to the recorded immutable suite version';
 END IF;
   metric_key:=policy.promotion_policy->>'improvement_metric';
   quality_key:=policy.promotion_policy->>'protected_quality_metric';
   required_pct:=coalesce((policy.promotion_policy->>'required_improvement')::numeric,0);
   IF metric_key IS NULL OR quality_key IS NULL OR required_pct<0 THEN
     RAISE EXCEPTION 'E1 promotion policy is incomplete';
   END IF;
   candidate_metric:=(challenger_metrics->'aggregate'->>metric_key)::numeric;
   champion_metric:=(baseline_metrics->'aggregate'->>metric_key)::numeric;
   candidate_quality:=(challenger_metrics->'aggregate'->>quality_key)::numeric;
   champion_quality:=(baseline_metrics->'aggregate'->>quality_key)::numeric;
   candidate_cost:=(challenger_metrics->'aggregate'->>'cost_units')::numeric;
   IF candidate_metric IS NULL OR champion_metric IS NULL OR candidate_quality IS NULL
      OR champion_quality IS NULL THEN RAISE EXCEPTION 'required comparison metric missing'; END IF;
   IF coalesce(policy.promotion_policy->>'monetary_evidence_mode','LEGACY_RECORDED') IN ('AUTHORITATIVE_ONLY','LEGACY_RECORDED')
      AND candidate_cost IS NULL THEN RAISE EXCEPTION 'authoritative monetary promotion evidence missing'; END IF;
   IF coalesce(policy.promotion_policy->>'monetary_evidence_mode','LEGACY_RECORDED') NOT IN
      ('AUTHORITATIVE_ONLY','LEGACY_RECORDED','CAMPAIGN_SAFETY_ONLY') THEN
     RAISE EXCEPTION 'unknown monetary evidence policy';
   END IF;
   IF coalesce(policy.promotion_policy->>'monetary_evidence_mode','LEGACY_RECORDED')='AUTHORITATIVE_ONLY'
      AND challenger_metrics->'aggregate'->>'cost_provenance' IS DISTINCT FROM 'AUTHORITATIVE_BILLING' THEN
     RAISE EXCEPTION 'monetary comparison requires authoritative billing provenance';
   END IF;
   IF candidate.tier<>'E1' OR candidate.parent_genome_id<>champion.genome_id
      OR jsonb_typeof(candidate.changed_paths)<>'array'
      OR jsonb_array_length(candidate.changed_paths)=0 THEN
     RAISE EXCEPTION 'candidate is not a bounded child E1 genome';
   END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(candidate.changed_paths) AS changed(path)
       WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements_text(policy.allowed_paths) AS allowed(pattern)
         WHERE changed.path LIKE replace(allowed.pattern,'*','%'))) THEN
     RAISE EXCEPTION 'candidate changed a field outside the registered E1 allowlist';
   END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(er.result_refs || baseline_run.result_refs) AS result(ref)
       WHERE NOT EXISTS(SELECT 1 FROM evolution.e1_executor_measurements m
         JOIN runtime.artifacts a ON a.artifact_id=m.artifact_ref
         JOIN runtime.tasks t ON t.task_id=m.task_id
         JOIN runtime.attempts at ON at.attempt_id=m.attempt_id
         WHERE m.measurement_id=result.ref AND m.scope_id=c.scope_id AND m.verified
           AND a.verification_status='VERIFIED' AND t.status='ACCEPTED' AND at.status='ACCEPTED')) THEN
     RAISE EXCEPTION 'evaluation cites missing or unverified measurement evidence';
   END IF;
   IF policy.promotion_policy->>'improvement_direction'='MIN' THEN
     IF candidate_metric > champion_metric*(1-required_pct) THEN RAISE EXCEPTION 'required improvement threshold not met'; END IF;
   ELSIF policy.promotion_policy->>'improvement_direction'='MAX' THEN
     IF candidate_metric < champion_metric*(1+required_pct) THEN RAISE EXCEPTION 'required improvement threshold not met'; END IF;
   ELSE RAISE EXCEPTION 'unknown improvement direction'; END IF;
   IF candidate_quality < champion_quality THEN
     RAISE EXCEPTION 'protected quality regression';
   END IF;
   IF coalesce(policy.promotion_policy->>'monetary_evidence_mode','LEGACY_RECORDED') IN ('AUTHORITATIVE_ONLY','LEGACY_RECORDED')
      AND candidate_cost > coalesce((policy.promotion_policy->>'maximum_cost_units')::numeric,1e100) THEN
     RAISE EXCEPTION 'authoritative monetary cost gate failed';
   END IF;
 IF session_user IN (candidate.provenance->>'created_by',er.evaluator_id) THEN
   RAISE EXCEPTION 'promotion controller must be independent from proposer and evaluator';
 END IF;
 IF policy.mode='ACTIVE_E1' THEN
   IF NOT er.reproducible OR er.status<>'COMPLETED' OR er.evaluator_id=candidate.provenance->>'created_by'
      OR er.pack_id IS DISTINCT FROM pack.pack_id OR pack.scope_id<>c.scope_id
      OR er.genome_id<>candidate.genome_id OR er.baseline_genome_id<>champion.genome_id THEN
     RAISE EXCEPTION 'evaluation integrity or lineage gate failed';
   END IF;
   IF NOT EXISTS(SELECT 1 FROM evolution.e1_evaluation_runs cr WHERE cr.run_id=c.champion_run_id
      AND cr.status='COMPLETED' AND cr.reproducible AND cr.pack_id=pack.pack_id
      AND cr.genome_id=champion.genome_id) THEN RAISE EXCEPTION 'champion baseline was not evaluated under same pack'; END IF;
   IF EXISTS(SELECT 1 FROM runtime.improvement_reservations r WHERE r.campaign_id=c.campaign_id AND r.status='RESERVED') THEN
     RAISE EXCEPTION 'campaign has unsettled resource reservations';
   END IF;
   SELECT coalesce(sum(d.consumed),0) INTO accounted
     FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
     WHERE r.campaign_id=c.campaign_id AND d.dimension='experiment_units';
   IF campaign.budget ? 'max_experiment_units' AND accounted>(campaign.budget->>'max_experiment_units')::numeric THEN
     RAISE EXCEPTION 'campaign experiment budget exceeded';
   END IF;
   FOREACH dimension_key IN ARRAY ARRAY['monetary_cost','tokens_input','tokens_output'] LOOP
     limit_key:=CASE dimension_key WHEN 'monetary_cost' THEN 'max_cost'
       WHEN 'tokens_input' THEN 'max_tokens_input' WHEN 'tokens_output' THEN 'max_tokens_output' END;
     IF campaign.budget ? limit_key THEN
       IF EXISTS(SELECT 1 FROM runtime.improvement_reservations r
           JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
           WHERE r.campaign_id=c.campaign_id AND d.dimension=dimension_key AND d.unknown) THEN
         RAISE EXCEPTION 'unknown usage cannot satisfy hard campaign ceiling %',limit_key;
       END IF;
       SELECT coalesce(sum(d.consumed),0) INTO accounted
         FROM runtime.improvement_reservations r JOIN runtime.improvement_reservation_dimensions d USING(reservation_id)
         WHERE r.campaign_id=c.campaign_id AND d.dimension=dimension_key;
       IF accounted>(campaign.budget->>limit_key)::numeric THEN RAISE EXCEPTION 'campaign hard budget exceeded for %',dimension_key; END IF;
     END IF;
   END LOOP;
   IF champion.config_hash<> (SELECT config_hash FROM evolution.system_genomes WHERE genome_id=cp.genome_id)
      OR candidate.config_hash<>(SELECT config_hash FROM evolution.system_genomes WHERE genome_id=candidate.genome_id) THEN
     auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text)::bytea),'hex');
     INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
     VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,p_actor,'STALE');
     RETURN 'STALE';
   END IF;
   auth_hash:=encode(sha256((candidate.config_hash||champion.config_hash||pack.definition_hash||c.rationale::text||
       er.result_refs::text||baseline_run.result_refs::text||policy.policy_hash)::bytea),'hex');
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,champion.genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       auth_hash,policy.policy_hash,session_user,'AUTHORIZED');
   decision_key:='e1decision_'||replace(p_authorization_id,'e1auth_','');
   PERFORM set_config('evolution.governed_promotion','authorized',true);
   UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=champion.genome_id;
   UPDATE evolution.system_genomes SET status='CHAMPION',rollback_ref=champion.genome_id WHERE genome_id=candidate.genome_id;
   UPDATE evolution.scope_champions SET genome_id=candidate.genome_id,updated_at=now() WHERE scope_id=c.scope_id;
   INSERT INTO evolution.e1_promotion_decisions(decision_id,authorization_id,scope_id,campaign_id,
       prior_champion_id,promoted_genome_id,comparison_id,pack_id,evidence_hash,decision,mode,created_by)
   VALUES(decision_key,p_authorization_id,c.scope_id,c.campaign_id,champion.genome_id,candidate.genome_id,
       c.comparison_id,pack.pack_id,auth_hash,'PROMOTED','ACTIVE_E1',session_user);
   UPDATE evolution.e1_promotion_authorizations SET status='EXECUTED',decision_id=decision_key
       WHERE authorization_id=p_authorization_id;
   ev_id:='e1evt_'||replace(p_authorization_id,'e1auth_','');
   event_payload:=jsonb_build_object('authorization_id',p_authorization_id,'decision_id',decision_key,
       'from',champion.genome_id,'to',candidate.genome_id,'pack_hash',pack.definition_hash);
   INSERT INTO evolution.e1_events(event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
   VALUES(ev_id,c.scope_id,c.campaign_id,'E1_CHAMPION_PROMOTED',session_user,event_payload,
       encode(sha256(convert_to(event_payload::text,'UTF8')),'hex'));
   RETURN decision_key;
 END IF;
 IF policy.mode='SHADOW' THEN
   INSERT INTO evolution.e1_promotion_authorizations(authorization_id,scope_id,campaign_id,
       expected_champion_id,expected_champion_hash,candidate_genome_id,candidate_hash,comparison_id,
       pack_id,pack_hash,evidence_hash,policy_hash,authorized_by,status)
   VALUES(p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,champion.config_hash,
       candidate.genome_id,candidate.config_hash,c.comparison_id,pack.pack_id,pack.definition_hash,
       encode(sha256((candidate.config_hash||pack.definition_hash)::bytea),'hex'),policy.policy_hash,p_actor,'REJECTED');
   decision_key:='e1decision_'||replace(p_authorization_id,'e1auth_','');
   INSERT INTO evolution.e1_promotion_decisions(decision_id,authorization_id,scope_id,campaign_id,
       prior_champion_id,promoted_genome_id,comparison_id,pack_id,evidence_hash,decision,mode,created_by)
   VALUES(decision_key,p_authorization_id,c.scope_id,c.campaign_id,c.champion_genome_id,c.candidate_genome_id,
       c.comparison_id,pack.pack_id,encode(sha256((candidate.config_hash||pack.definition_hash)::bytea),'hex'),
       'WOULD_PROMOTE','SHADOW',session_user);
   UPDATE evolution.e1_promotion_authorizations SET decision_id=decision_key WHERE authorization_id=p_authorization_id;
   event_payload:=jsonb_build_object('authorization_id',p_authorization_id,'decision_id',decision_key,
       'candidate_genome_id',candidate.genome_id,'comparison_id',c.comparison_id,
       'pack_hash',pack.definition_hash,'mode','SHADOW','decision','WOULD_PROMOTE');
   ev_id:='e1evt_'||replace(p_authorization_id,'e1auth_','');
   INSERT INTO evolution.e1_events(event_id,scope_id,campaign_id,event_type,actor_id,payload,payload_hash)
   VALUES(ev_id,c.scope_id,c.campaign_id,'E1_SHADOW_DECISION_RECORDED',session_user,event_payload,
       encode(sha256(convert_to(event_payload::text,'UTF8')),'hex'));
   RETURN 'WOULD_PROMOTE';
 END IF;
 RAISE EXCEPTION 'candidate, baseline or mode is stale/unauthorized';
END; $$;
