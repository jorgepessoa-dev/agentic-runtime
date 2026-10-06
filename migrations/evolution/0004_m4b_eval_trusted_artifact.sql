ALTER TABLE evolution.mutation_proposals ADD COLUMN proposal_fingerprint text;
ALTER TABLE evolution.mutation_proposals ADD COLUMN evidence_fingerprint text;
CREATE UNIQUE INDEX mutation_scope_fingerprint_unique ON evolution.mutation_proposals(scope_id,proposal_fingerprint,evidence_fingerprint)
 WHERE proposal_fingerprint IS NOT NULL AND evidence_fingerprint IS NOT NULL;
CREATE TABLE evolution.eval_artifact_quarantine (
 quarantine_id text PRIMARY KEY,
 eval_suite_id text NOT NULL,
 eval_suite_version text NOT NULL,
 definition_ref text NOT NULL,
 expected_hash text NOT NULL,
 observed_hash text,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 created_by text NOT NULL,
 FOREIGN KEY(eval_suite_id,eval_suite_version) REFERENCES evolution.eval_suite_versions(eval_suite_id,version)
);
CREATE TABLE evolution.governance_events (
 event_id text PRIMARY KEY,
 event_type text NOT NULL,
 authorization_id text,
 scope_id text,
 actor_id text NOT NULL,
 payload jsonb NOT NULL,
 payload_hash text NOT NULL CHECK(payload_hash ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION evolution.execute_authorized_promotion(p_authorization_id text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path=evolution,pg_temp AS $$
DECLARE auth evolution.promotion_authorizations%ROWTYPE;
DECLARE active_id text;
DECLARE action_event text;
DECLARE payload_json jsonb;
DECLARE event_digest text;
BEGIN
 SELECT * INTO auth FROM evolution.promotion_authorizations
  WHERE authorization_id=p_authorization_id FOR UPDATE;
 IF NOT FOUND OR auth.status <> 'AUTHORIZED' THEN
  RAISE EXCEPTION 'authorization is absent or not executable';
 END IF;
 SELECT genome_id INTO active_id FROM evolution.scope_champions
  WHERE scope_id=auth.scope_id FOR UPDATE;
 IF active_id IS DISTINCT FROM auth.expected_champion_id THEN
  UPDATE evolution.promotion_authorizations SET status='STALE' WHERE authorization_id=p_authorization_id;
  RETURN 'STALE';
 END IF;
 PERFORM set_config('evolution.governed_promotion','authorized',true);
 UPDATE evolution.system_genomes SET status='SUPERSEDED' WHERE genome_id=active_id;
 UPDATE evolution.system_genomes SET status='CHAMPION',rollback_ref=active_id WHERE genome_id=auth.challenger_id;
 UPDATE evolution.scope_champions SET genome_id=auth.challenger_id,updated_at=now() WHERE scope_id=auth.scope_id;
 UPDATE evolution.promotion_authorizations SET status='EXECUTED' WHERE authorization_id=p_authorization_id;
 INSERT INTO evolution.champion_history(history_id,scope_id,prior_champion_id,resulting_champion_id,authorization_id,action)
  VALUES ('chist_'||md5(random()::text||clock_timestamp()::text),auth.scope_id,active_id,auth.challenger_id,p_authorization_id,auth.action);
 action_event := 'CHAMPION_'||auth.action;
 payload_json := jsonb_build_object('authorization_id',p_authorization_id,'prior',active_id,'result',auth.challenger_id,'action',auth.action);
 event_digest := encode(sha256(convert_to(payload_json::text,'UTF8')),'hex');
 INSERT INTO evolution.evolution_events(event_id,event_type,scope_id,actor_id,causation_ref,payload,payload_hash)
  VALUES ('eevt_'||md5(random()::text||clock_timestamp()::text),action_event,auth.scope_id,auth.authorized_by,auth.shadow_decision_id,payload_json,event_digest);
 INSERT INTO evolution.governance_events(event_id,event_type,authorization_id,scope_id,actor_id,payload,payload_hash)
  VALUES ('gevt_'||md5(random()::text||clock_timestamp()::text),action_event,p_authorization_id,auth.scope_id,auth.authorized_by,payload_json,event_digest);
 RETURN auth.challenger_id;
END; $$;
REVOKE ALL ON FUNCTION evolution.execute_authorized_promotion(text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION evolution.record_challenger_outcome(p_genome_id text,p_status text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=evolution,pg_temp AS $$
BEGIN
 IF p_status NOT IN ('CHALLENGER','REJECTED','RETAINED_FOR_DIVERSITY') THEN
  RAISE EXCEPTION 'invalid challenger lifecycle result';
 END IF;
 IF NOT EXISTS (SELECT 1 FROM evolution.system_genomes WHERE genome_id=p_genome_id AND status='CHALLENGER')
    OR EXISTS (SELECT 1 FROM evolution.scope_champions WHERE genome_id=p_genome_id) THEN
  RAISE EXCEPTION 'genome is not an unevaluated challenger';
 END IF;
 IF p_status <> 'CHALLENGER' AND NOT EXISTS (
   SELECT 1 FROM evolution.evaluation_records WHERE candidate_genome_id=p_genome_id AND status IN ('COMPLETED','FAILED','QUARANTINED')) THEN
  RAISE EXCEPTION 'candidate status requires a durable evaluation result';
 END IF;
 UPDATE evolution.system_genomes SET status=p_status WHERE genome_id=p_genome_id;
END; $$;
REVOKE ALL ON FUNCTION evolution.record_challenger_outcome(text,text) FROM PUBLIC;
