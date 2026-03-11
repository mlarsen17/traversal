CREATE OR REPLACE VIEW serving.submission_timeline AS
SELECT
  s.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  'SUBMISSION_RECEIVED'::text AS event_type,
  s.status AS event_status,
  s.received_at AS event_at
FROM submission s
UNION ALL
SELECT
  pr.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  'PARSE_RUN'::text AS event_type,
  pr.status AS event_status,
  COALESCE(pr.ended_at, pr.started_at) AS event_at
FROM parse_run pr
JOIN submission s ON s.submission_id = pr.submission_id
UNION ALL
SELECT
  vr.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  'VALIDATION_RUN'::text AS event_type,
  COALESCE(vr.outcome, vr.status) AS event_status,
  COALESCE(vr.ended_at, vr.started_at) AS event_at
FROM validation_run vr
JOIN submission s ON s.submission_id = vr.submission_id
UNION ALL
SELECT
  sgb.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  'SUBMITTER_GOLD_BUILD'::text AS event_type,
  sgb.status AS event_status,
  COALESCE(sgb.ended_at, sgb.started_at) AS event_at
FROM submitter_gold_build sgb
JOIN submission s ON s.submission_id = sgb.submission_id;
