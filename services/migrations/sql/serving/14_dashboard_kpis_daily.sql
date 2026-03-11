CREATE OR REPLACE VIEW serving.dashboard_kpis_daily AS
WITH bounds AS (
  SELECT
    COALESCE(MIN(DATE(s.received_at)), CURRENT_DATE - INTERVAL '30 days') AS min_day,
    GREATEST(COALESCE(MAX(DATE(s.received_at)), CURRENT_DATE), CURRENT_DATE) AS max_day
  FROM submission s
),
days AS (
  SELECT
    generate_series(
      (SELECT b.min_day FROM bounds b),
      (SELECT b.max_day FROM bounds b),
      INTERVAL '1 day'
    )::date AS day
),
submission_kpi AS (
  SELECT
    DATE(s.received_at) AS day,
    COUNT(*) AS submissions_received
  FROM submission s
  GROUP BY DATE(s.received_at)
),
parse_kpi AS (
  SELECT
    DATE(pr.ended_at) AS day,
    COUNT(*) FILTER (WHERE pr.status = 'SUCCEEDED') AS parse_succeeded,
    COUNT(*) FILTER (WHERE pr.status = 'FAILED') AS parse_failed
  FROM parse_run pr
  WHERE pr.ended_at IS NOT NULL
  GROUP BY DATE(pr.ended_at)
),
validation_kpi AS (
  SELECT
    DATE(vr.ended_at) AS day,
    COUNT(*) FILTER (WHERE vr.status = 'SUCCEEDED') AS validation_runs_succeeded,
    COUNT(*) FILTER (WHERE vr.status = 'FAILED') AS validation_runs_failed,
    COUNT(*) FILTER (WHERE vr.outcome = 'PASS') AS validation_outcome_pass,
    COUNT(*) FILTER (WHERE vr.outcome = 'PASS_WITH_WARNINGS') AS validation_outcome_warn,
    COUNT(*) FILTER (WHERE vr.outcome = 'FAIL_HARD') AS validation_outcome_fail_hard
  FROM validation_run vr
  WHERE vr.ended_at IS NOT NULL
  GROUP BY DATE(vr.ended_at)
),
submitter_gold_kpi AS (
  SELECT
    DATE(sgb.ended_at) AS day,
    COUNT(*) FILTER (WHERE sgb.status = 'SUCCEEDED') AS submitter_gold_succeeded,
    COUNT(*) FILTER (WHERE sgb.status = 'FAILED') AS submitter_gold_failed
  FROM submitter_gold_build sgb
  WHERE sgb.ended_at IS NOT NULL
  GROUP BY DATE(sgb.ended_at)
),
canonical_kpi AS (
  SELECT
    DATE(cmb.ended_at) AS day,
    COUNT(*) FILTER (WHERE cmb.status = 'SUCCEEDED') AS canonical_succeeded,
    COUNT(*) FILTER (WHERE cmb.status = 'SKIPPED') AS canonical_skipped,
    COUNT(*) FILTER (WHERE cmb.status = 'FAILED') AS canonical_failed
  FROM canonical_month_build cmb
  WHERE cmb.ended_at IS NOT NULL
  GROUP BY DATE(cmb.ended_at)
)
SELECT
  d.day,
  COALESCE(sk.submissions_received, 0) AS submissions_received,
  COALESCE(pk.parse_succeeded, 0) AS parse_succeeded,
  COALESCE(pk.parse_failed, 0) AS parse_failed,
  COALESCE(vk.validation_runs_succeeded, 0) AS validation_runs_succeeded,
  COALESCE(vk.validation_runs_failed, 0) AS validation_runs_failed,
  COALESCE(vk.validation_outcome_pass, 0) AS validation_outcome_pass,
  COALESCE(vk.validation_outcome_warn, 0) AS validation_outcome_warn,
  COALESCE(vk.validation_outcome_fail_hard, 0) AS validation_outcome_fail_hard,
  COALESCE(sgk.submitter_gold_succeeded, 0) AS submitter_gold_succeeded,
  COALESCE(sgk.submitter_gold_failed, 0) AS submitter_gold_failed,
  COALESCE(ck.canonical_succeeded, 0) AS canonical_succeeded,
  COALESCE(ck.canonical_skipped, 0) AS canonical_skipped,
  COALESCE(ck.canonical_failed, 0) AS canonical_failed
FROM days d
LEFT JOIN submission_kpi sk ON sk.day = d.day
LEFT JOIN parse_kpi pk ON pk.day = d.day
LEFT JOIN validation_kpi vk ON vk.day = d.day
LEFT JOIN submitter_gold_kpi sgk ON sgk.day = d.day
LEFT JOIN canonical_kpi ck ON ck.day = d.day;
