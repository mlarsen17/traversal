\timing on
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
  submission_status,
  COUNT(*)
FROM serving.submission_overview
GROUP BY submission_status;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
  day,
  submissions_received,
  parse_succeeded,
  validation_runs_failed,
  canonical_failed
FROM serving.dashboard_kpis_daily
WHERE day >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY day;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
  state,
  file_type,
  atomic_month,
  pending_rebuild_events,
  canonical_latest_status
FROM serving.canonical_month_status
WHERE pending_rebuild_events > 0 OR canonical_latest_status = 'FAILED'
ORDER BY pending_rebuild_events DESC, atomic_month DESC
LIMIT 100;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
  submitter_id,
  COUNT(*) FILTER (WHERE failed) AS failed_findings
FROM serving.validation_findings
WHERE validation_ended_at >= NOW() - INTERVAL '30 days'
GROUP BY submitter_id
ORDER BY failed_findings DESC
LIMIT 50;
