SELECT
  day,
  submissions_received,
  parse_succeeded,
  parse_failed,
  validation_runs_succeeded,
  validation_runs_failed,
  canonical_succeeded,
  canonical_failed,
  canonical_skipped
FROM serving.dashboard_kpis_daily
WHERE day >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY day;
