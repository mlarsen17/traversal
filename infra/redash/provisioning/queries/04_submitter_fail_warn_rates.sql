SELECT
  submitter_id,
  state,
  file_type,
  COUNT(*) FILTER (WHERE failed AND severity = 'HARD') AS hard_fail_findings,
  COUNT(*) FILTER (WHERE failed AND severity = 'SOFT') AS soft_fail_findings,
  COUNT(*) AS total_findings,
  ROUND(100.0 * COUNT(*) FILTER (WHERE failed)::numeric / NULLIF(COUNT(*), 0), 2) AS failed_pct
FROM serving.validation_findings
WHERE validation_ended_at >= NOW() - INTERVAL '30 days'
GROUP BY submitter_id, state, file_type
ORDER BY hard_fail_findings DESC, soft_fail_findings DESC
LIMIT 200;
