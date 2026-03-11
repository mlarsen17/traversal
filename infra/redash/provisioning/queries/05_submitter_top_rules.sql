SELECT
  rule_name,
  severity,
  COUNT(*) FILTER (WHERE failed) AS failed_count,
  COUNT(*) AS evaluated_count,
  ROUND(100.0 * COUNT(*) FILTER (WHERE failed)::numeric / NULLIF(COUNT(*), 0), 2) AS failed_pct
FROM serving.validation_findings
WHERE validation_ended_at >= NOW() - INTERVAL '30 days'
GROUP BY rule_name, severity
ORDER BY failed_count DESC
LIMIT 50;
