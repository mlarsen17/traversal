SELECT
  state,
  file_type,
  atomic_month,
  canonical_latest_status,
  pending_rebuild_events,
  canonical_build_attempts,
  canonical_build_successes,
  last_success_at
FROM serving.canonical_month_status
WHERE pending_rebuild_events > 0
   OR canonical_latest_status = 'FAILED'
ORDER BY pending_rebuild_events DESC, atomic_month DESC
LIMIT 200;
