SELECT
  sms.state,
  sms.file_type,
  sms.atomic_month,
  sms.submitter_id,
  sms.winning_submission_id,
  sms.winning_validation_outcome,
  sms.canonical_latest_status,
  sms.canonical_rebuild_pending,
  sms.pending_rebuild_events,
  cms.canonical_build_attempts,
  cms.canonical_build_successes,
  cms.last_success_at
FROM serving.submitter_month_status sms
LEFT JOIN serving.canonical_month_status cms
  ON cms.state = sms.state
 AND cms.file_type = sms.file_type
 AND cms.atomic_month = sms.atomic_month
ORDER BY sms.atomic_month DESC, sms.state, sms.file_type, sms.submitter_id
LIMIT 500;
