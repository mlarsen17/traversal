CREATE OR REPLACE VIEW serving.submitter_month_status AS
WITH latest_canonical AS (
  SELECT *
  FROM (
    SELECT
      cmb.*,
      ROW_NUMBER() OVER (
        PARTITION BY cmb.state, cmb.file_type, cmb.atomic_month
        ORDER BY cmb.started_at DESC, cmb.canonical_month_build_id DESC
      ) AS rn
    FROM canonical_month_build cmb
  ) ranked
  WHERE ranked.rn = 1
),
queue_pending AS (
  SELECT
    crq.state,
    crq.file_type,
    crq.atomic_month,
    COUNT(*) AS pending_rebuild_events
  FROM canonical_rebuild_queue crq
  WHERE crq.processed_at IS NULL
  GROUP BY crq.state, crq.file_type, crq.atomic_month
)
SELECT
  smp.state,
  smp.file_type,
  smp.submitter_id,
  smp.atomic_month,
  smp.winning_submission_id,
  smp.replaced_submission_id,
  smp.winning_validation_run_id,
  smp.winner_timestamp,
  smp.updated_at,
  smp.reason,
  s.received_at AS winning_submission_received_at,
  vr.outcome AS winning_validation_outcome,
  vr.ended_at AS winning_validation_ended_at,
  lc.canonical_month_build_id,
  lc.status AS canonical_latest_status,
  lc.started_at AS canonical_latest_started_at,
  lc.ended_at AS canonical_latest_ended_at,
  COALESCE(qp.pending_rebuild_events, 0) AS pending_rebuild_events,
  (COALESCE(qp.pending_rebuild_events, 0) > 0) AS canonical_rebuild_pending
FROM submitter_month_pointer smp
LEFT JOIN submission s ON s.submission_id = smp.winning_submission_id
LEFT JOIN validation_run vr ON vr.validation_run_id = smp.winning_validation_run_id
LEFT JOIN latest_canonical lc
  ON lc.state = smp.state
 AND lc.file_type = smp.file_type
 AND lc.atomic_month = smp.atomic_month
LEFT JOIN queue_pending qp
  ON qp.state = smp.state
 AND qp.file_type = smp.file_type
 AND qp.atomic_month = smp.atomic_month;
