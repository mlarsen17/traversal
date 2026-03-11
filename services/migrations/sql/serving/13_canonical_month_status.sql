CREATE OR REPLACE VIEW serving.canonical_month_status AS
WITH latest_build AS (
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
build_counts AS (
  SELECT
    cmb.state,
    cmb.file_type,
    cmb.atomic_month,
    COUNT(*) AS canonical_build_attempts,
    SUM(CASE WHEN cmb.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS canonical_build_successes,
    MAX(cmb.ended_at) FILTER (WHERE cmb.status = 'SUCCEEDED') AS last_success_at
  FROM canonical_month_build cmb
  GROUP BY cmb.state, cmb.file_type, cmb.atomic_month
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
),
winner_coverage AS (
  SELECT
    smp.state,
    smp.file_type,
    smp.atomic_month,
    COUNT(*) AS submitter_winner_count
  FROM submitter_month_pointer smp
  GROUP BY smp.state, smp.file_type, smp.atomic_month
)
SELECT
  COALESCE(lb.state, wc.state) AS state,
  COALESCE(lb.file_type, wc.file_type) AS file_type,
  COALESCE(lb.atomic_month, wc.atomic_month) AS atomic_month,
  lb.canonical_schema_id,
  lb.canonical_month_build_id,
  lb.status AS canonical_latest_status,
  lb.started_at AS canonical_latest_started_at,
  lb.ended_at AS canonical_latest_ended_at,
  lb.input_fingerprint,
  lb.output_prefix,
  cs.schema_version AS canonical_schema_version,
  bc.canonical_build_attempts,
  bc.canonical_build_successes,
  bc.last_success_at,
  COALESCE(qp.pending_rebuild_events, 0) AS pending_rebuild_events,
  COALESCE(wc.submitter_winner_count, 0) AS submitter_winner_count
FROM latest_build lb
FULL OUTER JOIN winner_coverage wc
  ON wc.state = lb.state
 AND wc.file_type = lb.file_type
 AND wc.atomic_month = lb.atomic_month
LEFT JOIN build_counts bc
  ON bc.state = COALESCE(lb.state, wc.state)
 AND bc.file_type = COALESCE(lb.file_type, wc.file_type)
 AND bc.atomic_month = COALESCE(lb.atomic_month, wc.atomic_month)
LEFT JOIN queue_pending qp
  ON qp.state = COALESCE(lb.state, wc.state)
 AND qp.file_type = COALESCE(lb.file_type, wc.file_type)
 AND qp.atomic_month = COALESCE(lb.atomic_month, wc.atomic_month)
LEFT JOIN canonical_schema cs
  ON cs.canonical_schema_id = lb.canonical_schema_id;
