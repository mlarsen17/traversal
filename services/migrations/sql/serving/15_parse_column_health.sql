CREATE OR REPLACE VIEW serving.parse_column_health AS
WITH run_totals AS (
  SELECT
    pfm.parse_run_id,
    SUM(pfm.rows_written) AS rows_written
  FROM parse_file_metrics pfm
  GROUP BY pfm.parse_run_id
)
SELECT
  pr.parse_run_id,
  pr.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  pcm.column_name,
  pcm.null_count,
  pcm.invalid_count,
  rt.rows_written,
  CASE
    WHEN COALESCE(rt.rows_written, 0) > 0 THEN pcm.null_count::numeric / rt.rows_written
    ELSE NULL
  END AS null_rate,
  CASE
    WHEN COALESCE(rt.rows_written, 0) > 0 THEN pcm.invalid_count::numeric / rt.rows_written
    ELSE NULL
  END AS invalid_rate
FROM parse_column_metrics pcm
JOIN parse_run pr ON pr.parse_run_id = pcm.parse_run_id
JOIN submission s ON s.submission_id = pr.submission_id
LEFT JOIN run_totals rt ON rt.parse_run_id = pr.parse_run_id;
