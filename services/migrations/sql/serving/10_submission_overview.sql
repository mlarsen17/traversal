CREATE OR REPLACE VIEW serving.submission_overview AS
WITH latest_parse AS (
  SELECT *
  FROM (
    SELECT
      pr.*,
      ROW_NUMBER() OVER (
        PARTITION BY pr.submission_id
        ORDER BY pr.started_at DESC, pr.parse_run_id DESC
      ) AS rn
    FROM parse_run pr
  ) ranked
  WHERE ranked.rn = 1
),
parse_totals AS (
  SELECT
    pfm.parse_run_id,
    SUM(pfm.rows_read) AS rows_read,
    SUM(pfm.rows_written) AS rows_written,
    SUM(pfm.rows_rejected) AS rows_rejected
  FROM parse_file_metrics pfm
  GROUP BY pfm.parse_run_id
),
latest_validation AS (
  SELECT *
  FROM (
    SELECT
      vr.*,
      ROW_NUMBER() OVER (
        PARTITION BY vr.submission_id
        ORDER BY vr.started_at DESC, vr.validation_run_id DESC
      ) AS rn
    FROM validation_run vr
  ) ranked
  WHERE ranked.rn = 1
),
validation_counts AS (
  SELECT
    vf.validation_run_id,
    SUM(
      CASE
        WHEN vf.passed = FALSE
          AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'HARD'
        THEN 1
        ELSE 0
      END
    ) AS hard_failed_rules,
    SUM(
      CASE
        WHEN vf.passed = FALSE
          AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'SOFT'
        THEN 1
        ELSE 0
      END
    ) AS soft_failed_rules
  FROM validation_finding vf
  JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
  JOIN validation_rule vrule ON vrule.rule_id = vf.rule_id
  LEFT JOIN validation_rule_set_rule vrsr
    ON vrsr.rule_set_id = vr.rule_set_id
   AND vrsr.rule_id = vf.rule_id
  GROUP BY vf.validation_run_id
),
latest_submitter_build AS (
  SELECT *
  FROM (
    SELECT
      sgb.*,
      ROW_NUMBER() OVER (
        PARTITION BY sgb.submission_id
        ORDER BY sgb.started_at DESC, sgb.submitter_gold_build_id DESC
      ) AS rn
    FROM submitter_gold_build sgb
  ) ranked
  WHERE ranked.rn = 1
),
winner_counts AS (
  SELECT
    smp.winning_submission_id AS submission_id,
    COUNT(*) AS months_currently_winning
  FROM submitter_month_pointer smp
  GROUP BY smp.winning_submission_id
)
SELECT
  s.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  s.layout_id,
  lr.layout_version,
  s.coverage_start_month,
  s.coverage_end_month,
  s.received_at,
  s.status AS submission_status,
  lp.parse_run_id,
  lp.status AS parse_status,
  lp.started_at AS parse_started_at,
  lp.ended_at AS parse_ended_at,
  pt.rows_read AS parse_rows_read,
  pt.rows_written AS parse_rows_written,
  pt.rows_rejected AS parse_rows_rejected,
  lv.validation_run_id,
  lv.status AS validation_run_status,
  lv.outcome AS validation_outcome,
  lv.started_at AS validation_started_at,
  lv.ended_at AS validation_ended_at,
  lv.total_rows AS validation_total_rows,
  vc.hard_failed_rules,
  vc.soft_failed_rules,
  lsb.submitter_gold_build_id,
  lsb.status AS submitter_gold_status,
  lsb.started_at AS submitter_gold_started_at,
  lsb.ended_at AS submitter_gold_ended_at,
  COALESCE(wc.months_currently_winning, 0) AS months_currently_winning
FROM submission s
LEFT JOIN layout_registry lr ON lr.layout_id = s.layout_id
LEFT JOIN latest_parse lp ON lp.submission_id = s.submission_id
LEFT JOIN parse_totals pt ON pt.parse_run_id = lp.parse_run_id
LEFT JOIN latest_validation lv ON lv.submission_id = s.submission_id
LEFT JOIN validation_counts vc ON vc.validation_run_id = lv.validation_run_id
LEFT JOIN latest_submitter_build lsb ON lsb.submission_id = s.submission_id
LEFT JOIN winner_counts wc ON wc.submission_id = s.submission_id;
