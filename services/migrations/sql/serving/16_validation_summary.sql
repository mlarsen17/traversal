CREATE OR REPLACE VIEW serving.validation_summary AS
WITH finding_rollup AS (
  SELECT
    vf.validation_run_id,
    COUNT(*) AS rules_evaluated,
    SUM(CASE WHEN vf.passed THEN 1 ELSE 0 END) AS rules_passed,
    SUM(CASE WHEN vf.passed = FALSE THEN 1 ELSE 0 END) AS rules_failed,
    SUM(
      CASE
        WHEN vf.passed = FALSE
          AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'HARD'
        THEN 1
        ELSE 0
      END
    ) AS hard_rules_failed,
    SUM(
      CASE
        WHEN vf.passed = FALSE
          AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'SOFT'
        THEN 1
        ELSE 0
      END
    ) AS soft_rules_failed,
    SUM(vf.violations_count) AS total_violations
  FROM validation_finding vf
  JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
  JOIN validation_rule vrule ON vrule.rule_id = vf.rule_id
  LEFT JOIN validation_rule_set_rule vrsr
    ON vrsr.rule_set_id = vr.rule_set_id
   AND vrsr.rule_id = vf.rule_id
  GROUP BY vf.validation_run_id
)
SELECT
  vr.validation_run_id,
  vr.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  vr.rule_set_id,
  vr.started_at,
  vr.ended_at,
  vr.status AS validation_run_status,
  vr.outcome AS validation_outcome,
  vr.total_rows,
  fr.rules_evaluated,
  fr.rules_passed,
  fr.rules_failed,
  fr.hard_rules_failed,
  fr.soft_rules_failed,
  fr.total_violations
FROM validation_run vr
JOIN submission s ON s.submission_id = vr.submission_id
LEFT JOIN finding_rollup fr ON fr.validation_run_id = vr.validation_run_id;
