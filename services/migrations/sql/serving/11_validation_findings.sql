CREATE OR REPLACE VIEW serving.validation_findings AS
SELECT
  vf.validation_finding_id,
  vf.validation_run_id,
  vr.submission_id,
  s.submitter_id,
  s.state,
  s.file_type,
  s.layout_id,
  vr.rule_set_id,
  vf.rule_id,
  vrule.name AS rule_name,
  vrule.rule_kind,
  COALESCE(vrsr.severity_override, vrule.default_severity) AS severity,
  COALESCE(vrsr.threshold_type_override, vrule.default_threshold_type) AS threshold_type,
  COALESCE(vrsr.threshold_value_override, vrule.default_threshold_value) AS threshold_value,
  vf.scope_month,
  vf.violations_count,
  vf.denominator_count,
  vf.violations_rate,
  vf.passed,
  (vf.passed = FALSE) AS failed,
  vf.sample_object_key,
  vf.computed_at,
  vr.started_at AS validation_started_at,
  vr.ended_at AS validation_ended_at,
  vr.status AS validation_run_status,
  vr.outcome AS validation_outcome
FROM validation_finding vf
JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
JOIN submission s ON s.submission_id = vr.submission_id
JOIN validation_rule vrule ON vrule.rule_id = vf.rule_id
LEFT JOIN validation_rule_set_rule vrsr
  ON vrsr.rule_set_id = vr.rule_set_id
 AND vrsr.rule_id = vf.rule_id;
