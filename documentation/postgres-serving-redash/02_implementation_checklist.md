# Postgres Serving Layer + Redash: Implementation Checklist

## Implementation status (2026-03-11)

- Completed:
  - `0008_p6_serving_redash` Alembic revision after `0007_p5_canonical_gold`
  - `0009_p6_serving_perf_indexes` Alembic revision for serving-related performance indexes
  - `serving` schema creation and all listed view definitions (core + optional) under `services/migrations/sql/serving/`
  - Migration-driven view lifecycle (upgrade creates/replaces views; downgrade drops views then schema)
  - Docker Compose Redash stack (`redash_postgres`, `redash_redis`, `redash_init`, `redash_server`, `redash_worker`, `redash_scheduler`)
  - First-boot Compose automation via `redash_bootstrap` one-shot service (admin user, datasource, content bootstrap)
  - Dedicated read-only role provisioning for BI (`redash_reader`) with grants on `serving` via `metadata_grants` service
  - Redash provisioning automation for saved queries, dashboards, schedules, and alerts via `infra/redash/provisioning/bootstrap_redash.py`
  - Performance baseline automation (`infra/tests/serving_performance_baseline.sh`) using `EXPLAIN (ANALYZE, BUFFERS)`
  - End-to-end connectivity automation (`infra/tests/serving_connectivity_smoke.sh`) including read-only permission enforcement checks

## Scope

Planning artifact for implementation of:

- `serving` schema in Postgres
- Dashboard-serving SQL views
- Redash integration in Docker Compose

This document is implementation-oriented but intentionally not executable code/migrations.

---

## Phase A: Schema and migration planning

### A1. Create `serving` schema

- Add Alembic revision after `0007_p5_canonical_gold`.
- Create schema with idempotent SQL (`CREATE SCHEMA IF NOT EXISTS serving`).
- Add downgrade behavior that drops views first, then schema if empty.

### A2. View type strategy (plain vs materialized)

- Start with plain views for first increment:
  - always fresh
  - minimal operational overhead
  - easier to iterate on definitions
- Move specific high-cost views to materialized only if query latency or load requires it.

### A3. Naming conventions

- Schema: `serving`
- Views: snake_case, business-grain specific
- Required initial names:
  - `serving.submission_overview`
  - `serving.validation_findings`
  - `serving.submitter_month_status`
  - `serving.canonical_month_status`
  - `serving.dashboard_kpis_daily`
- Optional:
  - `serving.parse_column_health`
  - `serving.validation_summary`
  - `serving.submission_timeline`

### A4. SQL file location

- Create SQL folder:
  - `services/migrations/sql/serving/`
- Recommended split:
  - `00_schema.sql`
  - one file per view (`10_submission_overview.sql`, etc.)

### A5. Alembic management approach

- Alembic revision should run DDL via `op.execute(...)`.
- Prefer `CREATE OR REPLACE VIEW` to support iterative evolution.
- Keep view SQL in separate `.sql` files loaded by migration to avoid giant inline Python strings.

---

## Phase B: First serving views

## Important assumptions and uncertainty

- Column mismatch risk: runtime code references `submission.created_at`, but Alembic migrations do not define that column. Avoid making views depend on `created_at` unless schema is confirmed.
- `validation_finding.scope_month` currently appears nullable and may be mostly `NULL`; still include it for forward compatibility.

### B1. `serving.submission_overview`

- Row grain: `1 row per submission_id`
- Source tables:
  - `submission`
  - `layout_registry`
  - latest `parse_run` (+ aggregated `parse_file_metrics`)
  - latest `validation_run` (+ aggregated `validation_finding`)
  - latest `submitter_gold_build`
  - `submitter_month_pointer` (to count current winning months)
- Major derived fields:
  - lifecycle status, latest parse/validation status
  - parse row totals (`rows_read`, `rows_written`, `rows_rejected`)
  - hard/soft failed rule counts
  - `months_currently_winning`
- Dashboard support:
  - Operations Overview
  - Submitter Health

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.submission_overview AS
WITH latest_parse AS (
  SELECT *
  FROM (
    SELECT pr.*, ROW_NUMBER() OVER (PARTITION BY pr.submission_id ORDER BY pr.started_at DESC) rn
    FROM parse_run pr
  ) x WHERE rn = 1
),
parse_totals AS (
  SELECT parse_run_id,
         SUM(rows_read) AS rows_read,
         SUM(rows_written) AS rows_written,
         SUM(rows_rejected) AS rows_rejected
  FROM parse_file_metrics
  GROUP BY parse_run_id
),
latest_validation AS (
  SELECT *
  FROM (
    SELECT vr.*, ROW_NUMBER() OVER (PARTITION BY vr.submission_id ORDER BY vr.started_at DESC) rn
    FROM validation_run vr
  ) x WHERE rn = 1
),
validation_counts AS (
  SELECT vf.validation_run_id,
         SUM(CASE WHEN vf.passed = FALSE AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'HARD' THEN 1 ELSE 0 END) AS hard_failed_rules,
         SUM(CASE WHEN vf.passed = FALSE AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'SOFT' THEN 1 ELSE 0 END) AS soft_failed_rules
  FROM validation_finding vf
  JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
  JOIN validation_rule vrule ON vrule.rule_id = vf.rule_id
  LEFT JOIN validation_rule_set_rule vrsr
    ON vrsr.rule_set_id = vr.rule_set_id AND vrsr.rule_id = vf.rule_id
  GROUP BY vf.validation_run_id
),
latest_submitter_build AS (
  SELECT *
  FROM (
    SELECT sgb.*, ROW_NUMBER() OVER (PARTITION BY sgb.submission_id ORDER BY sgb.started_at DESC) rn
    FROM submitter_gold_build sgb
  ) x WHERE rn = 1
),
winner_counts AS (
  SELECT winning_submission_id AS submission_id, COUNT(*) AS months_currently_winning
  FROM submitter_month_pointer
  GROUP BY winning_submission_id
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
  lp.status AS parse_status,
  lp.started_at AS parse_started_at,
  lp.ended_at AS parse_ended_at,
  pt.rows_read AS parse_rows_read,
  pt.rows_written AS parse_rows_written,
  pt.rows_rejected AS parse_rows_rejected,
  lv.validation_run_id,
  lv.status AS validation_run_status,
  lv.outcome AS validation_outcome,
  lv.ended_at AS validation_ended_at,
  lv.total_rows AS validation_total_rows,
  vc.hard_failed_rules,
  vc.soft_failed_rules,
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
```

### B2. `serving.validation_findings`

- Row grain: `1 row per validation_finding_id` (equivalent to run+rule+scope in current model)
- Source tables:
  - `validation_finding`
  - `validation_run`
  - `submission`
  - `validation_rule`
  - `validation_rule_set_rule`
- Major derived fields:
  - effective severity/threshold values
  - `failed` boolean
- Dashboard support:
  - Submitter Health
  - rule-level failure trend widgets

SQL sketch:

```sql
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
  ON vrsr.rule_set_id = vr.rule_set_id AND vrsr.rule_id = vf.rule_id;
```

### B3. `serving.submitter_month_status`

- Row grain: `1 row per (state, file_type, submitter_id, atomic_month)`
- Source tables:
  - `submitter_month_pointer`
  - winner `submission`
  - winner `validation_run`
  - latest `canonical_month_build` per month
  - pending `canonical_rebuild_queue` per month
- Major derived fields:
  - canonical pending flag
  - canonical latest status timestamps
  - winner lineage references
- Dashboard support:
  - Month Control Plane
  - Submitter Health

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.submitter_month_status AS
WITH latest_canonical AS (
  SELECT *
  FROM (
    SELECT cmb.*,
           ROW_NUMBER() OVER (
             PARTITION BY cmb.state, cmb.file_type, cmb.atomic_month
             ORDER BY cmb.started_at DESC
           ) rn
    FROM canonical_month_build cmb
  ) x WHERE rn = 1
),
queue_pending AS (
  SELECT state, file_type, atomic_month, COUNT(*) AS pending_rebuild_events
  FROM canonical_rebuild_queue
  WHERE processed_at IS NULL
  GROUP BY state, file_type, atomic_month
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
```

### B4. `serving.canonical_month_status`

- Row grain: `1 row per (state, file_type, atomic_month)`
- Source tables:
  - `canonical_month_build`
  - `canonical_rebuild_queue`
  - `submitter_month_pointer`
  - `canonical_schema`
- Major derived fields:
  - latest canonical status
  - attempts/success counts
  - pending queue count
  - winner coverage count
- Dashboard support:
  - Month Control Plane
  - Operations Overview

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.canonical_month_status AS
WITH latest_build AS (
  SELECT *
  FROM (
    SELECT cmb.*,
           ROW_NUMBER() OVER (
             PARTITION BY cmb.state, cmb.file_type, cmb.atomic_month
             ORDER BY cmb.started_at DESC
           ) rn
    FROM canonical_month_build cmb
  ) x WHERE rn = 1
),
build_counts AS (
  SELECT state, file_type, atomic_month,
         COUNT(*) AS canonical_build_attempts,
         SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS canonical_build_successes,
         MAX(ended_at) FILTER (WHERE status = 'SUCCEEDED') AS last_success_at
  FROM canonical_month_build
  GROUP BY state, file_type, atomic_month
),
queue_pending AS (
  SELECT state, file_type, atomic_month, COUNT(*) AS pending_rebuild_events
  FROM canonical_rebuild_queue
  WHERE processed_at IS NULL
  GROUP BY state, file_type, atomic_month
),
winner_coverage AS (
  SELECT state, file_type, atomic_month, COUNT(*) AS submitter_winner_count
  FROM submitter_month_pointer
  GROUP BY state, file_type, atomic_month
)
SELECT
  COALESCE(lb.state, wc.state) AS state,
  COALESCE(lb.file_type, wc.file_type) AS file_type,
  COALESCE(lb.atomic_month, wc.atomic_month) AS atomic_month,
  lb.canonical_schema_id,
  lb.status AS canonical_latest_status,
  lb.started_at AS canonical_latest_started_at,
  lb.ended_at AS canonical_latest_ended_at,
  lb.input_fingerprint,
  lb.output_prefix,
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
 AND qp.atomic_month = COALESCE(lb.atomic_month, wc.atomic_month);
```

### B5. `serving.dashboard_kpis_daily`

- Row grain: `1 row per UTC day`
- Source tables:
  - `submission`
  - `parse_run`
  - `validation_run`
  - `submitter_gold_build`
  - `canonical_month_build`
- Major derived fields:
  - daily counts by stage outcome
- Dashboard support:
  - Operations Overview KPI tiles/trends

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.dashboard_kpis_daily AS
WITH bounds AS (
  SELECT
    COALESCE(MIN(DATE(received_at)), CURRENT_DATE - INTERVAL '30 days') AS min_day,
    GREATEST(COALESCE(MAX(DATE(received_at)), CURRENT_DATE), CURRENT_DATE) AS max_day
  FROM submission
),
days AS (
  SELECT generate_series((SELECT min_day FROM bounds), (SELECT max_day FROM bounds), INTERVAL '1 day')::date AS day
),
submission_kpi AS (
  SELECT DATE(received_at) AS day, COUNT(*) AS submissions_received
  FROM submission
  GROUP BY DATE(received_at)
),
parse_kpi AS (
  SELECT DATE(ended_at) AS day,
         COUNT(*) FILTER (WHERE status = 'SUCCEEDED') AS parse_succeeded,
         COUNT(*) FILTER (WHERE status = 'FAILED') AS parse_failed
  FROM parse_run
  WHERE ended_at IS NOT NULL
  GROUP BY DATE(ended_at)
),
validation_kpi AS (
  SELECT DATE(ended_at) AS day,
         COUNT(*) FILTER (WHERE status = 'SUCCEEDED') AS validation_runs_succeeded,
         COUNT(*) FILTER (WHERE status = 'FAILED') AS validation_runs_failed,
         COUNT(*) FILTER (WHERE outcome = 'PASS') AS validation_outcome_pass,
         COUNT(*) FILTER (WHERE outcome = 'PASS_WITH_WARNINGS') AS validation_outcome_warn,
         COUNT(*) FILTER (WHERE outcome = 'FAIL_HARD') AS validation_outcome_fail_hard
  FROM validation_run
  WHERE ended_at IS NOT NULL
  GROUP BY DATE(ended_at)
),
submitter_gold_kpi AS (
  SELECT DATE(ended_at) AS day,
         COUNT(*) FILTER (WHERE status = 'SUCCEEDED') AS submitter_gold_succeeded,
         COUNT(*) FILTER (WHERE status = 'FAILED') AS submitter_gold_failed
  FROM submitter_gold_build
  WHERE ended_at IS NOT NULL
  GROUP BY DATE(ended_at)
),
canonical_kpi AS (
  SELECT DATE(ended_at) AS day,
         COUNT(*) FILTER (WHERE status = 'SUCCEEDED') AS canonical_succeeded,
         COUNT(*) FILTER (WHERE status = 'SKIPPED') AS canonical_skipped,
         COUNT(*) FILTER (WHERE status = 'FAILED') AS canonical_failed
  FROM canonical_month_build
  WHERE ended_at IS NOT NULL
  GROUP BY DATE(ended_at)
)
SELECT
  d.day,
  COALESCE(sk.submissions_received, 0) AS submissions_received,
  COALESCE(pk.parse_succeeded, 0) AS parse_succeeded,
  COALESCE(pk.parse_failed, 0) AS parse_failed,
  COALESCE(vk.validation_runs_succeeded, 0) AS validation_runs_succeeded,
  COALESCE(vk.validation_runs_failed, 0) AS validation_runs_failed,
  COALESCE(vk.validation_outcome_pass, 0) AS validation_outcome_pass,
  COALESCE(vk.validation_outcome_warn, 0) AS validation_outcome_warn,
  COALESCE(vk.validation_outcome_fail_hard, 0) AS validation_outcome_fail_hard,
  COALESCE(sgk.submitter_gold_succeeded, 0) AS submitter_gold_succeeded,
  COALESCE(sgk.submitter_gold_failed, 0) AS submitter_gold_failed,
  COALESCE(ck.canonical_succeeded, 0) AS canonical_succeeded,
  COALESCE(ck.canonical_skipped, 0) AS canonical_skipped,
  COALESCE(ck.canonical_failed, 0) AS canonical_failed
FROM days d
LEFT JOIN submission_kpi sk ON sk.day = d.day
LEFT JOIN parse_kpi pk ON pk.day = d.day
LEFT JOIN validation_kpi vk ON vk.day = d.day
LEFT JOIN submitter_gold_kpi sgk ON sgk.day = d.day
LEFT JOIN canonical_kpi ck ON ck.day = d.day;
```

### B6. Optional `serving.parse_column_health`

- Row grain: `1 row per (parse_run_id, column_name)`
- Source tables:
  - `parse_column_metrics`, `parse_run`, `submission`, aggregated `parse_file_metrics`
- Major derived fields:
  - null/invalid rates using rows_written denominator
- Dashboard support:
  - Submitter Health parse diagnostics

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.parse_column_health AS
WITH run_totals AS (
  SELECT parse_run_id, SUM(rows_written) AS rows_written
  FROM parse_file_metrics
  GROUP BY parse_run_id
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
  CASE WHEN COALESCE(rt.rows_written, 0) > 0 THEN pcm.null_count::numeric / rt.rows_written ELSE NULL END AS null_rate,
  CASE WHEN COALESCE(rt.rows_written, 0) > 0 THEN pcm.invalid_count::numeric / rt.rows_written ELSE NULL END AS invalid_rate
FROM parse_column_metrics pcm
JOIN parse_run pr ON pr.parse_run_id = pcm.parse_run_id
JOIN submission s ON s.submission_id = pr.submission_id
LEFT JOIN run_totals rt ON rt.parse_run_id = pr.parse_run_id;
```

### B7. Optional `serving.validation_summary`

- Row grain: `1 row per validation_run_id`
- Source tables:
  - `validation_run`, `submission`, aggregated `validation_finding`
- Major derived fields:
  - rules_failed totals split by hard/soft
  - total violations
- Dashboard support:
  - validation trend widgets and status rollups

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.validation_summary AS
WITH finding_rollup AS (
  SELECT
    vf.validation_run_id,
    COUNT(*) AS rules_evaluated,
    SUM(CASE WHEN vf.passed THEN 1 ELSE 0 END) AS rules_passed,
    SUM(CASE WHEN vf.passed = FALSE THEN 1 ELSE 0 END) AS rules_failed,
    SUM(CASE WHEN vf.passed = FALSE AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'HARD' THEN 1 ELSE 0 END) AS hard_rules_failed,
    SUM(CASE WHEN vf.passed = FALSE AND COALESCE(vrsr.severity_override, vrule.default_severity) = 'SOFT' THEN 1 ELSE 0 END) AS soft_rules_failed,
    SUM(vf.violations_count) AS total_violations
  FROM validation_finding vf
  JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
  JOIN validation_rule vrule ON vrule.rule_id = vf.rule_id
  LEFT JOIN validation_rule_set_rule vrsr
    ON vrsr.rule_set_id = vr.rule_set_id AND vrsr.rule_id = vf.rule_id
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
```

### B8. Optional `serving.submission_timeline`

- Row grain: `1 row per submission event`
- Source tables:
  - `submission`, `parse_run`, `validation_run`, `submitter_gold_build`, `submitter_month_pointer`
- Major derived fields:
  - normalized `event_type`, `event_status`, `event_at`
- Dashboard support:
  - drill-through timeline

SQL sketch:

```sql
CREATE OR REPLACE VIEW serving.submission_timeline AS
SELECT submission_id, submitter_id, state, file_type,
       'SUBMISSION_RECEIVED'::text AS event_type,
       status AS event_status,
       received_at AS event_at
FROM submission
UNION ALL
SELECT pr.submission_id, s.submitter_id, s.state, s.file_type,
       'PARSE_RUN'::text, pr.status, COALESCE(pr.ended_at, pr.started_at)
FROM parse_run pr
JOIN submission s ON s.submission_id = pr.submission_id
UNION ALL
SELECT vr.submission_id, s.submitter_id, s.state, s.file_type,
       'VALIDATION_RUN'::text, COALESCE(vr.outcome, vr.status), COALESCE(vr.ended_at, vr.started_at)
FROM validation_run vr
JOIN submission s ON s.submission_id = vr.submission_id
UNION ALL
SELECT sgb.submission_id, s.submitter_id, s.state, s.file_type,
       'SUBMITTER_GOLD_BUILD'::text, sgb.status, COALESCE(sgb.ended_at, sgb.started_at)
FROM submitter_gold_build sgb
JOIN submission s ON s.submission_id = sgb.submission_id;
```

---

## Phase C: Redash integration planning

### C1. Services to add in compose

- `redash_server`
- `redash_worker`
- `redash_scheduler`
- `redash_redis`
- `redash_postgres` (for Redash application metadata)

### C2. Redash metadata DB strategy

- Use separate Postgres DB for Redash internals.
- Do not reuse platform `metadata_postgres` for Redash application tables.

### C3. Redash data source to platform Postgres

- Configure data source in Redash pointing to platform `metadata_postgres`.
- Use Docker DNS hostname (`metadata_postgres`) and existing metadata DB name.
- Validate with a simple query against `serving.submission_overview`.

### C4. Permission model

- Create dedicated read user (e.g., `redash_reader`).
- Grants:
  - `CONNECT` on metadata DB
  - `USAGE` on `serving` schema
  - `SELECT` on all `serving` views
- No write permissions, no DDL.
- Keep base operational tables ungranted unless explicitly required.

### C5. Initial rollout

- Publish vetted saved queries first.
- Build three initial dashboards from those queries.
- Add schedule refresh and basic alerts for failed/pending thresholds.

---

## Phase D: Dashboard rollout planning

### D1. Operations Overview

- Widgets:
  - status distribution of submissions
  - daily submissions/parse/validation/canonical trend lines
  - canonical pending rebuild count
  - latest failed runs list
- Views:
  - `serving.submission_overview`
  - `serving.dashboard_kpis_daily`
  - `serving.canonical_month_status`
- Filters:
  - date range
  - `state`
  - `file_type`
  - `submitter_id`

### D2. Submitter Health

- Widgets:
  - fail/warn rates by submitter
  - top failing rules
  - parse null/invalid hotspots
  - months currently winning per submitter
- Views:
  - `serving.validation_findings`
  - `serving.submission_overview`
  - `serving.parse_column_health` (optional)
  - `serving.submitter_month_status`
- Filters:
  - `submitter_id`
  - `state`
  - `file_type`
  - date range
  - `atomic_month`

### D3. Month Control Plane

- Widgets:
  - month x submitter winner table
  - canonical latest status by month
  - pending rebuild queue panel
  - failed canonical attempts and last success timestamp
- Views:
  - `serving.submitter_month_status`
  - `serving.canonical_month_status`
- Filters:
  - `state`
  - `file_type`
  - month range / `atomic_month`
  - canonical status

---

## Phase E: Performance and maintenance planning

### E1. Candidate indexes on source tables

- `parse_run(submission_id, started_at DESC)`
- `validation_run(submission_id, started_at DESC)`
- `validation_finding(validation_run_id, rule_id, passed)`
- `submitter_month_pointer(winning_submission_id)`
- `canonical_month_build(state, file_type, atomic_month, started_at DESC)` (validate if existing scope index is sufficient)

### E2. Materialized-view decision points

- Introduce materialization if:
  - dashboard queries are consistently slow
  - high BI concurrency causes load concerns
  - expensive aggregations are repeatedly recomputed

### E3. Refresh approach (later phase)

- Add explicit refresh orchestration via Dagster after relevant jobs.
- For Postgres materialized views, use concurrent refresh with required indexes.

### E4. Contract stability and schema evolution

- Treat `serving.*` as stable API.
- Prefer additive column changes.
- For breaking semantics, add versioned replacement view (`*_v2`) and deprecate old view.

---

## Phase F: Testing and validation planning

### F1. Serving view validation

- Migration smoke:
  - upgrade creates schema/views
  - downgrade removes them in correct dependency order
- `SELECT * FROM serving.<view> LIMIT 1` smoke checks for all views
- Grain checks:
  - uniqueness assertions for intended keys

### F2. Query correctness checks

- Compare aggregate counts against base tables.
- Validate status/outcome mappings:
  - parse: `SUCCEEDED` vs `FAILED`
  - validation: `PASS`, `PASS_WITH_WARNINGS`, `FAIL_HARD`
  - canonical: `SUCCEEDED`, `SKIPPED`, `FAILED`
- Validate latest-run logic under rerun scenarios.

### F3. Redash connectivity tests

- Confirm Redash can authenticate to platform Postgres with read-only user.
- Confirm queries on `serving` succeed.
- Confirm direct base-table reads are denied if using strict permission model.

### F4. Performance smoke tests

- Run `EXPLAIN ANALYZE` for top dashboard queries.
- Test with typical filters (`state`, `file_type`, date/month).
- Record baseline latency before considering materialization.

---

## Recommended delivery sequence

1. Milestone 1: `serving` schema + 4 core views
   - `submission_overview`
   - `validation_findings`
   - `submitter_month_status`
   - `canonical_month_status`
2. Milestone 2: Redash stack + datasource + initial saved queries
3. Milestone 3: KPI and optional drilldown views
   - `dashboard_kpis_daily`
   - `validation_summary`
   - `submission_timeline`
   - optional `parse_column_health`
4. Milestone 4: tuning and hardening
   - index adjustments
   - dashboard query cleanup and alerts
5. Milestone 5: optional materialized views and refresh orchestration

---

## Risks and open questions

1. Should any view be materialized from day one?
2. Does production `submission` include `created_at` or not (schema mismatch risk)?
3. Should canonical row counts be persisted in metadata for better dashboarding?
4. Are current lineage fields sufficient for all drill-through needs?
5. What backward-compatibility policy will govern serving view changes?
