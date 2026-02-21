# Health Data Ingestion Platform

This repository includes Dagster orchestration, Postgres/SQLite metadata tracking, MinIO-backed S3-compatible object storage, Alembic migrations, and integration tests spanning intake through Gold-C canonical output.

## Pipeline state machine

- Intake: `RECEIVED` → `READY_FOR_PARSE` (or `NEEDS_REVIEW` for unknown classifications)
- Parse (P2): `READY_FOR_PARSE` → `PARSED` on success, `PARSE_FAILED` on hard failures
- Validation (P3): `PARSED` → `VALIDATED`, `VALIDATED_WITH_WARNINGS`, or `VALIDATION_FAILED`
- Submitter Gold (P4): winner chosen per `(state, file_type, submitter_id, atomic_month)`
- Canonical Gold (P5): statewide canonical partition built per `(state, file_type, atomic_month)`

## Storage conventions

Raw:

- `raw/{submitter_id}/{file_type}/{submission_id}/data/*`
- `raw/{submitter_id}/{file_type}/{submission_id}/manifest.generated.json`

Silver:

- `silver/{submitter_id}/{file_type}/{submission_id}/atomic_month=YYYY-MM/part-*.parquet`
- `silver/{submitter_id}/{file_type}/{submission_id}/atomic_month=__unknown__/part-*.parquet`
- `silver/{submitter_id}/{file_type}/{submission_id}/parse_report.json`

Submitter gold (P4):

- `gold_submitter/{state}/{file_type}/{submitter_id}/atomic_month=YYYY-MM/part-*.parquet`

Canonical statewide gold (P5):

- `gold_canonical/{state}/{file_type}/atomic_month=YYYY-MM/part-*.parquet`

Canonical output includes canonical schema fields plus lineage columns:

- `state`, `file_type`, `atomic_month`, `submitter_id`
- `source_submission_id`, `source_layout_id`
- `canonical_schema_version`, `canonical_built_at`

## Canonical registry + mappings

Canonical registry is data-driven from JSON under:

- `services/dagster/src/health_platform/canonical/schemas/{file_type}/v*.json`
- `services/dagster/src/health_platform/canonical/mappings/{file_type}/{layout_version}/v*.json`

`sync_canonical_registry` upserts:

- `canonical_schema` (`file_type`, schema version, columns)
- `canonical_mapping` (`layout_id` + schema mapping version)

To add a new layout mapping:

1. Add/verify the layout in `layouts/` and sync layout registry.
2. Add mapping JSON for that layout version and canonical schema version.
3. Run `sync_canonical_registry` asset.

## Canonical rebuild trigger flow

- P4 `build_submitter_gold_job` enqueues rows in `canonical_rebuild_queue` when month winners change.
- `canonical_rebuild_sensor` drains unprocessed queue rows, groups by `(state,file_type,atomic_month)`, and triggers `build_canonical_month_job`.
- Canonical builds compute an input fingerprint over winners + mapping/schema identity.
  - unchanged fingerprint => `SKIPPED`
  - changed fingerprint => overwrite month partition and record `SUCCEEDED`
- Failed builds are recorded and queue rows are marked processed; retries happen by re-enqueueing.

## Running checks

```bash
cd services/dagster
ruff format --check .
ruff check .
pytest -q
```
