# Health Data Ingestion Platform - Phase 2 Parse to Silver

This repository includes Dagster orchestration, Postgres metadata tracking, MinIO-backed S3-compatible object storage, Alembic migrations, and integration tests for intake registration and parsing to silver Parquet.

## Pipeline state machine

- Intake: `RECEIVED` → `READY_FOR_PARSE` (or `NEEDS_REVIEW` for unknown classifications)
- Parse (P2): `READY_FOR_PARSE` → `PARSED` on success, `PARSE_FAILED` on hard failures (missing files/layout, parser exceptions)

## Storage conventions

Raw:

- `raw/{submitter_id}/{file_type}/{submission_id}/data/*`
- `raw/{submitter_id}/{file_type}/{submission_id}/manifest.generated.json`

Silver:

- `silver/{submitter_id}/{file_type}/{submission_id}/atomic_month=YYYY-MM/part-*.parquet`
- `silver/{submitter_id}/{file_type}/{submission_id}/atomic_month=__unknown__/part-*.parquet` (fallback when anchor date is absent/unparseable)
- `silver/{submitter_id}/{file_type}/{submission_id}/parse_report.json`

Parser output also appends lineage columns in Parquet:

- `submission_id`
- `source_object_key`
- `ingested_at`
- `layout_id`
- `layout_version`

## Intake behavior

Submitters drop files under:

- `inbox/{submitter_id}/{filename}`
- optional grouped drops: `inbox/{submitter_id}/{drop_folder}/{filename}`

Supported default naming pattern:

- `{file_type}_YYYYMM_YYYYMM.(txt|csv|gz)`
- file types: `medical`, `pharmacy`, `members`, `enrollment`

Grouping closes using:

1. **Marker-based**: `_SUCCESS` closes the folder immediately.
2. **Quiescence-based**: `now - last_changed_at >= INTAKE_QUIESCENCE_MINUTES` (default `10`).

Closed groups are moved to immutable raw storage and registered in metadata DB.

## Parse engine (DuckDB)

P2 uses a DuckDB-first parser engine (swappable later):

- Loads layout contracts from `layout_registry` (`schema_json`, `parser_config_json`)
- Applies coercion-safe typing casts
- Counts invalid casts and row rejects without failing the whole submission
- Computes `atomic_month` from `anchor_date_column` for partitioned silver writes
- Writes parse metrics to metadata tables (`parse_run`, `parse_file_metrics`, `parse_column_metrics`)

For environments where DuckDB `httpfs` is not available, parsing falls back to local staging reads while preserving the same output contract.

## Object storage configuration (S3-compatible only)

All runtime object operations use a unified S3-compatible API:

- `S3_ENDPOINT_URL` (MinIO endpoint in dev, blank for AWS S3)
- `S3_ACCESS_KEY_ID`
- `S3_SECRET_ACCESS_KEY`
- `S3_REGION`
- `S3_BUCKET`

There is no local filesystem object-store runtime mode.

## Running locally with Docker

```bash
docker compose up --build
```

## Running in dev mode (no Docker)

```bash
source ./dev_bootstrap.sh
```

`dev_bootstrap.sh` starts standalone MinIO (if needed), creates the bucket, exports canonical S3_* vars, and runs migrations.

To start Dagster after bootstrap:

```bash
./dev_up.sh
```

## Triggering parse in Dagster

- `ready_for_parse_sensor` scans for `submission.status = READY_FOR_PARSE` and emits deduplicated run keys.
- `parse_submission_job` parses one submission run (or can be manually launched with `ops.parse_submission_op.config.submission_id`).

## Inspecting silver Parquet with DuckDB CLI

```bash
duckdb -c "SELECT atomic_month, count(*) FROM read_parquet('s3://$S3_BUCKET/silver/acme/medical/<submission_id>/atomic_month=*/**/*.parquet') GROUP BY 1"
```

## Tests

```bash
cd services/dagster
pytest -q
```

E2E targets:

```bash
make test-e2e-dev
make test-e2e-docker
```

## Add a new filename convention

1. Implement a class deriving from `FilenameConvention` in intake modules.
2. Implement:
   - `name`
   - `match(filename)`
   - `parse(filename) -> ParsedFilename`
3. Register it in a `ConventionRegistry`.
