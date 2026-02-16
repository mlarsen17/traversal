# Health Data Ingestion Platform - Phase 1.1 Hardening

This repository includes Dagster orchestration, Postgres metadata tracking, MinIO-backed object storage, Alembic migrations, and integration tests for intake registration.

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

Closed groups are moved to immutable raw storage:

- `raw/{submitter_id}/{file_type}/{submission_id}/data/{original_filename}`
- `raw/{submitter_id}/{file_type}/{submission_id}/manifest.generated.json`

Unknown classification is marked `NEEDS_REVIEW`.

## Object storage configuration (MinIO-only in dev/test)

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
./dev_up.sh
```

`dev_up.sh` starts MinIO, creates the configured bucket, runs migrations, and starts Dagster with the same S3 code path used by docker-compose.

## Tests

```bash
pytest -q
```

## Add a new filename convention

1. Implement a class deriving from `FilenameConvention` in intake modules.
2. Implement:
   - `name`
   - `match(filename)`
   - `parse(filename) -> ParsedFilename`
3. Register it in a `ConventionRegistry`.
