# Health Data Ingestion Platform - Phase 1 Intake

This repository now includes Phase 0 foundation plus **Phase 1 intake registration**:

- Dagster orchestration + sensors for inbox discovery/grouping
- Metadata DB (Postgres in Docker, SQLite in dev mode)
- Object store support via S3-compatible API (MinIO in local/dev)
- Canonical internal manifest generation (`manifest.generated.json`)
- Layout registry sync asset seeded from Synthea CSV data dictionary examples

## Intake behavior (no client manifest required)

Submitters drop files under:

- `inbox/{submitter_id}/{filename}`
- optional: `inbox/{submitter_id}/{drop_folder}/{filename}`

Accepted default naming pattern:

- `{file_type}_YYYYMM_YYYYMM.(txt|csv|gz)` where `file_type` is one of:
  - `medical`
  - `pharmacy`
  - `members`
  - `enrollment`

Grouping closes using:

1. **Marker-based**: `_SUCCESS` in a folder (preferred)
2. **Quiescence-based**: no file change for N minutes (default `10`, configurable via `INTAKE_QUIESCENCE_MINUTES`)

Closed groups are registered as a submission and moved immutably to:

- `raw/{submitter_id}/{file_type}/{submission_id}/data/{original_filename}`
- `raw/{submitter_id}/{file_type}/{submission_id}/manifest.generated.json`

Unknown classification routes submission status to `NEEDS_REVIEW`.

## Running locally with Docker

```bash
docker compose up --build
```

## Running in dev mode (no Docker)

```bash
./dev_up.sh
```

## Tests

Run all tests:

```bash
pytest -q
```

## Add a new filename convention

1. Create a class implementing `FilenameConvention` in `services/dagster/src/health_platform/intake/filename_conventions.py` (or another intake module).
2. Implement:
   - `name`
   - `match(filename)`
   - `parse(filename) -> ParsedFilename`
3. Register it in a `ConventionRegistry` (e.g., augment `default_registry()` or inject one in tests/services).

This design keeps filename parsing pluggable without changing core grouping/move/register logic.
