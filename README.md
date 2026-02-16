# Health Data Ingestion Platform - Phase 0 Foundation

A production-oriented local foundation for a health data ingestion platform, using **Dagster** for orchestration, **Postgres** for Dagster + metadata storage, and **MinIO** for S3-compatible object storage.

## What this includes

- Containerized runtime with Docker Compose
- Dagster webserver + daemon
- Dagster instance storage configured to **Postgres** (run/event/schedule storage)
- Separate metadata Postgres database for platform tables
- Alembic migrations for metadata DB
- MinIO + one-shot bucket initialization
- A `bootstrap_heartbeat_asset` Dagster asset that:
  - inserts `phase0_ok` into `bootstrap_heartbeat`
  - uploads `bootstrap/hello.txt` into `health-raw`
- A **non-Docker dev mode** for Codex/runtime environments that do not have Docker:
  - Dagster local filesystem storage
  - SQLite metadata DB
  - local folder-backed object store

## Architecture

This setup uses **two Postgres containers** for clarity:

- `dagster_postgres`: Dagster internal storage
- `metadata_postgres`: platform metadata tables managed by Alembic

## Prerequisites

- Docker Engine + Docker Compose plugin (for containerized mode)
- Python 3.11 (for non-Docker dev mode)

## Docker mode (local/prod-like)

1. Copy env file:

   ```bash
   cp .env.example .env
   ```

2. Start all services:

   ```bash
   docker compose up --build
   ```

3. Open Dagster UI:

   - http://localhost:3000

## Non-Docker dev mode (Codex-friendly)

Run:

```bash
./dev_up.sh
```

What `dev_up.sh` does:

1. Creates `.venv`
2. Installs Dagster project dependencies from `services/dagster/pyproject.toml`
3. Sets dev-mode env vars (`METADATA_DB_URL=sqlite:///...`, `OBJECT_STORE_MODE=local`)
4. Runs Alembic migrations against SQLite
5. Starts `dagster dev` on port `3000`

Dev-mode local state paths:

- Dagster home/config: `.dagster_home/`
- SQLite metadata DB: `.local/metadata.db`
- Object store files: `.local_object_store/health-raw/bootstrap/hello.txt`

## Running the hello asset

### From Dagster UI

1. Open **Assets**.
2. Select `bootstrap_heartbeat_asset`.
3. Click **Materialize**.

### From CLI in Docker mode

```bash
docker compose exec dagster_webserver dagster asset materialize --select bootstrap_heartbeat_asset -w /opt/dagster/app/workspace.yaml
```

### From CLI in non-Docker dev mode

```bash
source .venv/bin/activate
export DAGSTER_HOME=.dagster_home
dagster asset materialize --select bootstrap_heartbeat_asset -w services/dagster/workspace.yaml
```

## Verify outputs

### Verify metadata DB row

Docker mode:

```bash
docker compose exec metadata_postgres psql -U "$METADATA_PG_USER" -d "$METADATA_PG_DB" -c "SELECT id, created_at, message FROM bootstrap_heartbeat ORDER BY id DESC LIMIT 5;"
```

Dev mode (SQLite):

```bash
python - <<'PY'
import sqlite3
conn = sqlite3.connect('.local/metadata.db')
rows = conn.execute('SELECT id, created_at, message FROM bootstrap_heartbeat ORDER BY id DESC LIMIT 5').fetchall()
print(rows)
conn.close()
PY
```

### Verify object exists

Docker mode (MinIO console):

- http://localhost:9001
- Login using `.env` credentials (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)
- Navigate to bucket `health-raw`
- Confirm object `bootstrap/hello.txt`

Dev mode:

```bash
cat .local_object_store/health-raw/bootstrap/hello.txt
```

## Service list (docker-compose)

- `dagster_webserver` (port 3000)
- `dagster_daemon`
- `dagster_postgres` (port 5432)
- `metadata_postgres`
- `minio` (API 9000, Console 9001)
- `minio_init` (one-shot bucket setup)
- `migrations` (one-shot Alembic migration)

## Troubleshooting

- **Dagster UI is up but no assets visible**
  - Check `dagster_webserver` logs:
    ```bash
    docker compose logs -f dagster_webserver
    ```
- **Migration container failed (docker mode)**
  - Re-run migrations:
    ```bash
    docker compose run --rm migrations alembic upgrade head
    ```
- **Dev-mode migration failed**
  - Ensure `METADATA_DB_URL` points to a writable SQLite path.
- **MinIO bucket missing (docker mode)**
  - Re-run bucket init:
    ```bash
    docker compose run --rm minio_init
    ```
- **Port conflicts**
  - Ensure local ports `3000`, `5432`, `9000`, and `9001` are available.

## Optional Makefile helpers

```bash
make up
make down
make logs
make dagster-cli CMD="asset materialize --select bootstrap_heartbeat_asset -w /opt/dagster/app/workspace.yaml"
make psql
make minio-shell
```
