#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$ROOT_DIR"

if ! docker compose ps >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 2
fi

echo "[1/3] checking serving views are queryable by redash_reader"
docker compose run --rm --no-deps metadata_grants sh -lc '
  PGPASSWORD="$REDASH_READER_PASSWORD" psql \
    -h metadata_postgres \
    -U redash_reader \
    -d "$METADATA_PG_DB" \
    -v ON_ERROR_STOP=1 \
    -c "SELECT 1 FROM serving.submission_overview LIMIT 1;" \
    -c "SELECT 1 FROM serving.validation_findings LIMIT 1;" \
    -c "SELECT 1 FROM serving.submitter_month_status LIMIT 1;" \
    -c "SELECT 1 FROM serving.canonical_month_status LIMIT 1;" \
    -c "SELECT 1 FROM serving.dashboard_kpis_daily LIMIT 1;" \
    -c "SELECT 1 FROM serving.parse_column_health LIMIT 1;" \
    -c "SELECT 1 FROM serving.validation_summary LIMIT 1;" \
    -c "SELECT 1 FROM serving.submission_timeline LIMIT 1;"
'

echo "[2/3] checking base table reads are blocked for redash_reader"
set +e
docker compose run --rm --no-deps metadata_grants sh -lc '
  PGPASSWORD="$REDASH_READER_PASSWORD" psql \
    -h metadata_postgres \
    -U redash_reader \
    -d "$METADATA_PG_DB" \
    -v ON_ERROR_STOP=1 \
    -c "SELECT 1 FROM submission LIMIT 1;"
' >/tmp/redash_reader_base_table_test.log 2>&1
status=$?
set -e
if [ "$status" -eq 0 ]; then
  echo "expected base table denial, but query succeeded" >&2
  cat /tmp/redash_reader_base_table_test.log >&2
  exit 1
fi
echo "base table read denied as expected"


echo "[3/3] checking Redash runtime services are up"
for svc in redash_server redash_worker redash_scheduler redash_redis redash_postgres; do
  if ! docker compose ps --services --filter status=running | grep -qx "$svc"; then
    echo "service not running: $svc" >&2
    exit 1
  fi
done

echo "serving connectivity smoke checks passed"
