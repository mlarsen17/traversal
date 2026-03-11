#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
OUT_DIR="$ROOT_DIR/documentation/postgres-serving-redash/baselines"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT_FILE="$OUT_DIR/serving_explain_${STAMP}.txt"

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

cat "$ROOT_DIR/infra/tests/sql/serving_explain_baseline.sql" \
  | docker compose run --rm --no-deps metadata_grants sh -lc '
      PGPASSWORD="$METADATA_PG_PASSWORD" psql \
        -h metadata_postgres \
        -U "$METADATA_PG_USER" \
        -d "$METADATA_PG_DB" \
        -v ON_ERROR_STOP=1
    ' >"$OUT_FILE"

echo "wrote baseline to $OUT_FILE"
