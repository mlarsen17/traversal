#!/bin/sh
set -eu

export PGPASSWORD="${METADATA_PG_PASSWORD}"

psql \
  -h "${METADATA_PG_HOST}" \
  -p "${METADATA_PG_PORT}" \
  -U "${METADATA_PG_USER}" \
  -d "${METADATA_PG_DB}" \
  -v ON_ERROR_STOP=1 \
  -v redash_reader_password="${REDASH_READER_PASSWORD}" \
  -v metadata_db="${METADATA_PG_DB}" <<'SQL'
SELECT format('CREATE ROLE redash_reader LOGIN PASSWORD %L', :'redash_reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'redash_reader')
\gexec

SELECT format('ALTER ROLE redash_reader WITH LOGIN PASSWORD %L', :'redash_reader_password')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'redash_reader')
\gexec

GRANT CONNECT ON DATABASE :"metadata_db" TO redash_reader;
GRANT USAGE ON SCHEMA serving TO redash_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO redash_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO redash_reader;
SQL
