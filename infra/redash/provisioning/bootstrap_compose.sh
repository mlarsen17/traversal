#!/bin/sh
set -eu

REDASH_URL="${REDASH_URL:-http://redash_server:5000}"
REDASH_ADMIN_EMAIL="${REDASH_ADMIN_EMAIL:-admin@local.dev}"
REDASH_ADMIN_NAME="${REDASH_ADMIN_NAME:-Local Admin}"
REDASH_ADMIN_PASSWORD="${REDASH_ADMIN_PASSWORD:-admin}"
REDASH_PLATFORM_DS_NAME="${REDASH_PLATFORM_DS_NAME:-Platform Metadata Postgres}"

wait_for_redash() {
  echo "waiting for Redash API at ${REDASH_URL}"
  i=0
  until curl -fsS "${REDASH_URL}/" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then
      echo "redash did not become ready in time" >&2
      return 1
    fi
    sleep 2
  done
}

user_exists() {
  python - <<'PY'
from redash import create_app, models
import os

app = create_app()
with app.app_context():
    email = os.environ["REDASH_ADMIN_EMAIL"]
    user = models.User.query.filter_by(email=email).first()
    print("1" if user else "0")
PY
}

fetch_api_key() {
  python - <<'PY'
from redash import create_app, models
import os

app = create_app()
with app.app_context():
    email = os.environ["REDASH_ADMIN_EMAIL"]
    user = models.User.query.filter_by(email=email).first()
    if not user:
        raise SystemExit("missing admin user for API key lookup")
    print(user.api_key)
PY
}

datasource_exists() {
  python - <<'PY'
from redash import create_app, models
import os

app = create_app()
with app.app_context():
    name = os.environ["REDASH_PLATFORM_DS_NAME"]
    ds = models.DataSource.query.filter_by(name=name).first()
    print("1" if ds else "0")
PY
}

build_ds_options_json() {
  python - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "dbname": os.environ["METADATA_PG_DB"],
            "host": "metadata_postgres",
            "port": 5432,
            "user": "redash_reader",
            "password": os.environ["REDASH_READER_PASSWORD"],
        }
    )
)
PY
}

wait_for_redash

if [ "$(user_exists)" = "0" ]; then
  echo "creating redash admin user ${REDASH_ADMIN_EMAIL}"
  /app/manage.py users create_root \
    "${REDASH_ADMIN_EMAIL}" \
    "${REDASH_ADMIN_NAME}" \
    --password "${REDASH_ADMIN_PASSWORD}" \
    --org default
else
  echo "redash admin user already exists: ${REDASH_ADMIN_EMAIL}"
fi

DS_OPTIONS=$(build_ds_options_json)
if [ "$(datasource_exists)" = "0" ]; then
  echo "creating datasource: ${REDASH_PLATFORM_DS_NAME}"
  /app/manage.py ds new \
    "${REDASH_PLATFORM_DS_NAME}" \
    --type pg \
    --options "${DS_OPTIONS}" \
    --org default
else
  echo "updating datasource: ${REDASH_PLATFORM_DS_NAME}"
  /app/manage.py ds edit \
    "${REDASH_PLATFORM_DS_NAME}" \
    --options "${DS_OPTIONS}" \
    --type pg \
    --org default
fi

REDASH_API_KEY="$(fetch_api_key)"
export REDASH_API_KEY
export REDASH_DATA_SOURCE_NAME="${REDASH_PLATFORM_DS_NAME}"

echo "running redash content bootstrap"
python /opt/redash/bootstrap/bootstrap_redash.py

echo "redash compose bootstrap complete"
