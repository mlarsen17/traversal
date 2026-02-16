#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
DAGSTER_HOME_DIR="${ROOT_DIR}/.dagster_home"
LOCAL_DB_DIR="${ROOT_DIR}/.local"
MINIO_DATA_DIR="${LOCAL_DB_DIR}/minio-data"
MINIO_BIN_DIR="${LOCAL_DB_DIR}/bin"

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -e "${ROOT_DIR}/services/dagster"
python -m pip install alembic==1.14.1

mkdir -p "${DAGSTER_HOME_DIR}" "${LOCAL_DB_DIR}" "${MINIO_DATA_DIR}" "${MINIO_BIN_DIR}"
cp "${ROOT_DIR}/services/dagster/dagster.dev.yaml" "${DAGSTER_HOME_DIR}/dagster.yaml"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
MINIO_CONSOLE_ADDRESS="${MINIO_CONSOLE_ADDRESS:-127.0.0.1:9001}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
RAW_BUCKET_NAME="${RAW_BUCKET_NAME:-health-raw}"

MINIO_BIN="${MINIO_BINARY:-}"
if [[ -z "${MINIO_BIN}" ]]; then
  if command -v minio >/dev/null 2>&1; then
    MINIO_BIN="$(command -v minio)"
  elif [[ -x "${MINIO_BIN_DIR}/minio" ]]; then
    MINIO_BIN="${MINIO_BIN_DIR}/minio"
  else
    MINIO_DOWNLOAD_URL="${MINIO_DOWNLOAD_URL:-https://github.com/minio/minio/releases/latest/download/minio}"
    MINIO_FALLBACK_DOWNLOAD_URL="${MINIO_FALLBACK_DOWNLOAD_URL:-https://dl.min.io/server/minio/release/linux-amd64/minio}"
    echo "MinIO binary not found. Downloading standalone binary to ${MINIO_BIN_DIR}/minio"
    if ! curl --fail --location --retry 3 --output "${MINIO_BIN_DIR}/minio" "${MINIO_DOWNLOAD_URL}"; then
      echo "Primary MinIO download URL failed (${MINIO_DOWNLOAD_URL}). Retrying with fallback URL."
      curl --fail --location --retry 3 --output "${MINIO_BIN_DIR}/minio" "${MINIO_FALLBACK_DOWNLOAD_URL}"
    fi
    chmod +x "${MINIO_BIN_DIR}/minio"
    MINIO_BIN="${MINIO_BIN_DIR}/minio"
  fi
fi

MINIO_LOG_FILE="${LOCAL_DB_DIR}/minio.log"
MINIO_PID_FILE="${LOCAL_DB_DIR}/minio.pid"

MINIO_ROOT_USER="${MINIO_ROOT_USER}" MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
  "${MINIO_BIN}" server "${MINIO_DATA_DIR}" --address "${MINIO_ENDPOINT}" --console-address "${MINIO_CONSOLE_ADDRESS}" \
  >"${MINIO_LOG_FILE}" 2>&1 &

echo $! > "${MINIO_PID_FILE}"
trap 'if [[ -f "${MINIO_PID_FILE}" ]]; then kill "$(cat "${MINIO_PID_FILE}")" >/dev/null 2>&1 || true; rm -f "${MINIO_PID_FILE}"; fi' EXIT

for attempt in {1..30}; do
  if curl --silent --fail "http://${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "MinIO failed to become healthy. See ${MINIO_LOG_FILE}"
    exit 1
  fi
  sleep 1
done

export DAGSTER_HOME="${DAGSTER_HOME_DIR}"
export METADATA_DB_URL="sqlite:///${LOCAL_DB_DIR}/metadata.db"
export OBJECT_STORE_MODE="s3"
export MINIO_ENDPOINT="${MINIO_ENDPOINT}"
export MINIO_ACCESS_KEY="${MINIO_ROOT_USER}"
export MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export MINIO_REGION="${MINIO_REGION:-us-east-1}"
export MINIO_SECURE="false"
export RAW_BUCKET_NAME="${RAW_BUCKET_NAME}"

python - <<'PY'
import boto3
import os

client = boto3.client(
    "s3",
    endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    region_name=os.environ["MINIO_REGION"],
)

bucket_name = os.environ["RAW_BUCKET_NAME"]
existing = {entry["Name"] for entry in client.list_buckets().get("Buckets", [])}
if bucket_name not in existing:
    client.create_bucket(Bucket=bucket_name)
PY

(
  cd "${ROOT_DIR}/services/migrations"
  alembic upgrade head
)

dagster dev -w "${ROOT_DIR}/services/dagster/workspace.yaml" -h 0.0.0.0 -p 3000
