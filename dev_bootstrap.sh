#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
DAGSTER_HOME_DIR="${ROOT_DIR}/.dagster_home"
LOCAL_DB_DIR="${ROOT_DIR}/.local"
MINIO_DATA_DIR="${LOCAL_DB_DIR}/minio-data"
MINIO_BIN_DIR="${LOCAL_DB_DIR}/bin"
MINIO_LOG_FILE="${LOCAL_DB_DIR}/minio.log"
MINIO_PID_FILE="${LOCAL_DB_DIR}/minio.pid"

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -e "${ROOT_DIR}/services/dagster"
python -m pip install alembic==1.14.1

mkdir -p "${DAGSTER_HOME_DIR}" "${LOCAL_DB_DIR}" "${MINIO_DATA_DIR}" "${MINIO_BIN_DIR}"
cp "${ROOT_DIR}/services/dagster/dagster.dev.yaml" "${DAGSTER_HOME_DIR}/dagster.yaml"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://${MINIO_ENDPOINT}}"
MINIO_CONSOLE_ADDRESS="${MINIO_CONSOLE_ADDRESS:-127.0.0.1:9001}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-${S3_ACCESS_KEY_ID:-minioadmin}}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-${S3_SECRET_ACCESS_KEY:-minioadmin}}"
S3_BUCKET="${S3_BUCKET:-${RAW_BUCKET_NAME:-health-raw}}"

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

start_minio() {
  if curl --silent --fail "http://${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "MinIO already healthy at ${MINIO_ENDPOINT}"
    return
  fi

  if [[ -f "${MINIO_PID_FILE}" ]] && kill -0 "$(cat "${MINIO_PID_FILE}")" >/dev/null 2>&1; then
    echo "MinIO pidfile exists but endpoint is unhealthy; restarting MinIO"
    kill "$(cat "${MINIO_PID_FILE}")" >/dev/null 2>&1 || true
    rm -f "${MINIO_PID_FILE}"
  fi

  if [[ ! -f "${MINIO_PID_FILE}" ]]; then
    echo "Starting standalone MinIO at ${MINIO_ENDPOINT}"
    nohup env MINIO_ROOT_USER="${MINIO_ROOT_USER}" MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
      "${MINIO_BIN}" server "${MINIO_DATA_DIR}" --address "${MINIO_ENDPOINT}" --console-address "${MINIO_CONSOLE_ADDRESS}" \
      >"${MINIO_LOG_FILE}" 2>&1 </dev/null &
    echo $! > "${MINIO_PID_FILE}"
  fi

  for attempt in {1..30}; do
    if curl --silent --fail "http://${MINIO_ENDPOINT}/minio/health/live" >/dev/null; then
      echo "MinIO healthy"
      return
    fi
    if [[ "${attempt}" -eq 30 ]]; then
      echo "MinIO failed to become healthy. See ${MINIO_LOG_FILE}"
      exit 1
    fi
    sleep 1
  done
}

start_minio

export DAGSTER_HOME="${DAGSTER_HOME_DIR}"
export METADATA_DB_URL="${METADATA_DB_URL:-sqlite:///${LOCAL_DB_DIR}/metadata.db}"
export S3_ENDPOINT_URL="${S3_ENDPOINT_URL}"
export S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-${MINIO_ROOT_USER}}"
export S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD}}"
export S3_REGION="${S3_REGION:-us-east-1}"
export S3_BUCKET="${S3_BUCKET}"

python - <<'PY'
import boto3
import os

client = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    region_name=os.environ["S3_REGION"],
    verify=False,
)

bucket_name = os.environ["S3_BUCKET"]
existing = {entry["Name"] for entry in client.list_buckets().get("Buckets", [])}
if bucket_name not in existing:
    client.create_bucket(Bucket=bucket_name)
PY

(
  cd "${ROOT_DIR}/services/migrations"
  alembic upgrade head
)

echo "Bootstrap complete. METADATA_DB_URL=${METADATA_DB_URL} S3_BUCKET=${S3_BUCKET}"
