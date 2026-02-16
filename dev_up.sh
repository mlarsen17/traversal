#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
DAGSTER_HOME_DIR="${ROOT_DIR}/.dagster_home"
LOCAL_DB_DIR="${ROOT_DIR}/.local"

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -e "${ROOT_DIR}/services/dagster"
python -m pip install alembic==1.14.1

mkdir -p "${DAGSTER_HOME_DIR}" "${LOCAL_DB_DIR}"
cp "${ROOT_DIR}/services/dagster/dagster.dev.yaml" "${DAGSTER_HOME_DIR}/dagster.yaml"

export DAGSTER_HOME="${DAGSTER_HOME_DIR}"
export METADATA_DB_URL="sqlite:///${LOCAL_DB_DIR}/metadata.db"
export RAW_BUCKET_NAME="health-raw"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
export MINIO_SECURE="${MINIO_SECURE:-false}"
export INBOX_PREFIX="inbox/"
export RAW_PREFIX="raw/"
export QUIESCENCE_MINUTES="15"
export GROUP_BY_DEPTH="3"
export CLASSIFY_CONFIDENCE_THRESHOLD="0.7"

(
  cd "${ROOT_DIR}/services/migrations"
  alembic upgrade head
)

dagster dev -w "${ROOT_DIR}/services/dagster/workspace.yaml" -h 0.0.0.0 -p 3000
