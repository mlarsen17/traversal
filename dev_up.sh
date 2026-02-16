#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
DAGSTER_HOME_DIR="${ROOT_DIR}/.dagster_home"
LOCAL_DB_DIR="${ROOT_DIR}/.local"
LOCAL_OBJECT_STORE_DIR="${ROOT_DIR}/.local_object_store"

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip
python -m pip install -e "${ROOT_DIR}/services/dagster"
python -m pip install alembic==1.14.1

mkdir -p "${DAGSTER_HOME_DIR}" "${LOCAL_DB_DIR}" "${LOCAL_OBJECT_STORE_DIR}"
cp "${ROOT_DIR}/services/dagster/dagster.dev.yaml" "${DAGSTER_HOME_DIR}/dagster.yaml"

export DAGSTER_HOME="${DAGSTER_HOME_DIR}"
export METADATA_DB_URL="sqlite:///${LOCAL_DB_DIR}/metadata.db"
export OBJECT_STORE_MODE="local"
export LOCAL_OBJECT_STORE_DIR="${LOCAL_OBJECT_STORE_DIR}"
export RAW_BUCKET_NAME="health-raw"

(
  cd "${ROOT_DIR}/services/migrations"
  alembic upgrade head
)

dagster dev -w "${ROOT_DIR}/services/dagster/workspace.yaml" -h 0.0.0.0 -p 3000
