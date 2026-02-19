FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY services/dagster/pyproject.toml /opt/dagster/app/pyproject.toml
COPY services/dagster/README.md /opt/dagster/app/README.md
COPY services/dagster/src /opt/dagster/app/src
COPY services/dagster/dagster.yaml /opt/dagster/app/dagster.yaml
COPY services/dagster/workspace.yaml /opt/dagster/app/workspace.yaml

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir . \
    && pip install --no-cache-dir ruff \
    && ruff format --check src

RUN mkdir -p /opt/dagster/dagster_home \
    && cp /opt/dagster/app/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

ENV PYTHONPATH=/opt/dagster/app/src

EXPOSE 3000
