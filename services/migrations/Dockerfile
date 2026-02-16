FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY services/migrations/alembic.ini /app/alembic.ini
COPY services/migrations/alembic /app/alembic
COPY services/migrations/app /app/app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir sqlalchemy==2.0.37 alembic==1.14.1 psycopg2-binary==2.9.10
