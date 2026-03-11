# Postgres Serving Layer + Redash: Overview

## Purpose

Add a `serving` schema in the metadata Postgres database to expose stable, dashboard-ready views for operations reporting. This creates a semantic layer on top of existing P0-P5 metadata tables without changing orchestration behavior.

## Why a `serving` schema

- Isolates analytics-facing contracts from internal pipeline tables.
- Reduces dashboard coupling to normalized operational schemas.
- Enables controlled evolution: internal tables can change while `serving` remains stable.
- Makes permissioning simple (grant BI read access to `serving` only).

## Why serving views before a custom UI

- Fastest route to usable ops visibility.
- SQL views are low-risk and reversible.
- Supports immediate BI tooling (Redash) with minimal engineering overhead.
- Keeps optionality open for future custom UI that can also consume the same views.

## Fit with current P0-P5 architecture

Current platform already includes:

- Dagster orchestration and sensors
- Metadata Postgres
- MinIO/S3 object storage
- Intake, parse, validation, submitter-gold, canonical-gold flows

The serving layer is additive:

- No changes to existing jobs/sensors needed to get first dashboards.
- Uses existing metadata events and state transitions as source-of-truth.
- Sits above runtime tables as read-only reporting models.

## Likely serving-layer data sources

- Intake / lifecycle: `submission`, `submission_file`, `layout_registry`
- Parse: `parse_run`, `parse_file_metrics`, `parse_column_metrics`
- Validation: `validation_run`, `validation_finding`, `validation_rule`, `validation_rule_set_rule`
- Submitter gold control plane: `submitter_month_pointer`, `submitter_gold_build`
- Canonical control plane: `canonical_month_build`, `canonical_rebuild_queue`, `canonical_schema`

## First dashboard use cases

- Operations Overview
  - Pipeline status distribution
  - Daily throughput and failure trends
  - Canonical backlog/failure visibility

- Submitter Health
  - Warning/failure concentration by submitter
  - Top failing validation rules
  - Parse column quality hotspots

- Month Control Plane
  - Winner status by `(state, file_type, submitter_id, atomic_month)`
  - Canonical latest status per month
  - Rebuild queue backlog and processing health

## Why Redash now

- SQL-native and lightweight for an early operations reporting layer.
- Supports parameterized queries, schedules, and alerting quickly.
- Lets team validate metrics semantics before investing in custom frontend.

## Design intent

Treat `serving.*` as a stable public analytics contract. Internal operational schemas remain implementation detail; dashboard queries should target serving views only.

