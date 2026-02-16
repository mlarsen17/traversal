from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import boto3
import pytest
from dagster import build_asset_context, build_sensor_context
from moto import mock_aws
from sqlalchemy import create_engine, text

from health_platform.assets.layout_assets import sync_layout_registry
from health_platform.intake.filename_conventions import FilenameConvention, ParsedFilename, default_registry
from health_platform.intake.jobs import register_submission_job
from health_platform.intake.object_store import S3ObjectStore
from health_platform.intake.processing import discover_inbox_objects
from health_platform.intake.sensors import inbox_grouping_sensor


@pytest.fixture()
def env(tmp_path):
    db_path = tmp_path / "metadata.db"
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE bootstrap_heartbeat (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT, message TEXT)")
        conn.exec_driver_sql("CREATE TABLE layout_registry (layout_id TEXT PRIMARY KEY, file_type TEXT NOT NULL, layout_version TEXT NOT NULL, schema_json TEXT NOT NULL, parser_config_json TEXT NOT NULL, effective_start_date DATE, effective_end_date DATE, status TEXT NOT NULL)")
        conn.exec_driver_sql("CREATE UNIQUE INDEX ux_layout_registry_file_type_version ON layout_registry(file_type, layout_version)")
        conn.exec_driver_sql("CREATE TABLE submission (submission_id TEXT PRIMARY KEY, submitter_id TEXT NOT NULL, state TEXT, file_type TEXT NOT NULL, layout_id TEXT, coverage_start_month TEXT, coverage_end_month TEXT, received_at TEXT NOT NULL, status TEXT NOT NULL, grouping_method TEXT NOT NULL, inbox_prefix TEXT NOT NULL, raw_prefix TEXT NOT NULL, manifest_object_key TEXT NOT NULL, manifest_sha256 TEXT NOT NULL)")
        conn.exec_driver_sql("CREATE INDEX ix_submission_submitter_type_received ON submission(submitter_id, file_type, received_at)")
        conn.exec_driver_sql("CREATE INDEX ix_submission_status ON submission(status)")
        conn.exec_driver_sql("CREATE TABLE submission_file (submission_file_id INTEGER PRIMARY KEY AUTOINCREMENT, submission_id TEXT NOT NULL, object_key TEXT NOT NULL, bytes BIGINT NOT NULL, etag TEXT, sha256 TEXT)")
        conn.exec_driver_sql("CREATE TABLE inbox_object (object_key TEXT PRIMARY KEY, submitter_id TEXT NOT NULL, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_modified_at TEXT, bytes BIGINT, etag TEXT, status TEXT NOT NULL)")

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="health-raw")
        yield engine, S3ObjectStore(client, "health-raw")


def _run_grouping(engine, store):
    ctx = build_sensor_context(resources={"metadata_db": engine, "object_store": store})
    return list(inbox_grouping_sensor(ctx))


def _execute_request(rr, engine, store):
    res = register_submission_job.execute_in_process(
        run_config=rr.run_config,
        resources={"metadata_db": engine, "object_store": store},
    )
    assert res.success


def test_marker_ingestion_happy_path(env):
    engine, store = env
    store.put_bytes("inbox/acme/medical_202501_202503.txt", b"ok")
    store.put_bytes("inbox/acme/_SUCCESS", b"")

    discover_inbox_objects(engine, store)
    reqs = _run_grouping(engine, store)
    assert len(reqs) == 1
    _execute_request(reqs[0], engine, store)

    with engine.begin() as conn:
        row = conn.execute(text("SELECT submission_id, file_type, coverage_start_month, coverage_end_month, status, manifest_object_key FROM submission")).one()
    assert row.file_type == "medical"
    assert row.coverage_start_month == "202501"
    assert row.coverage_end_month == "202503"
    assert row.status == "READY_FOR_PARSE"
    assert store.stat_object(f"raw/acme/medical/{row.submission_id}/data/medical_202501_202503.txt") is not None
    assert store.stat_object(row.manifest_object_key) is not None
    assert store.stat_object("inbox/acme/medical_202501_202503.txt") is None


def test_quiescence_grouping(env):
    engine, store = env
    store.put_bytes("inbox/acme/pharmacy_202501_202501.csv", b"x")

    discover_inbox_objects(engine, store)
    reqs = _run_grouping(engine, store)
    assert reqs == []

    with engine.begin() as conn:
        conn.execute(text("UPDATE inbox_object SET last_seen_at=:ts"), {"ts": datetime.now(timezone.utc) - timedelta(minutes=11)})

    reqs = _run_grouping(engine, store)
    assert len(reqs) == 1
    _execute_request(reqs[0], engine, store)


def test_extensible_filename_conventions():
    class DummyConvention(FilenameConvention):
        name = "dummy"

        def match(self, filename: str) -> bool:
            return bool(re.match(r"^med_claims-\d{6}-\d{6}\.dat$", filename))

        def parse(self, filename: str) -> ParsedFilename:
            m = re.match(r"^med_claims-(\d{6})-(\d{6})\.dat$", filename)
            assert m
            return ParsedFilename(file_type="medical", coverage_start_month=m.group(1), coverage_end_month=m.group(2), layout_version="v1", confidence=0.95)

    registry = default_registry()
    registry.register(DummyConvention())
    parsed = registry.parse("med_claims-202401-202402.dat")
    assert parsed.file_type == "medical"
    assert parsed.coverage_start_month == "202401"


def test_unknown_classification_needs_review(env):
    engine, store = env
    store.put_bytes("inbox/acme/mystery_file.bin", b"abc")
    store.put_bytes("inbox/acme/_SUCCESS", b"")
    discover_inbox_objects(engine, store)

    reqs = _run_grouping(engine, store)
    _execute_request(reqs[0], engine, store)

    with engine.begin() as conn:
        row = conn.execute(text("SELECT status, layout_id, file_type FROM submission")).one()
    assert row.status == "NEEDS_REVIEW"
    assert row.layout_id is None
    assert row.file_type == "unknown"


def test_layout_registry_sync(env):
    engine, _ = env
    result = sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    assert result.metadata["layouts_loaded"] >= 3
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT file_type, layout_version FROM layout_registry")).fetchall()
    assert {tuple(r) for r in rows} >= {("members", "v1"), ("medical", "v1"), ("pharmacy", "v1")}
