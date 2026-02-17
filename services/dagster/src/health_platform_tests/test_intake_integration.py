from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from dagster import build_asset_context, build_sensor_context
from moto.server import ThreadedMotoServer
from sqlalchemy import create_engine, text

from health_platform.assets.layout_assets import LAYOUT_ROOT, sync_layout_registry
from health_platform.intake.filename_conventions import (
    FilenameConvention,
    ParsedFilename,
    default_registry,
)
from health_platform.intake.jobs import discover_inbox_objects_job, register_submission_job
from health_platform.intake.object_store import S3ObjectStore
from health_platform.intake.processing import discover_inbox_objects
from health_platform.intake.sensors import inbox_discovery_sensor, inbox_grouping_sensor
from health_platform.parse.jobs import parse_submission_job


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_path = tmp_path / "metadata.db"
    engine = create_engine(f"sqlite+pysqlite:///{db_path}", future=True)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE bootstrap_heartbeat (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT, message TEXT)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE layout_registry (
                layout_id TEXT PRIMARY KEY,
                file_type TEXT NOT NULL,
                layout_version TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                parser_config_json TEXT NOT NULL,
                effective_start_date DATE,
                effective_end_date DATE,
                status TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX ux_layout_registry_file_type_version ON layout_registry(file_type, layout_version)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE submission (
                submission_id TEXT PRIMARY KEY,
                submitter_id TEXT NOT NULL,
                state TEXT,
                file_type TEXT NOT NULL,
                layout_id TEXT,
                coverage_start_month TEXT,
                coverage_end_month TEXT,
                received_at TEXT NOT NULL,
                status TEXT NOT NULL,
                grouping_method TEXT NOT NULL,
                inbox_prefix TEXT NOT NULL,
                raw_prefix TEXT NOT NULL,
                manifest_object_key TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL,
                group_fingerprint TEXT
            )
            """
        )
        conn.exec_driver_sql(
            "CREATE INDEX ix_submission_submitter_type_received ON submission(submitter_id, file_type, received_at)"
        )
        conn.exec_driver_sql("CREATE INDEX ix_submission_status ON submission(status)")
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX ux_submission_group_fingerprint ON submission(group_fingerprint)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE submission_file (
                submission_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id TEXT NOT NULL,
                object_key TEXT NOT NULL,
                bytes BIGINT NOT NULL,
                etag TEXT,
                sha256 TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE inbox_object (
                object_key TEXT PRIMARY KEY,
                submitter_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_changed_at TEXT,
                last_modified_at TEXT,
                bytes BIGINT,
                etag TEXT,
                status TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE parse_run (
                parse_run_id TEXT PRIMARY KEY,
                submission_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                engine TEXT NOT NULL,
                silver_prefix TEXT NOT NULL,
                report_object_key TEXT,
                error_message TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE parse_file_metrics (
                parse_file_metrics_id INTEGER PRIMARY KEY AUTOINCREMENT,
                parse_run_id TEXT NOT NULL,
                raw_object_key TEXT NOT NULL,
                rows_read BIGINT NOT NULL,
                rows_written BIGINT NOT NULL,
                rows_rejected BIGINT NOT NULL,
                bytes_read BIGINT,
                etag TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE parse_column_metrics (
                parse_column_metrics_id INTEGER PRIMARY KEY AUTOINCREMENT,
                parse_run_id TEXT NOT NULL,
                column_name TEXT NOT NULL,
                null_count BIGINT NOT NULL,
                invalid_count BIGINT NOT NULL
            )
            """
        )

    server = ThreadedMotoServer(port=0)
    server.start()
    _host, port = server.get_host_and_port()
    endpoint = f"http://127.0.0.1:{port}"

    bucket = f"health-raw-{uuid.uuid4().hex[:8]}"
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client.create_bucket(Bucket=bucket)

    monkeypatch.setenv("S3_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("S3_REGION", "us-east-1")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("S3_BUCKET", bucket)
    try:
        yield engine, S3ObjectStore(client, bucket)
    finally:
        server.stop()


def _run_discovery_sensor(engine, store):
    ctx = build_sensor_context(resources={"metadata_db": engine, "object_store": store})
    return inbox_discovery_sensor(ctx)


def _run_grouping_sensor(engine, store):
    ctx = build_sensor_context(resources={"metadata_db": engine, "object_store": store})
    return list(inbox_grouping_sensor(ctx))


def _execute_discovery(engine, store):
    result = discover_inbox_objects_job.execute_in_process(
        resources={"metadata_db": engine, "object_store": store}
    )
    assert result.success


def _execute_register(run_request, engine, store):
    result = register_submission_job.execute_in_process(
        run_config=run_request.run_config,
        resources={"metadata_db": engine, "object_store": store},
    )
    assert result.success


def _execute_parse(run_config, engine, store):
    result = parse_submission_job.execute_in_process(
        run_config=run_config,
        resources={"metadata_db": engine, "object_store": store},
        raise_on_error=False,
    )
    return result


def test_marker_ingestion_happy_path(env):
    engine, store = env
    store.put_bytes("inbox/acme/drop1/medical_202501_202503.txt", b"ok")
    store.put_bytes("inbox/acme/drop1/_SUCCESS", b"")

    discovery_request = _run_discovery_sensor(engine, store)
    assert discovery_request is not None
    _execute_discovery(engine, store)

    requests = _run_grouping_sensor(engine, store)
    assert len(requests) == 1
    _execute_register(requests[0], engine, store)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT submission_id, file_type, coverage_start_month, coverage_end_month, status, manifest_object_key
                FROM submission
                """
            )
        ).one()

    assert row.file_type == "medical"
    assert row.coverage_start_month == "202501"
    assert row.coverage_end_month == "202503"
    assert row.status == "READY_FOR_PARSE"
    assert (
        store.stat_object(f"raw/acme/medical/{row.submission_id}/data/medical_202501_202503.txt")
        is not None
    )
    assert store.stat_object(row.manifest_object_key) is not None
    assert store.stat_object("inbox/acme/drop1/medical_202501_202503.txt") is None
    assert store.stat_object("inbox/acme/drop1/_SUCCESS") is None


def test_quiescence_grouping_uses_last_changed_at(env):
    engine, store = env
    store.put_bytes("inbox/acme/pharmacy_202501_202501.csv", b"x")

    discover_inbox_objects(engine, store)
    assert _run_grouping_sensor(engine, store) == []

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE inbox_object
                SET last_seen_at = :seen,
                    last_changed_at = :changed
                """
            ),
            {
                "seen": datetime.now(timezone.utc),
                "changed": datetime.now(timezone.utc) - timedelta(minutes=11),
            },
        )

    requests = _run_grouping_sensor(engine, store)
    assert len(requests) == 1


def test_extensible_filename_conventions():
    class DummyConvention(FilenameConvention):
        name = "dummy"

        def match(self, filename: str) -> bool:
            return bool(re.match(r"^med_claims-\d{6}-\d{6}\.dat$", filename))

        def parse(self, filename: str) -> ParsedFilename:
            matched = re.match(r"^med_claims-(\d{6})-(\d{6})\.dat$", filename)
            assert matched
            return ParsedFilename(
                file_type="medical",
                coverage_start_month=matched.group(1),
                coverage_end_month=matched.group(2),
                layout_version="v1",
                confidence=0.95,
            )

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
    requests = _run_grouping_sensor(engine, store)
    _execute_register(requests[0], engine, store)

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
        rows = conn.execute(
            text("SELECT file_type, layout_version FROM layout_registry")
        ).fetchall()

    assert {tuple(row) for row in rows} >= {
        ("members", "v1"),
        ("medical", "v1"),
        ("pharmacy", "v1"),
    }


def test_layout_files_are_valid_json():
    for layout_file in sorted(Path(LAYOUT_ROOT).glob("*/*.json")):
        payload = json.loads(layout_file.read_text())
        assert "schema" in payload
        assert "parser_config" in payload
        assert isinstance(payload["schema"].get("columns"), list)


def _seed_ready_medical_submission(
    engine, store, csv_payload: bytes, object_name: str = "medical_202501_202503.csv"
):
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    store.put_bytes(f"inbox/acme/drop1/{object_name}", csv_payload)
    store.put_bytes("inbox/acme/drop1/_SUCCESS", b"")
    _execute_discovery(engine, store)
    requests = _run_grouping_sensor(engine, store)
    _execute_register(requests[0], engine, store)

    with engine.begin() as conn:
        return conn.execute(text("SELECT submission_id FROM submission")).scalar_one()


def test_p1_to_p2_happy_path_medical(env):
    engine, store = env
    csv_payload = (
        b"Id,START,STOP,PATIENT,CODE\nenc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
    )
    submission_id = _seed_ready_medical_submission(engine, store, csv_payload)

    result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        status = conn.execute(
            text("SELECT status FROM submission WHERE submission_id=:submission_id"),
            {"submission_id": submission_id},
        ).scalar_one()
        parse_run_count = conn.execute(text("SELECT COUNT(*) FROM parse_run")).scalar_one()
        metric_count = conn.execute(text("SELECT COUNT(*) FROM parse_file_metrics")).scalar_one()

    assert status == "PARSED"
    assert parse_run_count == 1
    assert metric_count == 1

    objects = store.list_objects(f"silver/acme/medical/{submission_id}/")
    keys = {obj.key for obj in objects}
    parquet_keys = [key for key in keys if key.endswith(".parquet")]
    assert parquet_keys
    assert f"silver/acme/medical/{submission_id}/parse_report.json" in keys


def test_parse_type_coercion_rejects_are_counted(env):
    engine, store = env
    csv_payload = (
        b"Id,START,STOP,PATIENT,CODE,BASE_ENCOUNTER_COST\n"
        b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201,10.5\n"
        b"enc2,not-a-date,2025-01-01T01:00:00Z,p2,99202,not-a-number\n"
        b"enc3,still-bad,2025-01-01T01:00:00Z,p3,99203,abc\n"
    )
    submission_id = _seed_ready_medical_submission(engine, store, csv_payload)

    result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        rows_read, rows_written, rejected = conn.execute(
            text("SELECT rows_read, rows_written, rows_rejected FROM parse_file_metrics")
        ).one()
        start_null_count, invalid_start = conn.execute(
            text(
                "SELECT null_count, invalid_count FROM parse_column_metrics WHERE column_name='START'"
            )
        ).one()

    assert rows_written == rows_read - rejected
    assert rejected > 0
    assert invalid_start > 0
    assert start_null_count > 0


def test_parse_unknown_layout_fails_cleanly(env):
    engine, store = env
    submission_id = _seed_ready_medical_submission(
        engine,
        store,
        b"Id,START,STOP,PATIENT,CODE\nenc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n",
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE submission SET layout_id = 'missing-layout' WHERE submission_id = :submission_id"
            ),
            {"submission_id": submission_id},
        )

    result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert not result.success

    with engine.begin() as conn:
        status = conn.execute(
            text("SELECT status FROM submission WHERE submission_id = :submission_id"),
            {"submission_id": submission_id},
        ).scalar_one()
    assert status == "PARSE_FAILED"
