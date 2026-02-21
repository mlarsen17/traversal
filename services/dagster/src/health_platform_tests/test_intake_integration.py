from __future__ import annotations

import json
import re
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import duckdb
import pytest
from dagster import build_asset_context, build_sensor_context
from sqlalchemy import create_engine, text

from health_platform.assets.layout_assets import LAYOUT_ROOT, sync_layout_registry
from health_platform.canonical.assets import MAPPING_ROOT, SCHEMA_ROOT, sync_canonical_registry
from health_platform.gold_canonical.jobs import build_canonical_month_job
from health_platform.gold_canonical.sensors import canonical_rebuild_sensor
from health_platform.gold_submitter.jobs import build_submitter_gold_job
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
from health_platform.validation.jobs import validate_submission_job
from health_platform.validation.sensors import ready_for_validate_sensor


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture()
def moto_s3_endpoint_url():
    port = _pick_free_port()
    cmd = [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    endpoint = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    ready = False
    while time.time() < deadline:
        try:
            client = boto3.client(
                "s3",
                region_name="us-east-1",
                endpoint_url=endpoint,
                aws_access_key_id="test",
                aws_secret_access_key="test",
            )
            client.list_buckets()
            ready = True
            break
        except Exception:
            if proc.poll() is not None:
                break
            time.sleep(0.25)

    if not ready:
        stdout, stderr = proc.communicate(timeout=5)
        raise RuntimeError(
            "moto_server failed to start for S3 integration tests. "
            f"stdout={stdout!r} stderr={stderr!r}"
        )

    try:
        yield endpoint
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@pytest.fixture()
def env(tmp_path, monkeypatch, moto_s3_endpoint_url):
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
                group_fingerprint TEXT,
                created_at TEXT,
                latest_validation_run_id TEXT
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
        conn.exec_driver_sql(
            """
            CREATE TABLE validation_rule (
                rule_id TEXT PRIMARY KEY,
                file_type TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                rule_kind TEXT NOT NULL,
                default_severity TEXT NOT NULL,
                default_threshold_type TEXT NOT NULL,
                default_threshold_value DOUBLE NOT NULL,
                definition_json TEXT NOT NULL,
                sql_template TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE validation_rule_set (
                rule_set_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                file_type TEXT NOT NULL,
                layout_version TEXT,
                effective_start DATE,
                effective_end DATE,
                status TEXT NOT NULL,
                created_by TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE validation_rule_set_rule (
                rule_set_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                enabled BOOLEAN NOT NULL,
                severity_override TEXT,
                threshold_type_override TEXT,
                threshold_value_override DOUBLE,
                params_override_json TEXT,
                PRIMARY KEY (rule_set_id, rule_id)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE validation_run (
                validation_run_id TEXT PRIMARY KEY,
                submission_id TEXT NOT NULL,
                rule_set_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                outcome TEXT,
                engine TEXT NOT NULL,
                engine_version TEXT,
                silver_prefix TEXT NOT NULL,
                total_rows BIGINT,
                report_object_key TEXT,
                error_message TEXT
            )
            """
        )
        conn.exec_driver_sql(
            "CREATE INDEX ix_validation_run_submission ON validation_run(submission_id)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE validation_finding (
                validation_finding_id INTEGER PRIMARY KEY AUTOINCREMENT,
                validation_run_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                scope_month TEXT,
                violations_count BIGINT NOT NULL,
                denominator_count BIGINT NOT NULL,
                violations_rate DOUBLE,
                sample_object_key TEXT,
                passed BOOLEAN NOT NULL,
                computed_at TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE submitter_month_pointer (
                state TEXT NOT NULL,
                file_type TEXT NOT NULL,
                submitter_id TEXT NOT NULL,
                atomic_month TEXT NOT NULL,
                winning_submission_id TEXT NOT NULL,
                replaced_submission_id TEXT,
                winning_validation_run_id TEXT,
                winner_timestamp TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                reason TEXT,
                UNIQUE (state, file_type, submitter_id, atomic_month)
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE submitter_gold_build (
                submitter_gold_build_id TEXT PRIMARY KEY,
                submission_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                months_affected_json TEXT NOT NULL,
                error_message TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE canonical_schema (
                canonical_schema_id TEXT PRIMARY KEY,
                file_type TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX ux_canonical_schema_file_type_ver ON canonical_schema(file_type, schema_version)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE canonical_mapping (
                mapping_id TEXT PRIMARY KEY,
                layout_id TEXT NOT NULL,
                canonical_schema_id TEXT NOT NULL,
                mapping_version TEXT NOT NULL,
                mapping_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX ux_canonical_mapping_layout_schema_ver ON canonical_mapping(layout_id, canonical_schema_id, mapping_version)"
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE canonical_month_build (
                canonical_month_build_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                file_type TEXT NOT NULL,
                atomic_month TEXT NOT NULL,
                canonical_schema_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                output_prefix TEXT NOT NULL,
                error_message TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE canonical_rebuild_queue (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                file_type TEXT NOT NULL,
                atomic_month TEXT NOT NULL,
                enqueued_at TEXT NOT NULL,
                reason TEXT,
                processed_at TEXT
            )
            """
        )

    endpoint = moto_s3_endpoint_url

    bucket = f"health-raw-{uuid.uuid4().hex[:8]}"
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    client.create_bucket(Bucket=bucket)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv("S3_URL_STYLE", "path")
    monkeypatch.setenv("S3_REGION", "us-east-1")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("S3_BUCKET", bucket)
    yield engine, S3ObjectStore(client, bucket)


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


def _run_validate_sensor(engine, store):
    ctx = build_sensor_context(resources={"metadata_db": engine, "object_store": store})
    return list(ready_for_validate_sensor(ctx))


def _execute_validate(run_config, engine, store):
    result = validate_submission_job.execute_in_process(
        run_config=run_config,
        resources={"metadata_db": engine, "object_store": store},
        raise_on_error=False,
    )
    return result


def _execute_submitter_gold(run_config, engine, store):
    result = build_submitter_gold_job.execute_in_process(
        run_config=run_config,
        resources={"metadata_db": engine, "object_store": store},
        raise_on_error=False,
    )
    return result


def _run_canonical_sensor(engine):
    ctx = build_sensor_context(resources={"metadata_db": engine})
    return list(canonical_rebuild_sensor(ctx))


def _execute_canonical(run_config, engine, store):
    result = build_canonical_month_job.execute_in_process(
        run_config=run_config,
        resources={"metadata_db": engine, "object_store": store},
        raise_on_error=False,
    )
    return result


def _seed_validation_ruleset(
    engine,
    *,
    file_type="medical",
    layout_version=None,
    rules=None,
    name=None,
):
    rule_set_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO validation_rule_set(
                    rule_set_id, name, file_type, layout_version, effective_start, effective_end, status, created_by, created_at
                ) VALUES (
                    :rule_set_id, :name, :file_type, :layout_version, NULL, NULL, 'ACTIVE', 'test', :created_at
                )
                """
            ),
            {
                "rule_set_id": rule_set_id,
                "name": name or f"test-{rule_set_id}",
                "file_type": file_type,
                "layout_version": layout_version,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        for idx, rule in enumerate(rules or []):
            rule_id = str(uuid.uuid4())
            conn.execute(
                text(
                    """
                    INSERT INTO validation_rule(
                        rule_id, file_type, name, description, rule_kind, default_severity,
                        default_threshold_type, default_threshold_value, definition_json, sql_template, created_at, updated_at
                    ) VALUES (
                        :rule_id, :file_type, :name, :description, :rule_kind, :default_severity,
                        :default_threshold_type, :default_threshold_value, :definition_json, :sql_template, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "rule_id": rule_id,
                    "file_type": file_type,
                    "name": rule.get("name", f"rule-{idx}"),
                    "description": rule.get("description"),
                    "rule_kind": rule["rule_kind"],
                    "default_severity": rule.get("severity", "HARD"),
                    "default_threshold_type": rule.get("threshold_type", "COUNT"),
                    "default_threshold_value": rule.get("threshold_value", 0),
                    "definition_json": json.dumps(rule.get("definition", {})),
                    "sql_template": rule.get("sql_template"),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO validation_rule_set_rule(
                        rule_set_id, rule_id, enabled, severity_override, threshold_type_override,
                        threshold_value_override, params_override_json
                    ) VALUES (
                        :rule_set_id, :rule_id, 1, NULL, NULL, NULL, NULL
                    )
                    """
                ),
                {"rule_set_id": rule_set_id, "rule_id": rule_id},
            )

    return rule_set_id


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
    engine,
    store,
    csv_payload: bytes,
    object_name: str = "medical_202501_202503.csv",
    drop_name: str = "drop1",
):
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    store.put_bytes(f"inbox/acme/{drop_name}/{object_name}", csv_payload)
    store.put_bytes(f"inbox/acme/{drop_name}/_SUCCESS", b"")
    _execute_discovery(engine, store)
    requests = _run_grouping_sensor(engine, store)
    _execute_register(requests[0], engine, store)

    with engine.begin() as conn:
        return conn.execute(
            text("SELECT submission_id FROM submission ORDER BY received_at DESC LIMIT 1")
        ).scalar_one()


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


def test_parse_rows_written_matches_non_rejected_output(env):
    engine, store = env
    csv_payload = (
        b"Id,START,STOP,PATIENT,CODE\n"
        b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
        b"enc2,not-a-date,2025-01-01T01:00:00Z,p2,99202\n"
        b"enc3,2025-01-03T00:00:00Z,2025-01-03T01:00:00Z,p3,99203\n"
    )
    submission_id = _seed_ready_medical_submission(engine, store, csv_payload)

    result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        rows_read, rows_written, rows_rejected, report_object_key = conn.execute(
            text(
                """
                SELECT pfm.rows_read, pfm.rows_written, pfm.rows_rejected, pr.report_object_key
                FROM parse_file_metrics pfm
                JOIN parse_run pr ON pr.parse_run_id = pfm.parse_run_id
                """
            )
        ).one()

    report = json.loads(store.get_bytes(report_object_key))
    output_rows = sum(report["output_partition_counts"].values())

    assert rows_read == 3
    assert rows_rejected == 1
    assert rows_written == 2
    assert rows_written == rows_read - rows_rejected
    assert output_rows == rows_written


def test_parse_column_metrics_persist_real_null_and_invalid_counts(env):
    engine, store = env
    csv_payload = (
        b"Id,START,STOP,PATIENT,CODE\n"
        b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
        b"enc2,2025-01-02T00:00:00Z,,p2,99202\n"
        b"enc3,2025-01-03T00:00:00Z,bad-date,p3,99203\n"
    )
    submission_id = _seed_ready_medical_submission(engine, store, csv_payload)

    result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        stop_null_count, stop_invalid_count = conn.execute(
            text(
                "SELECT null_count, invalid_count FROM parse_column_metrics WHERE column_name='STOP'"
            )
        ).one()

    assert stop_null_count > 0
    assert stop_invalid_count > 0


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


def _parse_medical_submission(
    engine,
    store,
    csv_payload: bytes,
    object_name: str = "medical_202501_202503.csv",
    drop_name: str = "drop1",
) -> str:
    submission_id = _seed_ready_medical_submission(
        engine, store, csv_payload, object_name=object_name, drop_name=drop_name
    )
    parse_result = _execute_parse(
        {"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert parse_result.success
    return submission_id


def test_validation_hard_fail_blocks_submission(env):
    engine, store = env
    submission_id = _parse_medical_submission(
        engine,
        store,
        (
            b"Id,START,STOP,PATIENT,CODE\n"
            b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
            b"enc2,2025-01-02T00:00:00Z,2025-01-02T01:00:00Z,,99202\n"
        ),
    )

    _seed_validation_ruleset(
        engine,
        layout_version="v1",
        rules=[
            {
                "name": "patient_required",
                "rule_kind": "NOT_NULL",
                "severity": "HARD",
                "threshold_type": "COUNT",
                "threshold_value": 0,
                "definition": {"column": "PATIENT"},
            }
        ],
    )

    result = _execute_validate(
        {"ops": {"validate_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        status, latest_validation_run_id = conn.execute(
            text(
                "SELECT status, latest_validation_run_id FROM submission WHERE submission_id=:submission_id"
            ),
            {"submission_id": submission_id},
        ).one()
        run = conn.execute(
            text(
                "SELECT status, outcome FROM validation_run WHERE validation_run_id=:validation_run_id"
            ),
            {"validation_run_id": latest_validation_run_id},
        ).one()
        finding_passed = conn.execute(
            text(
                "SELECT passed FROM validation_finding WHERE validation_run_id=:validation_run_id"
            ),
            {"validation_run_id": latest_validation_run_id},
        ).scalar_one()

    assert status == "VALIDATION_FAILED"
    assert run.status == "SUCCEEDED"
    assert run.outcome == "FAIL_HARD"
    assert finding_passed == 0


def test_validation_soft_fail_yields_warnings(env):
    engine, store = env
    submission_id = _parse_medical_submission(
        engine,
        store,
        (
            b"Id,START,STOP,PATIENT,CODE\n"
            b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,200000\n"
            b"enc2,2025-01-02T00:00:00Z,2025-01-02T01:00:00Z,p2,99202\n"
        ),
    )

    _seed_validation_ruleset(
        engine,
        layout_version="v1",
        rules=[
            {
                "name": "code_range_soft",
                "rule_kind": "RANGE",
                "severity": "SOFT",
                "threshold_type": "RATE",
                "threshold_value": 0.01,
                "definition": {"column": "CODE", "min": 0, "max": 100000},
            }
        ],
    )

    result = _execute_validate(
        {"ops": {"validate_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        status = conn.execute(
            text("SELECT status FROM submission WHERE submission_id=:submission_id"),
            {"submission_id": submission_id},
        ).scalar_one()
        outcome = conn.execute(
            text("SELECT outcome FROM validation_run WHERE submission_id=:submission_id"),
            {"submission_id": submission_id},
        ).scalar_one()

    assert status == "VALIDATED_WITH_WARNINGS"
    assert outcome == "PASS_WITH_WARNINGS"


def test_validation_sample_artifact_written(env):
    engine, store = env
    submission_id = _parse_medical_submission(
        engine,
        store,
        (b"Id,START,STOP,PATIENT,CODE\nenc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,,99201\n"),
    )
    _seed_validation_ruleset(
        engine,
        layout_version="v1",
        rules=[
            {
                "name": "patient_required",
                "rule_kind": "NOT_NULL",
                "severity": "HARD",
                "threshold_type": "COUNT",
                "threshold_value": 0,
                "definition": {"column": "PATIENT"},
            }
        ],
    )

    result = _execute_validate(
        {"ops": {"validate_submission_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        sample_key = conn.execute(
            text(
                """
                SELECT vf.sample_object_key
                FROM validation_finding vf
                JOIN validation_run vr ON vr.validation_run_id = vf.validation_run_id
                WHERE vr.submission_id=:submission_id
                """
            ),
            {"submission_id": submission_id},
        ).scalar_one()

    assert sample_key
    assert store.stat_object(sample_key) is not None
    assert len(store.get_bytes(sample_key)) > 0


def test_validation_rule_set_selection_prefers_layout_specific(env):
    engine, store = env
    submission_id = _parse_medical_submission(
        engine,
        store,
        b"Id,START,STOP,PATIENT,CODE\nenc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n",
    )

    _seed_validation_ruleset(
        engine,
        layout_version=None,
        name="default",
        rules=[
            {
                "name": "default_not_null",
                "rule_kind": "NOT_NULL",
                "severity": "HARD",
                "threshold_type": "COUNT",
                "threshold_value": 0,
                "definition": {"column": "PATIENT"},
            }
        ],
    )
    preferred_rule_set = _seed_validation_ruleset(
        engine,
        layout_version="v1",
        name="layout-v1",
        rules=[
            {
                "name": "layout_not_null",
                "rule_kind": "NOT_NULL",
                "severity": "HARD",
                "threshold_type": "COUNT",
                "threshold_value": 0,
                "definition": {"column": "PATIENT"},
            }
        ],
    )

    requests = _run_validate_sensor(engine, store)
    assert requests
    result = _execute_validate(requests[0].run_config, engine, store)
    assert result.success

    with engine.begin() as conn:
        used_rule_set = conn.execute(
            text("SELECT rule_set_id FROM validation_run WHERE submission_id=:submission_id"),
            {"submission_id": submission_id},
        ).scalar_one()

    assert used_rule_set == preferred_rule_set


def _mark_submission_validated(engine, submission_id: str, ended_at: str):
    validation_run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO validation_run(
                    validation_run_id, submission_id, rule_set_id, started_at, ended_at, status,
                    outcome, engine, engine_version, silver_prefix, total_rows, report_object_key, error_message
                ) VALUES (
                    :validation_run_id, :submission_id, :rule_set_id, :started_at, :ended_at, 'SUCCEEDED',
                    'PASS', 'duckdb', NULL, :silver_prefix, 0, NULL, NULL
                )
                """
            ),
            {
                "validation_run_id": validation_run_id,
                "submission_id": submission_id,
                "rule_set_id": "test-ruleset",
                "started_at": ended_at,
                "ended_at": ended_at,
                "silver_prefix": f"silver/acme/medical/{submission_id}",
            },
        )
        conn.execute(
            text(
                """
                UPDATE submission
                SET status='VALIDATED', latest_validation_run_id=:validation_run_id
                WHERE submission_id=:submission_id
                """
            ),
            {"validation_run_id": validation_run_id, "submission_id": submission_id},
        )


def _gold_month_rows_local(store, submitter_id: str, state: str, file_type: str, month: str) -> int:
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import duckdb

    key_prefix = f"gold_submitter/{state}/{file_type}/{submitter_id}/atomic_month={month}/"
    keys = [obj.key for obj in store.list_objects(key_prefix) if obj.key.endswith(".parquet")]
    assert keys
    with TemporaryDirectory() as tempdir:
        local_paths: list[str] = []
        for idx, key in enumerate(keys):
            path = Path(tempdir) / f"part_{idx}.parquet"
            path.write_bytes(store.get_bytes(key))
            local_paths.append(path.as_posix())
        con = duckdb.connect()
        try:
            path_list = ", ".join(f"'{p}'" for p in local_paths)
            return int(
                con.execute(f"SELECT COUNT(*) FROM read_parquet([{path_list}])").fetchone()[0]
            )
        finally:
            con.close()


def test_submitter_gold_initial_merge_creates_pointer_and_partitions(env):
    engine, store = env
    submission_id = _parse_medical_submission(
        engine,
        store,
        (
            b"Id,START,STOP,PATIENT,CODE\n"
            b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
            b"enc2,2025-02-01T00:00:00Z,2025-02-01T01:00:00Z,p2,99202\n"
        ),
        object_name="medical_202501_202502.csv",
    )
    _mark_submission_validated(engine, submission_id, "2025-02-20T00:00:00+00:00")

    result = _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT atomic_month, winning_submission_id
                FROM submitter_month_pointer
                ORDER BY atomic_month
                """
            )
        ).fetchall()

    assert [(row.atomic_month, row.winning_submission_id) for row in rows] == [
        ("2025-01", submission_id),
        ("2025-02", submission_id),
    ]
    assert _gold_month_rows_local(store, "acme", "__unknown__", "medical", "2025-01") == 1
    assert _gold_month_rows_local(store, "acme", "__unknown__", "medical", "2025-02") == 1


def test_submitter_gold_correction_updates_only_overlapping_month(env):
    engine, store = env
    s1 = _parse_medical_submission(
        engine,
        store,
        (
            b"Id,START,STOP,PATIENT,CODE\n"
            b"enc1,2025-01-01T00:00:00Z,2025-01-01T01:00:00Z,p1,99201\n"
            b"enc2,2025-02-01T00:00:00Z,2025-02-01T01:00:00Z,p2,99202\n"
        ),
        object_name="medical_202501_202502.csv",
    )
    _mark_submission_validated(engine, s1, "2025-02-20T00:00:00+00:00")
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s1}}}},
        engine,
        store,
    ).success

    s2 = _parse_medical_submission(
        engine,
        store,
        b"Id,START,STOP,PATIENT,CODE\nenc3,2025-02-05T00:00:00Z,2025-02-05T01:00:00Z,p3,99299\n",
        object_name="medical_202502_202502.csv",
    )
    _mark_submission_validated(engine, s2, "2025-02-21T00:00:00+00:00")
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s2}}}},
        engine,
        store,
    ).success

    with engine.begin() as conn:
        jan = conn.execute(
            text(
                """
                SELECT winning_submission_id, replaced_submission_id
                FROM submitter_month_pointer
                WHERE atomic_month='2025-01'
                """
            )
        ).one()
        feb = conn.execute(
            text(
                """
                SELECT winning_submission_id, replaced_submission_id
                FROM submitter_month_pointer
                WHERE atomic_month='2025-02'
                """
            )
        ).one()

    assert jan.winning_submission_id == s1
    assert jan.replaced_submission_id is None
    assert feb.winning_submission_id == s2
    assert feb.replaced_submission_id == s1
    assert _gold_month_rows_local(store, "acme", "__unknown__", "medical", "2025-01") == 1
    assert _gold_month_rows_local(store, "acme", "__unknown__", "medical", "2025-02") == 1


def test_submitter_gold_older_submission_is_noop(env):
    engine, store = env
    s1 = _parse_medical_submission(
        engine,
        store,
        b"Id,START,STOP,PATIENT,CODE\nenc1,2025-02-01T00:00:00Z,2025-02-01T01:00:00Z,p1,99201\n",
        object_name="medical_202502_202502.csv",
    )
    _mark_submission_validated(engine, s1, "2025-02-10T00:00:00+00:00")

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE submission SET received_at='2025-02-02T00:00:00+00:00' WHERE submission_id=:id"
            ),
            {"id": s1},
        )

    s2 = _parse_medical_submission(
        engine,
        store,
        b"Id,START,STOP,PATIENT,CODE\nenc2,2025-02-10T00:00:00Z,2025-02-10T01:00:00Z,p2,99202\n",
        object_name="medical_202502_202502.csv",
        drop_name="drop2",
    )
    _mark_submission_validated(engine, s2, "2025-02-20T00:00:00+00:00")
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE submission SET received_at='2025-02-15T00:00:00+00:00' WHERE submission_id=:id"
            ),
            {"id": s2},
        )

    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s2}}}},
        engine,
        store,
    ).success
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s1}}}},
        engine,
        store,
    ).success

    with engine.begin() as conn:
        pointer = conn.execute(
            text(
                "SELECT winning_submission_id FROM submitter_month_pointer WHERE atomic_month='2025-02'"
            )
        ).scalar_one()
        latest_build = conn.execute(
            text(
                """
                SELECT months_affected_json
                FROM submitter_gold_build
                WHERE submission_id=:submission_id
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {"submission_id": s1},
        ).scalar_one()

    assert pointer == s2
    assert json.loads(latest_build) == []


def _canonical_month_rows_local(store, state: str, file_type: str, month: str):
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import duckdb

    key_prefix = f"gold_canonical/{state}/{file_type}/atomic_month={month}/"
    keys = [obj.key for obj in store.list_objects(key_prefix) if obj.key.endswith(".parquet")]
    assert keys
    with TemporaryDirectory() as tempdir:
        local_paths: list[str] = []
        for idx, key in enumerate(keys):
            path = Path(tempdir) / f"canonical_{idx}.parquet"
            path.write_bytes(store.get_bytes(key))
            local_paths.append(path.as_posix())
        con = duckdb.connect()
        try:
            path_list = ", ".join(f"'{p}'" for p in local_paths)
            rows = con.execute(
                f"SELECT * FROM read_parquet([{path_list}]) ORDER BY submitter_id"
            ).fetchall()
            cols = [d[0] for d in con.description]
            return rows, cols
        finally:
            con.close()


def test_canonical_registry_files_are_valid_json():
    for schema_file in sorted(Path(SCHEMA_ROOT).glob("*/*.json")):
        payload = json.loads(schema_file.read_text())
        assert payload["file_type"]
        assert isinstance(payload["columns"], list)

    for mapping_file in sorted(Path(MAPPING_ROOT).glob("*/*/*.json")):
        payload = json.loads(mapping_file.read_text())
        assert payload["mapping_version"]
        assert isinstance(payload["mapping"], dict)


def test_canonical_union_across_submitters(env):
    engine, store = env
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    sync_canonical_registry(build_asset_context(resources={"metadata_db": engine}))

    for submitter_id, encounter_id in (("aetna", "enc1"), ("cigna", "enc2")):
        submission_id = str(uuid.uuid4())
        with engine.begin() as conn:
            layout_id = conn.execute(
                text(
                    "SELECT layout_id FROM layout_registry WHERE file_type='medical' AND layout_version='v1'"
                )
            ).scalar_one()
            conn.execute(
                text(
                    """
                    INSERT INTO submission(
                        submission_id, submitter_id, state, file_type, layout_id, coverage_start_month,
                        coverage_end_month, received_at, status, grouping_method, inbox_prefix, raw_prefix,
                        manifest_object_key, manifest_sha256, created_at, latest_validation_run_id
                    ) VALUES (
                        :submission_id, :submitter_id, 'MA', 'medical', :layout_id, '202501',
                        '202501', :received_at, 'VALIDATED', 'AUTO', 'inbox/x', 'raw/x',
                        'raw/x/manifest.json', 'abc', :received_at, :validation_run_id
                    )
                    """
                ),
                {
                    "submission_id": submission_id,
                    "submitter_id": submitter_id,
                    "layout_id": layout_id,
                    "received_at": "2025-02-20T00:00:00+00:00",
                    "validation_run_id": f"vr-{submission_id}",
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO validation_run(
                        validation_run_id, submission_id, rule_set_id, started_at, ended_at, status,
                        outcome, engine, engine_version, silver_prefix, total_rows, report_object_key, error_message
                    ) VALUES (
                        :validation_run_id, :submission_id, 'rules', :ended_at, :ended_at, 'SUCCEEDED',
                        'PASS', 'duckdb', NULL, :silver_prefix, 1, NULL, NULL
                    )
                    """
                ),
                {
                    "validation_run_id": f"vr-{submission_id}",
                    "submission_id": submission_id,
                    "ended_at": "2025-02-20T00:00:00+00:00",
                    "silver_prefix": f"silver/{submitter_id}/medical/{submission_id}",
                },
            )

        with duckdb.connect() as con:
            tmp = Path(f"/tmp/{submission_id}.parquet")
            con.execute(
                f"""
                COPY (
                    SELECT '{encounter_id}' AS Id,
                           '2025-01-01T00:00:00Z' AS START,
                           '2025-01-01T01:00:00Z' AS STOP,
                           '{submitter_id}-m1' AS PATIENT,
                           'prov1' AS PROVIDER,
                           'ORG' AS ORGANIZATION,
                           'ambulatory' AS ENCOUNTERCLASS,
                           '123' AS CODE,
                           'visit' AS DESCRIPTION
                ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
                """
            )
            store.put_bytes(
                f"silver/{submitter_id}/medical/{submission_id}/atomic_month=2025-01/part-0000.parquet",
                tmp.read_bytes(),
            )

        assert _execute_submitter_gold(
            {"ops": {"build_submitter_gold_op": {"config": {"submission_id": submission_id}}}},
            engine,
            store,
        ).success

    run_requests = _run_canonical_sensor(engine)
    assert run_requests
    result = _execute_canonical(run_requests[0].run_config, engine, store)
    assert result.success

    rows, cols = _canonical_month_rows_local(store, "MA", "medical", "2025-01")
    assert len(rows) == 2
    assert "source_submission_id" in cols
    assert "canonical_built_at" in cols
    submitters = {row[cols.index("submitter_id")] for row in rows}
    assert submitters == {"aetna", "cigna"}


def test_canonical_idempotency_skip(env):
    engine, store = env
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    sync_canonical_registry(build_asset_context(resources={"metadata_db": engine}))

    submission_id = str(uuid.uuid4())
    with engine.begin() as conn:
        layout_id = conn.execute(
            text(
                "SELECT layout_id FROM layout_registry WHERE file_type='medical' AND layout_version='v1'"
            )
        ).scalar_one()
        conn.execute(
            text(
                """
                INSERT INTO submission(
                    submission_id, submitter_id, state, file_type, layout_id, coverage_start_month,
                    coverage_end_month, received_at, status, grouping_method, inbox_prefix, raw_prefix,
                    manifest_object_key, manifest_sha256, created_at, latest_validation_run_id
                ) VALUES (
                    :submission_id, 'aetna', 'MA', 'medical', :layout_id, '202501',
                    '202501', '2025-02-20T00:00:00+00:00', 'VALIDATED', 'AUTO', 'inbox/x', 'raw/x',
                    'raw/x/manifest.json', 'abc', '2025-02-20T00:00:00+00:00', :validation_run_id
                )
                """
            ),
            {
                "submission_id": submission_id,
                "layout_id": layout_id,
                "validation_run_id": f"vr-{submission_id}",
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO validation_run(
                    validation_run_id, submission_id, rule_set_id, started_at, ended_at, status,
                    outcome, engine, engine_version, silver_prefix, total_rows, report_object_key, error_message
                ) VALUES (
                    :validation_run_id, :submission_id, 'rules', '2025-02-20T00:00:00+00:00',
                    '2025-02-20T00:00:00+00:00', 'SUCCEEDED', 'PASS', 'duckdb', NULL,
                    :silver_prefix, 1, NULL, NULL
                )
                """
            ),
            {
                "validation_run_id": f"vr-{submission_id}",
                "submission_id": submission_id,
                "silver_prefix": f"silver/aetna/medical/{submission_id}",
            },
        )

    with duckdb.connect() as con:
        tmp = Path(f"/tmp/{submission_id}.parquet")
        con.execute(
            f"""
            COPY (
                SELECT 'enc-idem' AS Id,
                       '2025-01-01T00:00:00Z' AS START,
                       '2025-01-01T01:00:00Z' AS STOP,
                       'member-idem' AS PATIENT,
                       'prov1' AS PROVIDER,
                       'ORG' AS ORGANIZATION,
                       'ambulatory' AS ENCOUNTERCLASS,
                       '123' AS CODE,
                       'visit' AS DESCRIPTION
            ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
            """
        )
        store.put_bytes(
            f"silver/aetna/medical/{submission_id}/atomic_month=2025-01/part-0000.parquet",
            tmp.read_bytes(),
        )

    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": submission_id}}}},
        engine,
        store,
    ).success

    assert _execute_canonical(
        {
            "ops": {
                "build_canonical_month_op": {
                    "config": {"state": "MA", "file_type": "medical", "atomic_month": "2025-01"}
                }
            }
        },
        engine,
        store,
    ).success

    result = _execute_canonical(
        {
            "ops": {
                "build_canonical_month_op": {
                    "config": {"state": "MA", "file_type": "medical", "atomic_month": "2025-01"}
                }
            }
        },
        engine,
        store,
    )
    assert result.success

    with engine.begin() as conn:
        statuses = conn.execute(
            text(
                """
                SELECT status
                FROM canonical_month_build
                WHERE state='MA' AND file_type='medical' AND atomic_month='2025-01'
                ORDER BY started_at
                """
            )
        ).fetchall()
    assert [row.status for row in statuses][-1] == "SKIPPED"


def test_canonical_correction_triggers_rebuild(env):
    engine, store = env
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    sync_canonical_registry(build_asset_context(resources={"metadata_db": engine}))

    layout_id = None
    with engine.begin() as conn:
        layout_id = conn.execute(
            text(
                "SELECT layout_id FROM layout_registry WHERE file_type='medical' AND layout_version='v1'"
            )
        ).scalar_one()

    def _seed(submission_id: str, patient: str, ended_at: str):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO submission(
                        submission_id, submitter_id, state, file_type, layout_id, coverage_start_month,
                        coverage_end_month, received_at, status, grouping_method, inbox_prefix, raw_prefix,
                        manifest_object_key, manifest_sha256, created_at, latest_validation_run_id
                    ) VALUES (
                        :submission_id, 'aetna', 'MA', 'medical', :layout_id, '202501',
                        '202501', :received_at, 'VALIDATED', 'AUTO', 'inbox/x', 'raw/x',
                        'raw/x/manifest.json', 'abc', :received_at, :validation_run_id
                    )
                    """
                ),
                {
                    "submission_id": submission_id,
                    "layout_id": layout_id,
                    "received_at": ended_at,
                    "validation_run_id": f"vr-{submission_id}",
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO validation_run(
                        validation_run_id, submission_id, rule_set_id, started_at, ended_at, status,
                        outcome, engine, engine_version, silver_prefix, total_rows, report_object_key, error_message
                    ) VALUES (
                        :validation_run_id, :submission_id, 'rules', :ended_at, :ended_at, 'SUCCEEDED',
                        'PASS', 'duckdb', NULL, :silver_prefix, 1, NULL, NULL
                    )
                    """
                ),
                {
                    "validation_run_id": f"vr-{submission_id}",
                    "submission_id": submission_id,
                    "ended_at": ended_at,
                    "silver_prefix": f"silver/aetna/medical/{submission_id}",
                },
            )
        with duckdb.connect() as con:
            tmp = Path(f"/tmp/{submission_id}.parquet")
            con.execute(
                f"""
                COPY (
                    SELECT 'enc-{submission_id[:4]}' AS Id,
                           '2025-01-01T00:00:00Z' AS START,
                           '2025-01-01T01:00:00Z' AS STOP,
                           '{patient}' AS PATIENT,
                           'prov1' AS PROVIDER,
                           'ORG' AS ORGANIZATION,
                           'ambulatory' AS ENCOUNTERCLASS,
                           '123' AS CODE,
                           'visit' AS DESCRIPTION
                ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
                """
            )
            store.put_bytes(
                f"silver/aetna/medical/{submission_id}/atomic_month=2025-01/part-0000.parquet",
                tmp.read_bytes(),
            )

    s1 = str(uuid.uuid4())
    _seed(s1, "member-old", "2025-02-20T00:00:00+00:00")
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s1}}}}, engine, store
    ).success
    assert _execute_canonical(_run_canonical_sensor(engine)[0].run_config, engine, store).success

    with engine.begin() as conn:
        first_fingerprint = conn.execute(
            text(
                "SELECT input_fingerprint FROM canonical_month_build WHERE status='SUCCEEDED' ORDER BY started_at DESC LIMIT 1"
            )
        ).scalar_one()

    s2 = str(uuid.uuid4())
    _seed(s2, "member-new", "2025-02-21T00:00:00+00:00")
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s2}}}}, engine, store
    ).success
    requests = _run_canonical_sensor(engine)
    assert requests
    assert _execute_canonical(requests[0].run_config, engine, store).success

    with engine.begin() as conn:
        second_fingerprint = conn.execute(
            text(
                "SELECT input_fingerprint FROM canonical_month_build WHERE status='SUCCEEDED' ORDER BY started_at DESC LIMIT 1"
            )
        ).scalar_one()

    assert second_fingerprint != first_fingerprint


def test_canonical_mapping_differences_union(env):
    engine, store = env
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))
    sync_canonical_registry(build_asset_context(resources={"metadata_db": engine}))

    with engine.begin() as conn:
        base_layout_id = conn.execute(
            text(
                "SELECT layout_id FROM layout_registry WHERE file_type='medical' AND layout_version='v1'"
            )
        ).scalar_one()
        schema_id = conn.execute(
            text(
                "SELECT canonical_schema_id FROM canonical_schema WHERE file_type='medical' AND schema_version='v1'"
            )
        ).scalar_one()
        alt_layout_id = str(uuid.uuid4())
        conn.execute(
            text(
                """
                INSERT INTO layout_registry(layout_id, file_type, layout_version, schema_json, parser_config_json, status)
                VALUES (:layout_id, 'medical', 'v2', :schema_json, '{}', 'ACTIVE')
                """
            ),
            {
                "layout_id": alt_layout_id,
                "schema_json": json.dumps({"columns": []}),
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO canonical_mapping(mapping_id, layout_id, canonical_schema_id, mapping_version, mapping_json, status, created_at)
                VALUES (:mapping_id, :layout_id, :schema_id, 'v1', :mapping_json, 'ACTIVE', :created_at)
                """
            ),
            {
                "mapping_id": str(uuid.uuid4()),
                "layout_id": alt_layout_id,
                "schema_id": schema_id,
                "mapping_json": json.dumps(
                    {
                        "member_id": {"source": "PATIENT"},
                        "encounter_id": {"source": "Id"},
                        "provider_id": {"const": "CONSTPROV"},
                        "service_start_date": {"source": "START", "cast": "DATE"},
                        "service_end_date": {"source": "STOP", "cast": "DATE"},
                        "encounter_class": {"source": "ENCOUNTERCLASS"},
                        "code": {"source": "CODE"},
                        "description": {"source": "DESCRIPTION"},
                    }
                ),
                "created_at": "2025-02-20T00:00:00+00:00",
            },
        )

    def _seed(submission_id: str, submitter_id: str, layout_id: str):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO submission(
                        submission_id, submitter_id, state, file_type, layout_id, coverage_start_month,
                        coverage_end_month, received_at, status, grouping_method, inbox_prefix, raw_prefix,
                        manifest_object_key, manifest_sha256, created_at, latest_validation_run_id
                    ) VALUES (
                        :submission_id, :submitter_id, 'MA', 'medical', :layout_id, '202501',
                        '202501', '2025-02-20T00:00:00+00:00', 'VALIDATED', 'AUTO', 'inbox/x', 'raw/x',
                        'raw/x/manifest.json', 'abc', '2025-02-20T00:00:00+00:00', :validation_run_id
                    )
                    """
                ),
                {
                    "submission_id": submission_id,
                    "submitter_id": submitter_id,
                    "layout_id": layout_id,
                    "validation_run_id": f"vr-{submission_id}",
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO validation_run(
                        validation_run_id, submission_id, rule_set_id, started_at, ended_at, status,
                        outcome, engine, engine_version, silver_prefix, total_rows, report_object_key, error_message
                    ) VALUES (
                        :validation_run_id, :submission_id, 'rules', '2025-02-20T00:00:00+00:00',
                        '2025-02-20T00:00:00+00:00', 'SUCCEEDED', 'PASS', 'duckdb', NULL,
                        :silver_prefix, 1, NULL, NULL
                    )
                    """
                ),
                {
                    "validation_run_id": f"vr-{submission_id}",
                    "submission_id": submission_id,
                    "silver_prefix": f"silver/{submitter_id}/medical/{submission_id}",
                },
            )
        with duckdb.connect() as con:
            tmp = Path(f"/tmp/{submission_id}.parquet")
            con.execute(
                f"""
                COPY (
                    SELECT '{submission_id[:6]}' AS Id,
                           '2025-01-01T00:00:00Z' AS START,
                           '2025-01-01T01:00:00Z' AS STOP,
                           '{submitter_id}-m' AS PATIENT,
                           'prov1' AS PROVIDER,
                           'ORG' AS ORGANIZATION,
                           'ambulatory' AS ENCOUNTERCLASS,
                           '123' AS CODE,
                           'visit' AS DESCRIPTION
                ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
                """
            )
            store.put_bytes(
                f"silver/{submitter_id}/medical/{submission_id}/atomic_month=2025-01/part-0000.parquet",
                tmp.read_bytes(),
            )

    s1 = str(uuid.uuid4())
    s2 = str(uuid.uuid4())
    _seed(s1, "aetna", base_layout_id)
    _seed(s2, "cigna", alt_layout_id)
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s1}}}}, engine, store
    ).success
    assert _execute_submitter_gold(
        {"ops": {"build_submitter_gold_op": {"config": {"submission_id": s2}}}}, engine, store
    ).success

    requests = _run_canonical_sensor(engine)
    assert requests
    assert _execute_canonical(requests[0].run_config, engine, store).success
    rows, cols = _canonical_month_rows_local(store, "MA", "medical", "2025-01")
    provider_values = {row[cols.index("provider_id")] for row in rows}
    assert provider_values == {"prov1", "CONSTPROV"}
