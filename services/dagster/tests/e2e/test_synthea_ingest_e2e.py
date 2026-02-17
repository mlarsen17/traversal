from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

import duckdb
from sqlalchemy import text

from tests.e2e.helpers.runtime import (
    build_resources,
    lookup_submission,
    run_parse_submission,
    run_register_submission,
    sync_layouts,
)
from tests.e2e.synthea.convert_to_intake import convert_synthea_to_intake
from tests.e2e.synthea.run_synthea import run_synthea


def test_synthea_ingest_intake_parse_e2e():
    submitter_id = f"synthea-{uuid.uuid4().hex[:8]}"
    drop_folder = "drop-e2e"
    intake_filename = "medical_202401_202401.csv"
    intake_key = f"inbox/{submitter_id}/{drop_folder}/{intake_filename}"

    engine, store = build_resources()
    sync_layouts(engine)

    with tempfile.TemporaryDirectory() as tempdir:
        temp_path = Path(tempdir)
        csv_dir = run_synthea(temp_path / "synthea", population=10)
        intake_path = convert_synthea_to_intake(
            csv_dir / "encounters.csv", temp_path / intake_filename
        )

        store.put_bytes(intake_key, intake_path.read_bytes(), content_type="text/csv")
        store.put_bytes(f"inbox/{submitter_id}/{drop_folder}/_SUCCESS", b"")

    register_result = run_register_submission(engine, store, submitter_id, drop_folder, intake_key)
    assert register_result.success

    submission = lookup_submission(engine, submitter_id, drop_folder)
    assert submission is not None
    assert submission.status == "READY_FOR_PARSE"

    parse_result = run_parse_submission(engine, store, submission.submission_id)
    assert parse_result.success

    with engine.begin() as conn:
        final = conn.execute(
            text("SELECT status FROM submission WHERE submission_id=:submission_id"),
            {"submission_id": submission.submission_id},
        ).fetchone()

    assert final is not None
    assert final.status == "PARSED"

    raw_data_key = (
        f"raw/{submitter_id}/medical/{submission.submission_id}/data/{intake_filename}"
    )
    raw_manifest_key = (
        f"raw/{submitter_id}/medical/{submission.submission_id}/manifest.generated.json"
    )
    silver_prefix = f"silver/{submitter_id}/medical/{submission.submission_id}"
    parse_report_key = f"{silver_prefix}/parse_report.json"

    assert store.stat_object(raw_data_key) is not None
    assert store.stat_object(raw_manifest_key) is not None
    assert store.stat_object(parse_report_key) is not None

    silver_parts = store.list_objects(f"{silver_prefix}/atomic_month=")
    parquet_parts = [obj.key for obj in silver_parts if obj.key.endswith(".parquet")]
    assert parquet_parts, "Expected at least one silver parquet object"

    with tempfile.TemporaryDirectory() as parquet_temp_dir:
        parquet_path = Path(parquet_temp_dir) / "part.parquet"
        parquet_path.write_bytes(store.get_bytes(parquet_parts[0]))
        columns = {
            row[0]
            for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}')").fetchall()
        }

    assert {"submission_id", "source_object_key", "ingested_at", "layout_id", "layout_version"}.issubset(columns)
    assert submission.layout_id is not None
    with engine.begin() as conn:
        layout_row = conn.execute(
            text("SELECT file_type, layout_version FROM layout_registry WHERE layout_id=:layout_id"),
            {"layout_id": submission.layout_id},
        ).fetchone()

    assert layout_row is not None
    assert layout_row.file_type == "medical"
    assert layout_row.layout_version == "v1"

    engine.dispose()
