from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import boto3
import pytest
from dagster import build_sensor_context
from sqlalchemy import create_engine, text

from health_platform.assets.intake_assets import register_submission_from_group_job
from health_platform.resources import metadata_db_resource, minio_resource
from health_platform.sensors.inbox_sensors import inbox_discovery_sensor


@pytest.fixture
def minio_endpoint(tmp_path):
    minio_bin = shutil.which("minio")
    if not minio_bin:
        pytest.skip("minio binary is required for this integration test")

    data_dir = tmp_path / "minio-data"
    data_dir.mkdir(parents=True, exist_ok=True)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    env = os.environ.copy()
    env["MINIO_ROOT_USER"] = "minioadmin"
    env["MINIO_ROOT_PASSWORD"] = "minioadmin"

    process = subprocess.Popen(
        [minio_bin, "server", str(data_dir), "--address", f"127.0.0.1:{port}"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    endpoint = f"127.0.0.1:{port}"
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    for _ in range(40):
        try:
            client.list_buckets()
            break
        except Exception:
            time.sleep(0.25)
    else:
        process.terminate()
        pytest.fail("minio did not start in time")

    yield endpoint

    process.terminate()
    process.wait(timeout=5)


def test_intake_submission_registration_end_to_end(tmp_path, monkeypatch, minio_endpoint):
    db_path = tmp_path / "metadata.db"
    db_url = f"sqlite:///{db_path}"

    env = os.environ.copy()
    env["METADATA_DB_URL"] = db_url
    subprocess.run(["alembic", "upgrade", "head"], cwd="/workspace/traversal/services/migrations", env=env, check=True)

    monkeypatch.setenv("METADATA_DB_URL", db_url)
    monkeypatch.setenv("MINIO_ENDPOINT", minio_endpoint)
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECURE", "false")
    monkeypatch.setenv("MINIO_REGION", "us-east-1")
    monkeypatch.setenv("RAW_BUCKET_NAME", "health-raw")
    monkeypatch.setenv("INBOX_PREFIX", "inbox/")
    monkeypatch.setenv("RAW_PREFIX", "raw/")
    monkeypatch.setenv("GROUP_BY_DEPTH", "3")
    monkeypatch.setenv("QUIESCENCE_MINUTES", "15")
    monkeypatch.setenv("CLASSIFY_CONFIDENCE_THRESHOLD", "0.7")
    monkeypatch.setenv("CLASSIFIER_IMPL", "submitter_filename")

    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{minio_endpoint}",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    s3_client.create_bucket(Bucket="health-raw")

    s3_client.put_object(
        Bucket="health-raw",
        Key="inbox/acme/unknown/drop-001/medical_claims.csv",
        Body=b"member_id,claim_id\nm1,c1\n",
    )
    s3_client.put_object(Bucket="health-raw", Key="inbox/acme/unknown/drop-001/_SUCCESS", Body=b"")

    with build_sensor_context(resources={"metadata_db": metadata_db_resource, "minio": minio_resource}) as context:
        run_requests = list(inbox_discovery_sensor(context))

    assert len(run_requests) == 1

    result = register_submission_from_group_job.execute_in_process(
        run_config=run_requests[0].run_config,
        resources={"metadata_db": metadata_db_resource, "minio": minio_resource},
    )
    assert result.success

    engine = create_engine(db_url)
    with engine.connect() as conn:
        submission = conn.execute(
            text("SELECT submission_id, state, inferred_file_type, raw_prefix FROM submission")
        ).mappings().one()
        files = conn.execute(text("SELECT count(1) AS c FROM submission_file")).mappings().one()

    assert submission["state"] == "READY_FOR_PARSE"
    assert submission["inferred_file_type"] == "medical"
    assert files["c"] == 1

    manifest_key = f"{submission['raw_prefix']}_manifest.generated.json"
    manifest = s3_client.get_object(Bucket="health-raw", Key=manifest_key)["Body"].read()
    manifest_data = json.loads(manifest)

    assert manifest_data["submission_id"] == submission["submission_id"]
    assert manifest_data["inferred_file_type"] == "medical"
    assert manifest_data["grouping_method"] == "MARKER_FILE"
