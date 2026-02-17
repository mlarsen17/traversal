from __future__ import annotations

from dagster import build_asset_context
from sqlalchemy import text

from health_platform.assets.layout_assets import sync_layout_registry
from health_platform.intake.jobs import register_submission_job
from health_platform.parse.jobs import parse_submission_job
from health_platform.resources import _build_s3_client, _metadata_db_url
from health_platform.intake.object_store import S3ObjectStore
from health_platform.utils.s3 import resolve_s3_settings
from sqlalchemy import create_engine


def build_resources():
    engine = create_engine(_metadata_db_url(), future=True)
    settings = resolve_s3_settings()
    store = S3ObjectStore(_build_s3_client(), settings.bucket)
    return engine, store


def sync_layouts(engine) -> None:
    sync_layout_registry(build_asset_context(resources={"metadata_db": engine}))


def run_register_submission(engine, store, submitter_id: str, drop_folder: str, object_key: str):
    return register_submission_job.execute_in_process(
        run_config={
            "ops": {
                "register_submission_op": {
                    "config": {
                        "submitter_id": submitter_id,
                        "inbox_prefix": f"inbox/{submitter_id}/{drop_folder}",
                        "grouping_method": "MARKER",
                        "object_keys": [object_key],
                    }
                }
            }
        },
        resources={"metadata_db": engine, "object_store": store},
    )


def lookup_submission(engine, submitter_id: str, drop_folder: str):
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                SELECT submission_id, status, layout_id
                FROM submission
                WHERE submitter_id=:submitter_id AND inbox_prefix=:inbox_prefix
                ORDER BY received_at DESC
                LIMIT 1
                """
            ),
            {"submitter_id": submitter_id, "inbox_prefix": f"inbox/{submitter_id}/{drop_folder}"},
        ).fetchone()


def run_parse_submission(engine, store, submission_id: str):
    return parse_submission_job.execute_in_process(
        run_config={"ops": {"parse_submission_op": {"config": {"submission_id": submission_id}}}},
        resources={"metadata_db": engine, "object_store": store},
    )
