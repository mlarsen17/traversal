from __future__ import annotations

import os
from datetime import datetime, timezone

from dagster import RunRequest, SensorEvaluationContext, sensor
from sqlalchemy import text

from health_platform.assets.intake_assets import register_submission_from_group_job
from health_platform.intake.grouping import discover_closed_groups, grouping_key_for_object


@sensor(job=register_submission_from_group_job, minimum_interval_seconds=30, required_resource_keys={"metadata_db", "minio"})
def inbox_discovery_sensor(context: SensorEvaluationContext, metadata_db=None, minio=None):
    engine = metadata_db or context.resources.metadata_db
    object_store = minio or context.resources.minio
    inbox_prefix = os.getenv("INBOX_PREFIX", "inbox/")
    group_by_depth = int(os.getenv("GROUP_BY_DEPTH", "3"))
    now = datetime.now(timezone.utc)

    objects = object_store.list_objects(inbox_prefix)
    with engine.begin() as conn:
        for obj in objects:
            grouping_key = grouping_key_for_object(obj.key, inbox_prefix, group_by_depth)
            conn.execute(
                text(
                    """
                    INSERT INTO inbox_object (object_key, grouping_key, size_bytes, etag, last_modified, first_seen_at, last_seen_at, status)
                    VALUES (:object_key, :grouping_key, :size_bytes, :etag, :last_modified, :first_seen_at, :last_seen_at, :status)
                    ON CONFLICT(object_key)
                    DO UPDATE SET grouping_key=:grouping_key, size_bytes=:size_bytes, etag=:etag, last_modified=:last_modified, last_seen_at=:last_seen_at
                    """
                ),
                {
                    "object_key": obj.key,
                    "grouping_key": grouping_key,
                    "size_bytes": obj.size_bytes,
                    "etag": obj.etag,
                    "last_modified": obj.last_modified,
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "status": "SEEN",
                },
            )

    requests: list[RunRequest] = []
    for candidate in discover_closed_groups(objects):
        with engine.begin() as conn:
            invalid_status_count = conn.execute(
                text("SELECT count(1) FROM inbox_object WHERE grouping_key=:grouping_key AND status NOT IN ('SEEN','CLAIMED','MOVED')"),
                {"grouping_key": candidate.grouping_key},
            ).scalar_one()
            already_registered = conn.execute(
                text("SELECT submission_id FROM submission WHERE grouping_key=:grouping_key AND state IN ('REGISTERED','STAGED','READY_FOR_PARSE','NEEDS_REVIEW')"),
                {"grouping_key": candidate.grouping_key},
            ).first()

        if invalid_status_count or already_registered:
            continue

        requests.append(
            RunRequest(
                run_key=f"{candidate.grouping_key}:{candidate.grouping_method}",
                run_config={
                    "ops": {
                        "register_submission_from_group": {
                            "config": {
                                "grouping_key": candidate.grouping_key,
                                "grouping_method": candidate.grouping_method,
                            }
                        }
                    }
                },
                tags={"grouping_key": candidate.grouping_key, "grouping_method": candidate.grouping_method},
            )
        )

    return requests
