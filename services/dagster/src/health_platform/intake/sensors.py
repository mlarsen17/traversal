from __future__ import annotations

import json
import os

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from health_platform.intake.constants import INBOX_ROOT
from health_platform.intake.jobs import discover_inbox_objects_job, register_submission_job
from health_platform.intake.processing import find_closed_groups


@sensor(
    minimum_interval_seconds=15,
    required_resource_keys={"metadata_db", "object_store"},
    job=discover_inbox_objects_job,
)
def inbox_discovery_sensor(context: SensorEvaluationContext, metadata_db, object_store):
    _ = metadata_db
    inbox_objects = [
        obj for obj in object_store.list_objects(f"{INBOX_ROOT}/") if not obj.key.endswith("/")
    ]
    if not inbox_objects:
        return SkipReason("No inbox objects pending discovery")

    context.log.info("Inbox discovery sensor detected %s object(s)", len(inbox_objects))
    run_tokens = [
        f"{obj.key}:{obj.size}:{obj.etag or ''}"
        for obj in sorted(inbox_objects, key=lambda entry: entry.key)
    ]
    run_key = f"discover:{','.join(run_tokens)}"
    return RunRequest(run_key=run_key)


@sensor(
    minimum_interval_seconds=30,
    required_resource_keys={"metadata_db", "object_store"},
    job=register_submission_job,
)
def inbox_grouping_sensor(context: SensorEvaluationContext, metadata_db, object_store):
    _ = object_store
    quiescence_minutes = int(os.getenv("INTAKE_QUIESCENCE_MINUTES", "10"))
    groups = find_closed_groups(metadata_db, quiescence_minutes=quiescence_minutes)
    if not groups:
        return SkipReason("No closed groups")

    context.log.info("Inbox grouping sensor found %s closed group(s)", len(groups))
    for group in groups:
        run_key = f"{group.grouping_method}:{group.inbox_prefix}:{','.join(group.object_keys)}"
        yield RunRequest(
            run_key=run_key,
            run_config={
                "ops": {
                    "register_submission_op": {
                        "config": {
                            "submitter_id": group.submitter_id,
                            "inbox_prefix": group.inbox_prefix,
                            "grouping_method": group.grouping_method,
                            "object_keys": group.object_keys,
                        }
                    }
                }
            },
            tags={"group": json.dumps(group.object_keys)},
        )
