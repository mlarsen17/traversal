from __future__ import annotations

import json
import os

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from health_platform.intake.jobs import register_submission_job
from health_platform.intake.processing import discover_inbox_objects, find_closed_groups


@sensor(minimum_interval_seconds=15, required_resource_keys={"metadata_db", "object_store"})
def inbox_discovery_sensor(context: SensorEvaluationContext, metadata_db, object_store):
    discovered = discover_inbox_objects(metadata_db, object_store)
    if discovered == 0:
        return SkipReason("No new inbox objects discovered")
    return SkipReason(f"Discovered/updated {discovered} inbox objects")


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
