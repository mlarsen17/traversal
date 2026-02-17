from __future__ import annotations

import hashlib

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor
from sqlalchemy import text

from health_platform.parse.jobs import parse_submission_job


@sensor(
    minimum_interval_seconds=30, required_resource_keys={"metadata_db"}, job=parse_submission_job
)
def ready_for_parse_sensor(context: SensorEvaluationContext, metadata_db):
    with metadata_db.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT submission_id
                FROM submission
                WHERE status = 'READY_FOR_PARSE'
                ORDER BY received_at
                LIMIT 25
                """
            )
        ).fetchall()

    if not rows:
        return SkipReason("No submissions ready for parse")

    for row in rows:
        digest = hashlib.sha1(row.submission_id.encode("utf-8")).hexdigest()[:12]
        yield RunRequest(
            run_key=f"parse:{digest}",
            run_config={
                "ops": {"parse_submission_op": {"config": {"submission_id": row.submission_id}}}
            },
        )
