from __future__ import annotations

import hashlib

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor
from sqlalchemy import text

from health_platform.gold_submitter.jobs import build_submitter_gold_job


@sensor(
    minimum_interval_seconds=30,
    required_resource_keys={"metadata_db"},
    job=build_submitter_gold_job,
)
def ready_for_submitter_gold_sensor(context: SensorEvaluationContext, metadata_db):
    with metadata_db.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT submission_id
                FROM submission
                WHERE status IN ('VALIDATED', 'VALIDATED_WITH_WARNINGS')
                ORDER BY received_at
                LIMIT 25
                """
            )
        ).fetchall()

    if not rows:
        return SkipReason("No submissions ready for submitter gold merge")

    for row in rows:
        digest = hashlib.sha1(row.submission_id.encode("utf-8")).hexdigest()[:12]
        yield RunRequest(
            run_key=f"submitter-gold:{digest}",
            run_config={
                "ops": {"build_submitter_gold_op": {"config": {"submission_id": row.submission_id}}}
            },
        )
