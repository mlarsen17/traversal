from __future__ import annotations

import hashlib

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor
from sqlalchemy import text

from health_platform.gold_canonical.jobs import build_canonical_month_job


@sensor(
    minimum_interval_seconds=30,
    required_resource_keys={"metadata_db"},
    job=build_canonical_month_job,
)
def canonical_rebuild_sensor(context: SensorEvaluationContext, metadata_db):
    with metadata_db.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT queue_id, state, file_type, atomic_month
                FROM canonical_rebuild_queue
                WHERE processed_at IS NULL
                ORDER BY enqueued_at, queue_id
                LIMIT 250
                """
            )
        ).fetchall()

        if not rows:
            return SkipReason("No canonical rebuilds queued")

        grouped: dict[tuple[str, str, str], list[int]] = {}
        for row in rows:
            grouped.setdefault((row.state, row.file_type, row.atomic_month), []).append(
                row.queue_id
            )

        for (state, file_type, atomic_month), _queue_ids in grouped.items():
            schema_version = (
                conn.execute(
                    text(
                        """
                    SELECT schema_version
                    FROM canonical_schema
                    WHERE file_type=:file_type AND status='ACTIVE'
                    ORDER BY schema_version DESC
                    LIMIT 1
                    """
                    ),
                    {"file_type": file_type},
                ).scalar_one_or_none()
                or "none"
            )

            digest = hashlib.sha256(
                f"{state}|{file_type}|{atomic_month}|{schema_version}".encode("utf-8")
            ).hexdigest()[:20]
            yield RunRequest(
                run_key=f"canonical:{digest}",
                run_config={
                    "ops": {
                        "build_canonical_month_op": {
                            "config": {
                                "state": state,
                                "file_type": file_type,
                                "atomic_month": atomic_month,
                            }
                        }
                    }
                },
            )
