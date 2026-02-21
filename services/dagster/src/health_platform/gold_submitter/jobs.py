from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from dagster import Field, Out, graph, op
from sqlalchemy import text

from health_platform.gold_submitter.engine_duckdb import (
    discover_submission_months,
    overwrite_gold_month_partition,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


@op(
    required_resource_keys={"metadata_db", "object_store"},
    config_schema={
        "submission_id": Field(str, is_required=False),
        "max_submissions": Field(int, default_value=1, is_required=False),
    },
    out=Out(list[str]),
)
def build_submitter_gold_op(context) -> list[str]:
    configured_submission_id = context.op_config.get("submission_id")
    max_submissions = context.op_config.get("max_submissions", 1)

    with context.resources.metadata_db.begin() as conn:
        if configured_submission_id:
            rows = conn.execute(
                text(
                    """
                    SELECT submission_id, submitter_id, state, file_type, status, latest_validation_run_id,
                           received_at, created_at
                    FROM submission
                    WHERE submission_id = :submission_id
                    """
                ),
                {"submission_id": configured_submission_id},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT submission_id, submitter_id, state, file_type, status, latest_validation_run_id,
                           received_at, created_at
                    FROM submission
                    WHERE status IN ('VALIDATED', 'VALIDATED_WITH_WARNINGS')
                    ORDER BY received_at
                    LIMIT :max_submissions
                    """
                ),
                {"max_submissions": max_submissions},
            ).fetchall()

    if not rows:
        context.log.info("No submissions ready for submitter-gold merge")
        return []

    processed: list[str] = []
    for row in rows:
        if row.status not in {"VALIDATED", "VALIDATED_WITH_WARNINGS"}:
            raise RuntimeError(
                f"Submission {row.submission_id} is not eligible for submitter gold merge: status={row.status}"
            )

        state = row.state or "__unknown__"
        build_id = str(uuid.uuid4())
        started_at = _now()
        months_changed: list[str] = []

        with context.resources.metadata_db.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO submitter_gold_build(
                        submitter_gold_build_id, submission_id, started_at, status, months_affected_json
                    ) VALUES (
                        :submitter_gold_build_id, :submission_id, :started_at, 'STARTED', :months_affected_json
                    )
                    """
                ),
                {
                    "submitter_gold_build_id": build_id,
                    "submission_id": row.submission_id,
                    "started_at": started_at,
                    "months_affected_json": json.dumps([]),
                },
            )

        try:
            with context.resources.metadata_db.begin() as conn:
                validation_ended_at = None
                if row.latest_validation_run_id:
                    validation_ended_at = conn.execute(
                        text(
                            """
                            SELECT ended_at
                            FROM validation_run
                            WHERE validation_run_id = :validation_run_id AND status='SUCCEEDED'
                            """
                        ),
                        {"validation_run_id": row.latest_validation_run_id},
                    ).scalar_one_or_none()

            winner_timestamp = validation_ended_at or row.received_at or row.created_at
            if winner_timestamp is None:
                raise RuntimeError(
                    f"Submission {row.submission_id} has no available winner timestamp"
                )

            silver_prefix = f"silver/{row.submitter_id}/{row.file_type}/{row.submission_id}"
            months = discover_submission_months(context.resources.object_store, silver_prefix)

            for month in months:
                with context.resources.metadata_db.begin() as conn:
                    result = conn.execute(
                        text(
                            """
                            INSERT INTO submitter_month_pointer(
                                state, file_type, submitter_id, atomic_month, winning_submission_id,
                                replaced_submission_id, winning_validation_run_id, winner_timestamp,
                                updated_at, reason
                            ) VALUES (
                                :state, :file_type, :submitter_id, :atomic_month, :winning_submission_id,
                                NULL, :winning_validation_run_id, :winner_timestamp, :updated_at, :reason
                            )
                            ON CONFLICT(state, file_type, submitter_id, atomic_month)
                            DO UPDATE SET
                                replaced_submission_id = submitter_month_pointer.winning_submission_id,
                                winning_submission_id = excluded.winning_submission_id,
                                winning_validation_run_id = excluded.winning_validation_run_id,
                                winner_timestamp = excluded.winner_timestamp,
                                updated_at = excluded.updated_at,
                                reason = excluded.reason
                            WHERE excluded.winner_timestamp > submitter_month_pointer.winner_timestamp
                            RETURNING winning_submission_id
                            """
                        ),
                        {
                            "state": state,
                            "file_type": row.file_type,
                            "submitter_id": row.submitter_id,
                            "atomic_month": month,
                            "winning_submission_id": row.submission_id,
                            "winning_validation_run_id": row.latest_validation_run_id,
                            "winner_timestamp": winner_timestamp,
                            "updated_at": _now(),
                            "reason": "latest_validated_wins",
                        },
                    ).fetchone()
                    if result is not None:
                        months_changed.append(month)
                        conn.execute(
                            text(
                                """
                                INSERT INTO canonical_rebuild_queue(
                                    state, file_type, atomic_month, enqueued_at, reason
                                ) VALUES (
                                    :state, :file_type, :atomic_month, :enqueued_at, :reason
                                )
                                """
                            ),
                            {
                                "state": state,
                                "file_type": row.file_type,
                                "atomic_month": month,
                                "enqueued_at": _now(),
                                "reason": "SUBMITTER_MONTH_WINNER_UPDATED",
                            },
                        )

            gold_built_at = _now()
            for month in months_changed:
                gold_month_prefix = (
                    f"gold_submitter/{state}/{row.file_type}/{row.submitter_id}/"
                    f"atomic_month={month}"
                )
                overwrite_gold_month_partition(
                    object_store=context.resources.object_store,
                    silver_prefix=silver_prefix,
                    gold_month_prefix=gold_month_prefix,
                    month=month,
                    submitter_id=row.submitter_id,
                    state=state,
                    file_type=row.file_type,
                    gold_built_at=gold_built_at,
                )

            with context.resources.metadata_db.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE submitter_gold_build
                        SET ended_at=:ended_at, status='SUCCEEDED', months_affected_json=:months_affected_json
                        WHERE submitter_gold_build_id=:submitter_gold_build_id
                        """
                    ),
                    {
                        "ended_at": _now(),
                        "months_affected_json": json.dumps(months_changed),
                        "submitter_gold_build_id": build_id,
                    },
                )

            processed.append(row.submission_id)
        except Exception as exc:
            with context.resources.metadata_db.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE submitter_gold_build
                        SET ended_at=:ended_at, status='FAILED', error_message=:error_message,
                            months_affected_json=:months_affected_json
                        WHERE submitter_gold_build_id=:submitter_gold_build_id
                        """
                    ),
                    {
                        "ended_at": _now(),
                        "error_message": str(exc),
                        "months_affected_json": json.dumps(months_changed),
                        "submitter_gold_build_id": build_id,
                    },
                )
            raise

    return processed


@graph
def build_submitter_gold_graph():
    build_submitter_gold_op()


build_submitter_gold_job = build_submitter_gold_graph.to_job(name="build_submitter_gold_job")
