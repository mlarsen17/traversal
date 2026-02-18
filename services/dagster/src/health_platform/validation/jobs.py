from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from dagster import Field, Out, graph, op
from sqlalchemy import text

from health_platform.validation.engine_duckdb import resolve_rule_set, run_validation_engine


@dataclass(frozen=True)
class SubmissionForValidation:
    submission_id: str
    submitter_id: str
    file_type: str
    layout_id: str | None
    layout_version: str | None
    coverage_start_month: str | None
    coverage_end_month: str | None
    status: str


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
def validate_submission_op(context) -> list[str]:
    configured_submission_id = context.op_config.get("submission_id")
    max_submissions = context.op_config.get("max_submissions", 1)

    with context.resources.metadata_db.begin() as conn:
        if configured_submission_id:
            rows = conn.execute(
                text(
                    """
                    SELECT submission_id, submitter_id, file_type, layout_id, coverage_start_month, coverage_end_month, status
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
                    SELECT submission_id, submitter_id, file_type, layout_id, coverage_start_month, coverage_end_month, status
                    FROM submission
                    WHERE status = 'PARSED'
                    ORDER BY received_at
                    LIMIT :max_submissions
                    """
                ),
                {"max_submissions": max_submissions},
            ).fetchall()

    if not rows:
        context.log.info("No submissions available for validation")
        return []

    processed: list[str] = []
    for row in rows:
        if row.status != "PARSED":
            context.log.info("Skipping submission %s with status=%s", row.submission_id, row.status)
            continue

        with context.resources.metadata_db.begin() as conn:
            layout_row = conn.execute(
                text("SELECT layout_version FROM layout_registry WHERE layout_id = :layout_id"),
                {"layout_id": row.layout_id},
            ).fetchone()

        submission = SubmissionForValidation(
            submission_id=row.submission_id,
            submitter_id=row.submitter_id,
            file_type=row.file_type,
            layout_id=row.layout_id,
            layout_version=layout_row.layout_version if layout_row else None,
            coverage_start_month=row.coverage_start_month,
            coverage_end_month=row.coverage_end_month,
            status=row.status,
        )

        validation_run_id = str(uuid.uuid4())
        silver_prefix = (
            f"silver/{submission.submitter_id}/{submission.file_type}/{submission.submission_id}"
        )
        started_at = _now()

        try:
            rule_set_id = resolve_rule_set(context.resources.metadata_db, submission)
            context.log.info(
                "Resolved validation rule set %s for submission %s",
                rule_set_id,
                submission.submission_id,
            )

            with context.resources.metadata_db.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO validation_run(
                            validation_run_id, submission_id, rule_set_id, started_at, status, engine, engine_version, silver_prefix
                        ) VALUES (
                            :validation_run_id, :submission_id, :rule_set_id, :started_at, 'STARTED', 'duckdb', :engine_version, :silver_prefix
                        )
                        """
                    ),
                    {
                        "validation_run_id": validation_run_id,
                        "submission_id": submission.submission_id,
                        "rule_set_id": rule_set_id,
                        "started_at": started_at,
                        "engine_version": None,
                        "silver_prefix": silver_prefix,
                    },
                )

            report = run_validation_engine(
                metadata_db=context.resources.metadata_db,
                object_store=context.resources.object_store,
                submission=submission,
                validation_run_id=validation_run_id,
                rule_set_id=rule_set_id,
                started_at=started_at,
                logger=context.log,
            )

            with context.resources.metadata_db.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE validation_run
                        SET ended_at=:ended_at, status='SUCCEEDED', outcome=:outcome,
                            total_rows=:total_rows, report_object_key=:report_object_key
                        WHERE validation_run_id=:validation_run_id
                        """
                    ),
                    {
                        "ended_at": _now(),
                        "outcome": report.outcome,
                        "total_rows": report.total_rows,
                        "report_object_key": report.report_object_key,
                        "validation_run_id": validation_run_id,
                    },
                )
                conn.execute(
                    text(
                        """
                        UPDATE submission
                        SET status=:status, latest_validation_run_id=:validation_run_id
                        WHERE submission_id=:submission_id
                        """
                    ),
                    {
                        "status": report.submission_status,
                        "validation_run_id": validation_run_id,
                        "submission_id": submission.submission_id,
                    },
                )

            context.log.info(
                "Validated submission %s outcome=%s status=%s",
                submission.submission_id,
                report.outcome,
                report.submission_status,
            )
            processed.append(submission.submission_id)
        except Exception as exc:
            with context.resources.metadata_db.begin() as conn:
                existing = conn.execute(
                    text(
                        "SELECT validation_run_id FROM validation_run WHERE validation_run_id=:validation_run_id"
                    ),
                    {"validation_run_id": validation_run_id},
                ).fetchone()
                if existing:
                    conn.execute(
                        text(
                            """
                            UPDATE validation_run
                            SET ended_at=:ended_at, status='FAILED', error_message=:error
                            WHERE validation_run_id=:validation_run_id
                            """
                        ),
                        {
                            "ended_at": _now(),
                            "error": str(exc),
                            "validation_run_id": validation_run_id,
                        },
                    )
            raise

    return processed


@graph
def validate_submission_graph():
    validate_submission_op()


validate_submission_job = validate_submission_graph.to_job(name="validate_submission_job")
