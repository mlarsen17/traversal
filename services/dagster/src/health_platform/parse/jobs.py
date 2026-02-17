from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from dagster import Field, Out, graph, op
from sqlalchemy import text

from health_platform.intake.constants import SILVER_ROOT
from health_platform.parse.engine_duckdb import parse_submission_to_silver


@dataclass(frozen=True)
class SubmissionForParse:
    submission_id: str
    submitter_id: str
    file_type: str
    layout_id: str | None
    layout_version: str | None
    parse_run_id: str
    files: list


def _now() -> datetime:
    return datetime.now(timezone.utc)


@op(
    required_resource_keys={"metadata_db", "object_store"},
    config_schema={"submission_id": Field(str, is_required=False)},
    out=Out(str),
)
def parse_submission_op(context) -> str:
    configured_submission_id = context.op_config.get("submission_id")

    with context.resources.metadata_db.begin() as conn:
        if configured_submission_id:
            row = conn.execute(
                text(
                    """
                    SELECT submission_id, submitter_id, file_type, layout_id, status
                    FROM submission
                    WHERE submission_id = :submission_id
                    """
                ),
                {"submission_id": configured_submission_id},
            ).fetchone()
        else:
            row = conn.execute(
                text(
                    """
                    SELECT submission_id, submitter_id, file_type, layout_id, status
                    FROM submission
                    WHERE status = 'READY_FOR_PARSE'
                    ORDER BY received_at
                    LIMIT 1
                    """
                )
            ).fetchone()

        if not row:
            context.log.info("No submissions available to parse")
            return "noop"

        if row.status not in {"READY_FOR_PARSE", "PARSED"}:
            context.log.info(
                "Submission %s status=%s is not parseable", row.submission_id, row.status
            )
            return row.submission_id

        file_rows = conn.execute(
            text(
                "SELECT object_key, bytes, etag FROM submission_file WHERE submission_id = :submission_id ORDER BY object_key"
            ),
            {"submission_id": row.submission_id},
        ).fetchall()
        layout = conn.execute(
            text(
                "SELECT layout_id, layout_version, schema_json, parser_config_json FROM layout_registry WHERE layout_id = :layout_id"
            ),
            {"layout_id": row.layout_id},
        ).fetchone()

        parse_run_id = str(uuid.uuid4())
        silver_prefix = f"{SILVER_ROOT}/{row.submitter_id}/{row.file_type}/{row.submission_id}"
        conn.execute(
            text(
                """
                INSERT INTO parse_run(parse_run_id, submission_id, started_at, status, engine, silver_prefix)
                VALUES (:parse_run_id, :submission_id, :started_at, 'STARTED', 'duckdb', :silver_prefix)
                """
            ),
            {
                "parse_run_id": parse_run_id,
                "submission_id": row.submission_id,
                "started_at": _now(),
                "silver_prefix": silver_prefix,
            },
        )

    if not layout:
        error = f"No layout found for submission {row.submission_id} with layout_id={row.layout_id}"
        with context.resources.metadata_db.begin() as conn:
            conn.execute(
                text(
                    "UPDATE parse_run SET ended_at=:ended_at, status='FAILED', error_message=:error WHERE parse_run_id=:parse_run_id"
                ),
                {"ended_at": _now(), "error": error, "parse_run_id": parse_run_id},
            )
            conn.execute(
                text(
                    "UPDATE submission SET status='PARSE_FAILED' WHERE submission_id=:submission_id"
                ),
                {"submission_id": row.submission_id},
            )
        raise RuntimeError(error)

    submission = SubmissionForParse(
        submission_id=row.submission_id,
        submitter_id=row.submitter_id,
        file_type=row.file_type,
        layout_id=layout.layout_id,
        layout_version=layout.layout_version,
        parse_run_id=parse_run_id,
        files=file_rows,
    )

    try:
        report = parse_submission_to_silver(
            submission=submission,
            layout=layout,
            object_store=context.resources.object_store,
            now_fn=_now,
        )
        with context.resources.metadata_db.begin() as conn:
            for file_metric in report.files:
                conn.execute(
                    text(
                        """
                        INSERT INTO parse_file_metrics(
                            parse_run_id, raw_object_key, rows_read, rows_written, rows_rejected, bytes_read, etag
                        ) VALUES (
                            :parse_run_id, :raw_object_key, :rows_read, :rows_written, :rows_rejected, :bytes_read, :etag
                        )
                        """
                    ),
                    {
                        "parse_run_id": parse_run_id,
                        "raw_object_key": file_metric.raw_object_key,
                        "rows_read": file_metric.rows_read,
                        "rows_written": file_metric.rows_written,
                        "rows_rejected": file_metric.rows_rejected,
                        "bytes_read": file_metric.bytes_read,
                        "etag": file_metric.etag,
                    },
                )

            for column_name, invalid_count in report.invalid_counts_by_column.items():
                conn.execute(
                    text(
                        """
                        INSERT INTO parse_column_metrics(parse_run_id, column_name, null_count, invalid_count)
                        VALUES (:parse_run_id, :column_name, :null_count, :invalid_count)
                        """
                    ),
                    {
                        "parse_run_id": parse_run_id,
                        "column_name": column_name,
                        "null_count": 0,
                        "invalid_count": invalid_count,
                    },
                )

            conn.execute(
                text(
                    """
                    UPDATE parse_run
                    SET ended_at=:ended_at, status='SUCCEEDED', report_object_key=:report_object_key
                    WHERE parse_run_id=:parse_run_id
                    """
                ),
                {
                    "ended_at": _now(),
                    "report_object_key": report.report_object_key,
                    "parse_run_id": parse_run_id,
                },
            )
            conn.execute(
                text("UPDATE submission SET status='PARSED' WHERE submission_id=:submission_id"),
                {"submission_id": row.submission_id},
            )

        return row.submission_id
    except Exception as exc:
        with context.resources.metadata_db.begin() as conn:
            conn.execute(
                text(
                    "UPDATE parse_run SET ended_at=:ended_at, status='FAILED', error_message=:error WHERE parse_run_id=:parse_run_id"
                ),
                {"ended_at": _now(), "error": str(exc), "parse_run_id": parse_run_id},
            )
            conn.execute(
                text(
                    "UPDATE submission SET status='PARSE_FAILED' WHERE submission_id=:submission_id"
                ),
                {"submission_id": row.submission_id},
            )
        raise


@graph
def parse_submission_graph():
    parse_submission_op()


parse_submission_job = parse_submission_graph.to_job(name="parse_submission_job")
