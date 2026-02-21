from __future__ import annotations

import uuid
from datetime import datetime, timezone

from dagster import Field, Out, graph, op
from sqlalchemy import text

from health_platform.gold_canonical.engine_duckdb import build_canonical_month


def _now() -> datetime:
    return datetime.now(timezone.utc)


@op(
    required_resource_keys={"metadata_db", "object_store"},
    config_schema={
        "state": Field(str),
        "file_type": Field(str),
        "atomic_month": Field(str),
    },
    out=Out(str),
)
def build_canonical_month_op(context) -> str:
    state = context.op_config["state"]
    file_type = context.op_config["file_type"]
    atomic_month = context.op_config["atomic_month"]

    started_at = _now()
    build_id = str(uuid.uuid4())

    with context.resources.metadata_db.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO canonical_month_build(
                    canonical_month_build_id, state, file_type, atomic_month, canonical_schema_id,
                    started_at, status, input_fingerprint, output_prefix
                ) VALUES (
                    :canonical_month_build_id, :state, :file_type, :atomic_month, '',
                    :started_at, 'STARTED', '', ''
                )
                """
            ),
            {
                "canonical_month_build_id": build_id,
                "state": state,
                "file_type": file_type,
                "atomic_month": atomic_month,
                "started_at": started_at,
            },
        )

    try:
        result = build_canonical_month(
            metadata_db=context.resources.metadata_db,
            object_store=context.resources.object_store,
            state=state,
            file_type=file_type,
            atomic_month=atomic_month,
            logger=context.log,
        )
        with context.resources.metadata_db.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE canonical_month_build
                    SET ended_at=:ended_at,
                        status=:status,
                        canonical_schema_id=:canonical_schema_id,
                        input_fingerprint=:input_fingerprint,
                        output_prefix=:output_prefix
                    WHERE canonical_month_build_id=:canonical_month_build_id
                    """
                ),
                {
                    "ended_at": _now(),
                    "status": result.status,
                    "canonical_schema_id": result.canonical_schema_id,
                    "input_fingerprint": result.input_fingerprint,
                    "output_prefix": result.output_prefix,
                    "canonical_month_build_id": build_id,
                },
            )
            if result.status in {"SUCCEEDED", "SKIPPED"}:
                conn.execute(
                    text(
                        """
                        UPDATE canonical_rebuild_queue
                        SET processed_at=:processed_at
                        WHERE state=:state
                          AND file_type=:file_type
                          AND atomic_month=:atomic_month
                          AND processed_at IS NULL
                        """
                    ),
                    {
                        "processed_at": _now(),
                        "state": state,
                        "file_type": file_type,
                        "atomic_month": atomic_month,
                    },
                )
        return result.status
    except Exception as exc:
        with context.resources.metadata_db.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE canonical_month_build
                    SET ended_at=:ended_at,
                        status='FAILED',
                        error_message=:error_message
                    WHERE canonical_month_build_id=:canonical_month_build_id
                    """
                ),
                {
                    "ended_at": _now(),
                    "error_message": str(exc),
                    "canonical_month_build_id": build_id,
                },
            )
            conn.execute(
                text(
                    """
                    UPDATE canonical_rebuild_queue
                    SET processed_at=:processed_at
                    WHERE state=:state
                      AND file_type=:file_type
                      AND atomic_month=:atomic_month
                      AND processed_at IS NULL
                    """
                ),
                {
                    "processed_at": _now(),
                    "state": state,
                    "file_type": file_type,
                    "atomic_month": atomic_month,
                },
            )
        raise


@graph
def build_canonical_month_graph():
    build_canonical_month_op()


build_canonical_month_job = build_canonical_month_graph.to_job(name="build_canonical_month_job")
