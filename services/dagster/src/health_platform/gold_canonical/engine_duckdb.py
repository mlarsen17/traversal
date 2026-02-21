from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from sqlalchemy import text

from health_platform.intake.object_store import ObjectStore


@dataclass(frozen=True)
class CanonicalBuildResult:
    status: str
    canonical_schema_id: str
    schema_version: str
    input_fingerprint: str
    output_prefix: str
    row_count: int


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _input_fingerprint(
    *,
    winners: list,
    canonical_schema_id: str,
    schema_version: str,
) -> str:
    parts = [f"schema:{canonical_schema_id}:{schema_version}"]
    for row in sorted(
        winners,
        key=lambda r: (
            r.submitter_id,
            r.winning_submission_id,
            r.layout_id,
            r.mapping_id,
            r.mapping_version,
        ),
    ):
        parts.append(
            "|".join(
                [
                    row.submitter_id,
                    row.winning_submission_id,
                    row.layout_id,
                    row.mapping_id,
                    row.mapping_version,
                ]
            )
        )
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def _required_mapping_expression(canonical_col: str, rule: dict, available_cols: set[str]) -> str:
    keys = {k for k in ("source", "expr", "const", "coalesce") if k in rule}
    if len(keys) != 1:
        raise RuntimeError(
            f"Invalid mapping for '{canonical_col}': expected exactly one mapping form"
        )

    if "source" in rule:
        source = rule["source"]
        if source not in available_cols:
            raise RuntimeError(f"Missing source column '{source}' for canonical '{canonical_col}'")
        expr = f'"{source}"'
        if rule.get("cast"):
            expr = f"TRY_CAST({expr} AS {rule['cast']})"
        return expr

    if "expr" in rule:
        return str(rule["expr"])

    if "const" in rule:
        const_value = str(rule["const"])
        return f"'{_escape_sql_string(const_value)}'"

    cols = rule["coalesce"]
    if not isinstance(cols, list) or not cols:
        raise RuntimeError(f"Invalid coalesce mapping for '{canonical_col}'")
    missing = [c for c in cols if c not in available_cols]
    if len(missing) == len(cols):
        raise RuntimeError(
            f"Missing all coalesce source columns {missing} for canonical '{canonical_col}'"
        )
    expressions = [f'"{c}"' for c in cols if c in available_cols]
    return f"COALESCE({', '.join(expressions)})"


def build_canonical_month(
    *,
    metadata_db,
    object_store: ObjectStore,
    state: str,
    file_type: str,
    atomic_month: str,
    logger,
) -> CanonicalBuildResult:
    with metadata_db.begin() as conn:
        schema_row = conn.execute(
            text(
                """
                SELECT canonical_schema_id, schema_version, columns_json
                FROM canonical_schema
                WHERE file_type=:file_type AND status='ACTIVE'
                ORDER BY schema_version DESC
                LIMIT 1
                """
            ),
            {"file_type": file_type},
        ).fetchone()
        if not schema_row:
            raise RuntimeError(f"No ACTIVE canonical schema found for file_type={file_type}")

        winners = conn.execute(
            text(
                """
                WITH mapping_ranked AS (
                    SELECT
                        mapping_id,
                        layout_id,
                        canonical_schema_id,
                        mapping_version,
                        mapping_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY layout_id, canonical_schema_id
                            ORDER BY mapping_version DESC, created_at DESC
                        ) AS rn
                    FROM canonical_mapping
                    WHERE status='ACTIVE'
                )
                SELECT
                    p.submitter_id,
                    p.winning_submission_id,
                    s.layout_id,
                    m.mapping_id,
                    m.mapping_version,
                    m.mapping_json
                FROM submitter_month_pointer p
                JOIN submission s ON s.submission_id = p.winning_submission_id
                JOIN mapping_ranked m
                  ON m.layout_id = s.layout_id
                 AND m.canonical_schema_id = :canonical_schema_id
                 AND m.rn = 1
                WHERE p.state=:state AND p.file_type=:file_type AND p.atomic_month=:atomic_month
                ORDER BY p.submitter_id
                """
            ),
            {
                "state": state,
                "file_type": file_type,
                "atomic_month": atomic_month,
                "canonical_schema_id": schema_row.canonical_schema_id,
            },
        ).fetchall()

        if not winners:
            raise RuntimeError(
                f"No submitter winners found for state={state} file_type={file_type} month={atomic_month}"
            )

        columns = json.loads(schema_row.columns_json)
        fingerprint = _input_fingerprint(
            winners=winners,
            canonical_schema_id=schema_row.canonical_schema_id,
            schema_version=schema_row.schema_version,
        )

        previous = conn.execute(
            text(
                """
                SELECT input_fingerprint
                FROM canonical_month_build
                WHERE state=:state
                  AND file_type=:file_type
                  AND atomic_month=:atomic_month
                  AND canonical_schema_id=:canonical_schema_id
                  AND status='SUCCEEDED'
                ORDER BY ended_at DESC
                LIMIT 1
                """
            ),
            {
                "state": state,
                "file_type": file_type,
                "atomic_month": atomic_month,
                "canonical_schema_id": schema_row.canonical_schema_id,
            },
        ).scalar_one_or_none()

        output_prefix = f"gold_canonical/{state}/{file_type}/atomic_month={atomic_month}"
        if previous == fingerprint:
            logger.info("Skipping canonical build due to unchanged fingerprint")
            return CanonicalBuildResult(
                status="SKIPPED",
                canonical_schema_id=schema_row.canonical_schema_id,
                schema_version=schema_row.schema_version,
                input_fingerprint=fingerprint,
                output_prefix=output_prefix,
                row_count=0,
            )

    con = duckdb.connect()
    try:
        with TemporaryDirectory() as tempdir:
            union_queries: list[str] = []

            for idx, winner in enumerate(winners):
                source_prefix = (
                    f"gold_submitter/{state}/{file_type}/{winner.submitter_id}/"
                    f"atomic_month={atomic_month}"
                )
                view_name = f"src_{idx}"
                local_paths: list[str] = []
                for key_idx, obj in enumerate(object_store.list_objects(source_prefix)):
                    if not obj.key.endswith(".parquet"):
                        continue
                    local = Path(tempdir) / f"winner_{idx}_{key_idx}.parquet"
                    local.write_bytes(object_store.get_bytes(obj.key))
                    local_paths.append(local.as_posix())
                if not local_paths:
                    raise RuntimeError(
                        f"No gold_submitter parquet files found under {source_prefix}"
                    )

                path_list = ", ".join(f"'{p}'" for p in local_paths)
                con.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet([{path_list}])"
                )
                table_cols = {
                    row[1] for row in con.execute(f"PRAGMA table_info('{view_name}')").fetchall()
                }

                mapping = json.loads(winner.mapping_json)
                select_exprs: list[str] = []
                for col in columns:
                    col_name = col["name"]
                    rule = mapping.get(col_name)
                    if rule is None:
                        if col.get("required"):
                            raise RuntimeError(
                                f"Mapping missing required canonical column '{col_name}' for layout {winner.layout_id}"
                            )
                        select_exprs.append(f"NULL::{col['type']} AS {col_name}")
                        continue
                    expr = _required_mapping_expression(col_name, rule, table_cols)
                    select_exprs.append(f"{expr}::{col['type']} AS {col_name}")

                built_at = _now().isoformat()
                select_exprs.extend(
                    [
                        f"'{_escape_sql_string(state)}' AS state",
                        f"'{_escape_sql_string(file_type)}' AS file_type",
                        f"'{_escape_sql_string(atomic_month)}' AS atomic_month",
                        f"'{_escape_sql_string(winner.submitter_id)}' AS submitter_id",
                        f"'{_escape_sql_string(winner.winning_submission_id)}' AS source_submission_id",
                        f"'{_escape_sql_string(winner.layout_id)}' AS source_layout_id",
                        f"'{_escape_sql_string(schema_row.schema_version)}' AS canonical_schema_version",
                        f"TIMESTAMP '{_escape_sql_string(built_at)}' AS canonical_built_at",
                    ]
                )
                union_queries.append(f"SELECT {', '.join(select_exprs)} FROM {view_name}")

            union_sql = " UNION ALL ".join(union_queries)
            row_count = int(con.execute(f"SELECT COUNT(*) FROM ({union_sql}) t").fetchone()[0] or 0)

            output_path = Path(tempdir) / "part-0000.parquet"
            con.execute(f"COPY ({union_sql}) TO '{output_path.as_posix()}' (FORMAT PARQUET)")

            for obj in object_store.list_objects(output_prefix):
                object_store.delete_object(obj.key)
            object_store.put_bytes(
                f"{output_prefix}/part-{uuid.uuid4().hex[:12]}.parquet",
                output_path.read_bytes(),
                "application/octet-stream",
            )

            return CanonicalBuildResult(
                status="SUCCEEDED",
                canonical_schema_id=schema_row.canonical_schema_id,
                schema_version=schema_row.schema_version,
                input_fingerprint=fingerprint,
                output_prefix=output_prefix,
                row_count=row_count,
            )
    finally:
        con.close()
