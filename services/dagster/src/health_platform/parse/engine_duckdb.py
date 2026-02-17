from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from health_platform.intake.constants import SILVER_ROOT
from health_platform.intake.object_store import ObjectStore


@dataclass(frozen=True)
class ParseFileMetric:
    raw_object_key: str
    rows_read: int
    rows_written: int
    rows_rejected: int
    bytes_read: int | None
    etag: str | None


@dataclass(frozen=True)
class ParseColumnMetric:
    column_name: str
    null_count: int
    invalid_count: int


@dataclass(frozen=True)
class ParseReport:
    submission_id: str
    parse_run_id: str
    engine: str
    started_at: str
    ended_at: str
    files: list[ParseFileMetric]
    invalid_counts_by_column: dict[str, int]
    column_metrics: dict[str, ParseColumnMetric]
    output_partition_counts: dict[str, int]
    warnings: list[str]
    unparseable_anchor_dates: int
    report_object_key: str


def _safe_name(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _duck_type(column_type: str) -> str:
    mapping = {
        "string": "VARCHAR",
        "integer": "BIGINT",
        "decimal": "DOUBLE",
        "date": "DATE",
        "datetime": "TIMESTAMP",
    }
    return mapping.get(column_type, "VARCHAR")


def _typed_expr(raw_col: str, column_type: str, date_formats: list[str]) -> str:
    cleaned = f"NULLIF(TRIM(CAST({_safe_name(raw_col)} AS VARCHAR)), '')"
    if column_type in {"date", "datetime"}:
        parses = [f"TRY_STRPTIME({cleaned}, '{fmt}')" for fmt in date_formats]
        parses.append(f"TRY_CAST({cleaned} AS TIMESTAMP)")
        base = f"COALESCE({', '.join(parses)})"
        if column_type == "date":
            return f"CAST({base} AS DATE)"
        return base
    return f"TRY_CAST({cleaned} AS {_duck_type(column_type)})"


def _configure_s3(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("LOAD httpfs")
    except Exception:
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except Exception:
            return False
    con.execute(f"SET s3_region='{os.getenv('S3_REGION', 'us-east-1')}'")
    con.execute(f"SET s3_access_key_id='{os.getenv('S3_ACCESS_KEY_ID', '')}'")
    con.execute(f"SET s3_secret_access_key='{os.getenv('S3_SECRET_ACCESS_KEY', '')}'")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        cleaned = endpoint.replace("http://", "").replace("https://", "")
        use_ssl = endpoint.startswith("https://")
        con.execute(f"SET s3_endpoint='{cleaned}'")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
        con.execute("SET s3_url_style='path'")
    return True


def parse_submission_to_silver(
    submission, layout, object_store: ObjectStore, now_fn
) -> ParseReport:
    parser_config = json.loads(layout.parser_config_json)
    schema = json.loads(layout.schema_json)
    columns = schema["columns"]
    date_formats = parser_config.get("date_formats") or [schema.get("date_format", "%Y-%m-%d")]
    anchor_col = schema.get("anchor_date_column")

    silver_prefix = (
        f"{SILVER_ROOT}/{submission.submitter_id}/{submission.file_type}/{submission.submission_id}"
    )
    for obj in object_store.list_objects(f"{silver_prefix}/"):
        object_store.delete_object(obj.key)

    started = now_fn()
    file_rows = []
    for row in submission.files:
        meta = object_store.stat_object(row.object_key)
        if meta is None:
            continue
        file_rows.append(
            {"raw_object_key": row.object_key, "bytes_read": meta.size, "etag": meta.etag}
        )

    if not file_rows:
        raise RuntimeError(f"No raw files found for submission {submission.submission_id}")

    report_key = f"{silver_prefix}/parse_report.json"
    all_col_metrics: dict[str, ParseColumnMetric] = {}
    warnings: list[str] = []
    partition_counts: dict[str, int] = {}

    with TemporaryDirectory() as tempdir:
        con = duckdb.connect()
        s3_enabled = _configure_s3(con)
        con.execute(
            "CREATE TEMP TABLE parsed_union AS SELECT * FROM (SELECT 1 AS _dummy) WHERE 1=0"
        )

        for idx, file_meta in enumerate(file_rows):
            s3_path = f"s3://{os.getenv('S3_BUCKET', 'health-raw')}/{file_meta['raw_object_key']}"
            if not s3_enabled:
                staged_path = Path(tempdir) / f"input_{idx}.csv"
                staged_path.write_bytes(object_store.get_bytes(file_meta["raw_object_key"]))
                s3_path = staged_path.as_posix()
            delimiter = parser_config.get("delimiter", ",")
            header = str(parser_config.get("header", True)).lower()
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE raw_input AS
                SELECT *, filename AS _filename
                FROM read_csv_auto('{s3_path}', delim='{delimiter}', header={header}, ignore_errors=true, filename=true)
                """
            )

            select_exprs = []
            invalid_row_terms = []
            raw_cols = {row[1] for row in con.execute("PRAGMA table_info(raw_input)").fetchall()}
            invalid_counts_by_column: dict[str, int] = {}
            for col in columns:
                if col["name"] in raw_cols:
                    expr = _typed_expr(col["name"], col["type"], date_formats)
                    col_name = _safe_name(col["name"])
                    invalid = f"CASE WHEN NULLIF(TRIM(CAST({col_name} AS VARCHAR)), '') IS NOT NULL AND {expr} IS NULL THEN 1 ELSE 0 END"
                    invalid_row_terms.append(invalid)
                    invalid_count = con.execute(
                        f"SELECT SUM({invalid}) FROM raw_input"
                    ).fetchone()[0]
                else:
                    expr = "NULL"
                    invalid_count = 0

                col_name = _safe_name(col["name"])
                select_exprs.append(f"{expr} AS {col_name}")
                invalid_counts_by_column[col["name"]] = int(invalid_count or 0)

            anchor_expr = "'__unknown__'"
            if anchor_col and anchor_col in raw_cols:
                anchor_parse = _typed_expr(anchor_col, "datetime", date_formats)
                anchor_expr = (
                    f"CASE WHEN {anchor_parse} IS NULL THEN '__unknown__' "
                    f"ELSE STRFTIME({anchor_parse}, '%Y-%m') END"
                )

            cast_select = ",\n".join(select_exprs)
            row_rejected_expr = " + ".join(invalid_row_terms) if invalid_row_terms else "0"
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE parsed_file AS
                SELECT
                    {cast_select},
                    '{submission.submission_id}' AS submission_id,
                    '{file_meta["raw_object_key"]}' AS source_object_key,
                    TIMESTAMP '{started.isoformat()}' AS ingested_at,
                    '{layout.layout_id}' AS layout_id,
                    '{layout.layout_version}' AS layout_version,
                    {anchor_expr} AS atomic_month,
                    ({row_rejected_expr}) > 0 AS _row_rejected
                FROM raw_input
                """
            )

            for col in columns:
                col_name = _safe_name(col["name"])
                null_count = con.execute(
                    f"""
                    SELECT
                        SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END)
                    FROM parsed_file
                    WHERE NOT _row_rejected
                    """
                ).fetchone()[0]
                existing = all_col_metrics.get(col["name"])
                all_col_metrics[col["name"]] = ParseColumnMetric(
                    column_name=col["name"],
                    null_count=(existing.null_count if existing else 0) + int(null_count or 0),
                    invalid_count=(existing.invalid_count if existing else 0)
                    + int(invalid_counts_by_column[col["name"]] or 0),
                )

            rows_read, rows_rejected = con.execute(
                "SELECT COUNT(*), SUM(CASE WHEN _row_rejected THEN 1 ELSE 0 END) FROM parsed_file"
            ).fetchone()
            rows_written = con.execute(
                "SELECT COUNT(*) FROM parsed_file WHERE NOT _row_rejected"
            ).fetchone()[0]
            con.execute(
                "INSERT INTO parsed_union SELECT * FROM parsed_file"
                if idx
                else "CREATE OR REPLACE TEMP TABLE parsed_union AS SELECT * FROM parsed_file"
            )
            file_meta["rows_read"] = int(rows_read or 0)
            file_meta["rows_rejected"] = int(rows_rejected or 0)
            file_meta["rows_written"] = rows_written

        out_dir = Path(tempdir) / "silver"
        con.execute(
            f"""
            COPY (
              SELECT * EXCLUDE (_row_rejected) FROM parsed_union WHERE NOT _row_rejected
            ) TO '{out_dir.as_posix()}' (FORMAT PARQUET, PARTITION_BY (atomic_month))
            """
        )

        unknown_count = con.execute(
            "SELECT COUNT(*) FROM parsed_union WHERE atomic_month = '__unknown__' AND NOT _row_rejected"
        ).fetchone()[0]
        part_rows = con.execute(
            "SELECT atomic_month, COUNT(*) FROM parsed_union WHERE NOT _row_rejected GROUP BY atomic_month"
        ).fetchall()
        partition_counts = {str(month): int(count) for month, count in part_rows}
        if unknown_count:
            warnings.append(f"{unknown_count} rows had unparseable anchor dates")
        if not s3_enabled:
            warnings.append("DuckDB httpfs unavailable; parser used local staged reads")

        for path in out_dir.rglob("*.parquet"):
            rel = path.relative_to(out_dir).as_posix()
            object_store.put_bytes(
                f"{silver_prefix}/{rel}", path.read_bytes(), "application/octet-stream"
            )

    ended = now_fn()
    report = ParseReport(
        submission_id=submission.submission_id,
        parse_run_id=submission.parse_run_id,
        engine="duckdb",
        started_at=started.isoformat(),
        ended_at=ended.isoformat(),
        files=[ParseFileMetric(**item) for item in file_rows],
        invalid_counts_by_column={k: v.invalid_count for k, v in all_col_metrics.items()},
        column_metrics=all_col_metrics,
        output_partition_counts=partition_counts,
        warnings=warnings,
        unparseable_anchor_dates=int(partition_counts.get("__unknown__", 0)),
        report_object_key=report_key,
    )
    object_store.put_bytes(
        report_key, json.dumps(asdict(report), indent=2).encode("utf-8"), "application/json"
    )
    return report
