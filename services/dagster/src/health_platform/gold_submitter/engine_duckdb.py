from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from health_platform.intake.object_store import ObjectStore


def _extract_partition_month(key: str) -> str | None:
    marker = "atomic_month="
    if marker not in key:
        return None
    return key.split(marker, 1)[1].split("/", 1)[0]


def _list_silver_parquet_keys(object_store: ObjectStore, silver_prefix: str) -> list[str]:
    keys = [
        obj.key
        for obj in object_store.list_objects(f"{silver_prefix}/")
        if obj.key.endswith(".parquet")
    ]
    if not keys:
        raise RuntimeError(f"No silver parquet files found under {silver_prefix}/")
    return keys


def discover_submission_months(object_store: ObjectStore, silver_prefix: str) -> list[str]:
    keys = _list_silver_parquet_keys(object_store, silver_prefix)
    months = sorted({m for key in keys if (m := _extract_partition_month(key)) is not None})
    return months or ["__all__"]


def overwrite_gold_month_partition(
    *,
    object_store: ObjectStore,
    silver_prefix: str,
    gold_month_prefix: str,
    month: str,
    submitter_id: str,
    state: str,
    file_type: str,
    gold_built_at: datetime,
) -> int:
    con = duckdb.connect()
    try:
        all_keys = _list_silver_parquet_keys(object_store, silver_prefix)
        if month == "__all__":
            source_keys = all_keys
        else:
            source_keys = [key for key in all_keys if _extract_partition_month(key) == month]

        with TemporaryDirectory() as tempdir:
            local_paths: list[str] = []
            for idx, key in enumerate(source_keys):
                path = Path(tempdir) / f"silver_{idx}.parquet"
                path.write_bytes(object_store.get_bytes(key))
                local_paths.append(path.as_posix())

            path_list = ", ".join(f"'{p}'" for p in local_paths)
            con.execute(
                f"CREATE OR REPLACE VIEW silver AS SELECT * FROM read_parquet([{path_list}])"
            )
            columns = {row[1] for row in con.execute("PRAGMA table_info('silver')").fetchall()}

            select_exprs = ["*"]
            if "submitter_id" not in columns:
                select_exprs.append(f"'{submitter_id}' AS submitter_id")
            if "state" not in columns:
                select_exprs.append(f"'{state}' AS state")
            if "file_type" not in columns:
                select_exprs.append(f"'{file_type}' AS file_type")
            if "atomic_month" not in columns:
                select_exprs.append(f"'{month}' AS atomic_month")
            select_exprs.append(f"TIMESTAMP '{gold_built_at.isoformat()}' AS gold_built_at")

            row_count = int(con.execute("SELECT COUNT(*) FROM silver").fetchone()[0] or 0)
            output_path = Path(tempdir) / "part-0000.parquet"
            con.execute(
                f"""
                COPY (
                    SELECT {", ".join(select_exprs)}
                    FROM silver
                ) TO '{output_path.as_posix()}' (FORMAT PARQUET)
                """
            )

            for obj in object_store.list_objects(gold_month_prefix):
                object_store.delete_object(obj.key)
            object_store.put_bytes(
                f"{gold_month_prefix.rstrip('/')}/part-0000.parquet",
                output_path.read_bytes(),
                "application/octet-stream",
            )
            return row_count
    finally:
        con.close()
