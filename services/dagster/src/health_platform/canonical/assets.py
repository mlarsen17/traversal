from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dagster import MaterializeResult, asset
from sqlalchemy import text

CANONICAL_ROOT = Path(__file__).resolve().parent
SCHEMA_ROOT = CANONICAL_ROOT / "schemas"
MAPPING_ROOT = CANONICAL_ROOT / "mappings"


def _now() -> datetime:
    return datetime.now(timezone.utc)


@asset(group_name="intake", required_resource_keys={"metadata_db"})
def sync_canonical_registry(context) -> MaterializeResult:
    schemas_loaded = 0
    mappings_loaded = 0

    with context.resources.metadata_db.begin() as conn:
        for path in sorted(SCHEMA_ROOT.glob("*/*.json")):
            payload = json.loads(path.read_text())
            row = conn.execute(
                text(
                    """
                    SELECT canonical_schema_id
                    FROM canonical_schema
                    WHERE file_type=:file_type AND schema_version=:schema_version
                    """
                ),
                {
                    "file_type": payload["file_type"],
                    "schema_version": payload["schema_version"],
                },
            ).fetchone()
            if row:
                schema_id = row.canonical_schema_id
                conn.execute(
                    text(
                        """
                        UPDATE canonical_schema
                        SET columns_json=:columns_json, status=:status
                        WHERE canonical_schema_id=:canonical_schema_id
                        """
                    ),
                    {
                        "canonical_schema_id": schema_id,
                        "columns_json": json.dumps(payload["columns"]),
                        "status": payload.get("status", "ACTIVE"),
                    },
                )
            else:
                schema_id = str(uuid.uuid4())
                conn.execute(
                    text(
                        """
                        INSERT INTO canonical_schema(
                            canonical_schema_id, file_type, schema_version, columns_json, status, created_at
                        ) VALUES (
                            :canonical_schema_id, :file_type, :schema_version, :columns_json, :status, :created_at
                        )
                        """
                    ),
                    {
                        "canonical_schema_id": schema_id,
                        "file_type": payload["file_type"],
                        "schema_version": payload["schema_version"],
                        "columns_json": json.dumps(payload["columns"]),
                        "status": payload.get("status", "ACTIVE"),
                        "created_at": _now(),
                    },
                )
            schemas_loaded += 1

        for path in sorted(MAPPING_ROOT.glob("*/*/*.json")):
            payload = json.loads(path.read_text())
            layout = conn.execute(
                text(
                    """
                    SELECT layout_id
                    FROM layout_registry
                    WHERE file_type=:file_type AND layout_version=:layout_version
                    """
                ),
                {
                    "file_type": payload["file_type"],
                    "layout_version": payload["layout_version"],
                },
            ).fetchone()
            if not layout:
                raise RuntimeError(
                    f"Layout not found for mapping file_type={payload['file_type']} layout_version={payload['layout_version']}"
                )

            schema = conn.execute(
                text(
                    """
                    SELECT canonical_schema_id
                    FROM canonical_schema
                    WHERE file_type=:file_type AND schema_version=:schema_version
                    """
                ),
                {
                    "file_type": payload["file_type"],
                    "schema_version": payload["schema_version"],
                },
            ).fetchone()
            if not schema:
                raise RuntimeError(
                    f"Canonical schema not found for mapping file_type={payload['file_type']} schema_version={payload['schema_version']}"
                )

            row = conn.execute(
                text(
                    """
                    SELECT mapping_id
                    FROM canonical_mapping
                    WHERE layout_id=:layout_id
                      AND canonical_schema_id=:canonical_schema_id
                      AND mapping_version=:mapping_version
                    """
                ),
                {
                    "layout_id": layout.layout_id,
                    "canonical_schema_id": schema.canonical_schema_id,
                    "mapping_version": payload["mapping_version"],
                },
            ).fetchone()
            if row:
                conn.execute(
                    text(
                        """
                        UPDATE canonical_mapping
                        SET mapping_json=:mapping_json, status=:status
                        WHERE mapping_id=:mapping_id
                        """
                    ),
                    {
                        "mapping_id": row.mapping_id,
                        "mapping_json": json.dumps(payload["mapping"]),
                        "status": payload.get("status", "ACTIVE"),
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO canonical_mapping(
                            mapping_id, layout_id, canonical_schema_id, mapping_version,
                            mapping_json, status, created_at
                        ) VALUES (
                            :mapping_id, :layout_id, :canonical_schema_id, :mapping_version,
                            :mapping_json, :status, :created_at
                        )
                        """
                    ),
                    {
                        "mapping_id": str(uuid.uuid4()),
                        "layout_id": layout.layout_id,
                        "canonical_schema_id": schema.canonical_schema_id,
                        "mapping_version": payload["mapping_version"],
                        "mapping_json": json.dumps(payload["mapping"]),
                        "status": payload.get("status", "ACTIVE"),
                        "created_at": _now(),
                    },
                )
            mappings_loaded += 1

    return MaterializeResult(
        metadata={"schemas_loaded": schemas_loaded, "mappings_loaded": mappings_loaded}
    )
