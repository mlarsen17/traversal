from __future__ import annotations

import json
import uuid
from pathlib import Path

from dagster import MaterializeResult, asset
from sqlalchemy import text

LAYOUT_ROOT = Path(__file__).resolve().parents[1] / "layouts"


@asset(group_name="intake", required_resource_keys={"metadata_db"})
def sync_layout_registry(context) -> MaterializeResult:
    loaded = 0
    with context.resources.metadata_db.begin() as conn:
        for path in sorted(LAYOUT_ROOT.glob("*/*.json")):
            payload = json.loads(path.read_text())
            row = conn.execute(
                text(
                    "SELECT layout_id FROM layout_registry WHERE file_type=:file_type AND layout_version=:layout_version"
                ),
                {"file_type": payload["file_type"], "layout_version": payload["layout_version"]},
            ).fetchone()
            if row:
                layout_id = row.layout_id
                conn.execute(
                    text(
                        """
                        UPDATE layout_registry
                        SET schema_json=:schema_json, parser_config_json=:parser_config_json,
                            effective_start_date=:effective_start_date, effective_end_date=:effective_end_date,
                            status=:status
                        WHERE layout_id=:layout_id
                        """
                    ),
                    {
                        "layout_id": layout_id,
                        "schema_json": json.dumps(payload["schema"]),
                        "parser_config_json": json.dumps(payload["parser_config"]),
                        "effective_start_date": payload.get("effective_start_date"),
                        "effective_end_date": payload.get("effective_end_date"),
                        "status": payload.get("status", "ACTIVE"),
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO layout_registry (
                            layout_id, file_type, layout_version, schema_json, parser_config_json,
                            effective_start_date, effective_end_date, status
                        ) VALUES (
                            :layout_id, :file_type, :layout_version, :schema_json, :parser_config_json,
                            :effective_start_date, :effective_end_date, :status
                        )
                        """
                    ),
                    {
                        "layout_id": str(uuid.uuid4()),
                        "file_type": payload["file_type"],
                        "layout_version": payload["layout_version"],
                        "schema_json": json.dumps(payload["schema"]),
                        "parser_config_json": json.dumps(payload["parser_config"]),
                        "effective_start_date": payload.get("effective_start_date"),
                        "effective_end_date": payload.get("effective_end_date"),
                        "status": payload.get("status", "ACTIVE"),
                    },
                )
            loaded += 1
    return MaterializeResult(metadata={"layouts_loaded": loaded})
