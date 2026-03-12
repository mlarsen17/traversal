from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dagster import MaterializeResult, asset
from sqlalchemy import text

LAYOUT_ROOT = Path(__file__).resolve().parents[1] / "layouts"
ROUTING_ROOT = LAYOUT_ROOT / "routing"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _layout_identity(payload: dict) -> tuple[str, str, str, str]:
    file_type = payload["file_type"]
    return (
        payload.get("submitter_id", "*"),
        file_type,
        payload.get("layout_key", file_type),
        payload["layout_version"],
    )


@asset(group_name="intake", required_resource_keys={"metadata_db"})
def sync_layout_registry(context) -> MaterializeResult:
    loaded = 0
    seen: dict[tuple[str, str, str, str], str] = {}
    with context.resources.metadata_db.begin() as conn:
        for path in sorted(LAYOUT_ROOT.glob("*/*.json")):
            if path.parent.name == "routing":
                continue
            payload = json.loads(path.read_text())
            if "schema" not in payload or "parser_config" not in payload:
                continue

            submitter_id, file_type, layout_key, layout_version = _layout_identity(payload)
            identity = (submitter_id, file_type, layout_key, layout_version)
            existing = seen.get(identity)
            if existing:
                raise RuntimeError(
                    "Duplicate layout identity in definitions: "
                    f"{identity} from {existing} and {path}"
                )
            seen[identity] = path.as_posix()

            row = conn.execute(
                text(
                    """
                    SELECT layout_id
                    FROM layout_registry
                    WHERE submitter_id=:submitter_id
                      AND file_type=:file_type
                      AND layout_key=:layout_key
                      AND layout_version=:layout_version
                    """
                ),
                {
                    "submitter_id": submitter_id,
                    "file_type": file_type,
                    "layout_key": layout_key,
                    "layout_version": layout_version,
                },
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
                            layout_id, submitter_id, file_type, layout_key, layout_version,
                            schema_json, parser_config_json,
                            effective_start_date, effective_end_date, status
                        ) VALUES (
                            :layout_id, :submitter_id, :file_type, :layout_key, :layout_version,
                            :schema_json, :parser_config_json,
                            :effective_start_date, :effective_end_date, :status
                        )
                        """
                    ),
                    {
                        "layout_id": str(uuid.uuid4()),
                        "submitter_id": submitter_id,
                        "file_type": file_type,
                        "layout_key": layout_key,
                        "layout_version": layout_version,
                        "schema_json": json.dumps(payload["schema"]),
                        "parser_config_json": json.dumps(payload["parser_config"]),
                        "effective_start_date": payload.get("effective_start_date"),
                        "effective_end_date": payload.get("effective_end_date"),
                        "status": payload.get("status", "ACTIVE"),
                    },
                )
            loaded += 1
    return MaterializeResult(metadata={"layouts_loaded": loaded})


@asset(group_name="intake", required_resource_keys={"metadata_db"})
def sync_submitter_file_config(context) -> MaterializeResult:
    loaded = 0
    if not ROUTING_ROOT.exists():
        return MaterializeResult(metadata={"routing_rules_loaded": loaded})

    with context.resources.metadata_db.begin() as conn:
        for path in sorted(ROUTING_ROOT.glob("*.json")):
            payload = json.loads(path.read_text())
            rules = payload.get("configs", [])
            for rule in rules:
                submitter_id = rule.get("submitter_id", "*")
                file_type = rule["file_type"]
                default_layout = rule["default_layout"]
                layout_row = conn.execute(
                    text(
                        """
                        SELECT layout_id
                        FROM layout_registry
                        WHERE submitter_id=:submitter_id
                          AND file_type=:file_type
                          AND layout_key=:layout_key
                          AND layout_version=:layout_version
                        """
                    ),
                    {
                        "submitter_id": default_layout.get("submitter_id", submitter_id),
                        "file_type": default_layout.get("file_type", file_type),
                        "layout_key": default_layout.get("layout_key", file_type),
                        "layout_version": default_layout["layout_version"],
                    },
                ).fetchone()
                if not layout_row:
                    raise RuntimeError(
                        "Missing layout for submitter_file_config default pointer: "
                        f"{default_layout} in {path}"
                    )

                row = conn.execute(
                    text(
                        """
                        SELECT routing_config_id
                        FROM submitter_file_config
                        WHERE submitter_id=:submitter_id
                          AND file_type=:file_type
                          AND COALESCE(filename_pattern, '') = COALESCE(:filename_pattern, '')
                          AND COALESCE(file_extension, '') = COALESCE(:file_extension, '')
                          AND priority=:priority
                        """
                    ),
                    {
                        "submitter_id": submitter_id,
                        "file_type": file_type,
                        "filename_pattern": rule.get("filename_pattern"),
                        "file_extension": rule.get("file_extension"),
                        "priority": rule.get("priority", 100),
                    },
                ).fetchone()
                params = {
                    "submitter_id": submitter_id,
                    "file_type": file_type,
                    "filename_pattern": rule.get("filename_pattern"),
                    "file_extension": rule.get("file_extension"),
                    "default_layout_id": layout_row.layout_id,
                    "delimiter": rule.get("delimiter"),
                    "has_header": rule.get("has_header"),
                    "header_signature_json": json.dumps(rule.get("header_signature", {})),
                    "priority": rule.get("priority", 100),
                    "status": rule.get("status", "ACTIVE"),
                    "created_at": _now(),
                }
                if row:
                    conn.execute(
                        text(
                            """
                            UPDATE submitter_file_config
                            SET default_layout_id=:default_layout_id,
                                delimiter=:delimiter,
                                has_header=:has_header,
                                header_signature_json=:header_signature_json,
                                status=:status
                            WHERE routing_config_id=:routing_config_id
                            """
                        ),
                        {**params, "routing_config_id": row.routing_config_id},
                    )
                else:
                    conn.execute(
                        text(
                            """
                            INSERT INTO submitter_file_config(
                                submitter_id, file_type, filename_pattern, file_extension, default_layout_id,
                                delimiter, has_header, header_signature_json, priority, status, created_at
                            ) VALUES (
                                :submitter_id, :file_type, :filename_pattern, :file_extension, :default_layout_id,
                                :delimiter, :has_header, :header_signature_json, :priority, :status, :created_at
                            )
                            """
                        ),
                        params,
                    )
                loaded += 1

    return MaterializeResult(metadata={"routing_rules_loaded": loaded})
