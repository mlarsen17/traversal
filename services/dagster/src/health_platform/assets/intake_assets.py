from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dagster import MaterializeResult, asset, job, op
from sqlalchemy import text

from health_platform.intake.classify import classify_submission
from health_platform.intake.fingerprint import fingerprint_group
from health_platform.intake.manifest import ManifestFile, build_manifest, manifest_bytes


def _submission_parts(grouping_key: str, inbox_prefix: str) -> tuple[str, str | None, str | None]:
    trimmed = grouping_key.removeprefix(inbox_prefix.rstrip("/") + "/").strip("/")
    segments = [segment for segment in trimmed.split("/") if segment]
    submitter_id = segments[0] if segments else "unknown"
    hinted_file_type = segments[1] if len(segments) > 1 and segments[1] != "unknown" else None
    drop_id = segments[2] if len(segments) > 2 else None
    return submitter_id, hinted_file_type, drop_id


@asset(group_name="intake", required_resource_keys={"metadata_db"})
def sync_layout_registry(context) -> MaterializeResult:
    now = datetime.now(timezone.utc)
    layout_dir = Path(__file__).resolve().parents[1] / "layouts"
    layout_files = sorted(layout_dir.glob("*.json"))
    synced = 0
    with context.resources.metadata_db.begin() as conn:
        for path in layout_files:
            payload = json.loads(path.read_text())
            file_type = payload["file_type"]
            layout_version = payload["layout_version"]
            conn.execute(
                text(
                    """
                    INSERT INTO file_type (name, description, created_at)
                    VALUES (:name, :description, :created_at)
                    ON CONFLICT(name) DO NOTHING
                    """
                ),
                {"name": file_type, "description": f"seeded from {path.name}", "created_at": now},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO layout_registry (file_type, layout_version, schema_json, parser_config_json, is_active, created_at, updated_at)
                    VALUES (:file_type, :layout_version, :schema_json, :parser_config_json, :is_active, :created_at, :updated_at)
                    ON CONFLICT(file_type, layout_version)
                    DO UPDATE SET schema_json=:schema_json, parser_config_json=:parser_config_json, is_active=:is_active, updated_at=:updated_at
                    """
                ),
                {
                    "file_type": file_type,
                    "layout_version": layout_version,
                    "schema_json": json.dumps(payload.get("schema_json", {})),
                    "parser_config_json": json.dumps(payload.get("parser_config_json", {})),
                    "is_active": True,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            synced += 1
    return MaterializeResult(metadata={"layouts_synced": synced})


@op(required_resource_keys={"metadata_db", "minio"}, config_schema={"grouping_key": str, "grouping_method": str})
def register_submission_from_group(context) -> str:
    engine = context.resources.metadata_db
    object_store = context.resources.minio
    inbox_prefix = context.op_config.get("grouping_key")
    grouping_method = context.op_config.get("grouping_method", "QUIESCENCE")

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT object_key, size_bytes, etag, last_modified
                FROM inbox_object
                WHERE grouping_key = :grouping_key AND status IN ('SEEN', 'CLAIMED', 'MOVED')
                ORDER BY object_key
                """
            ),
            {"grouping_key": inbox_prefix},
        ).mappings().all()

    if not rows:
        context.log.warning("No inbox objects found for grouping key: %s", inbox_prefix)
        return "skipped"

    from health_platform.utils.object_store import ObjectMeta

    def _as_dt(value):
        if hasattr(value, "isoformat"):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    objects = [
        ObjectMeta(
            key=row["object_key"],
            size_bytes=row["size_bytes"],
            etag=row["etag"],
            last_modified=_as_dt(row["last_modified"]),
        )
        for row in rows
    ]

    fp = fingerprint_group(inbox_prefix, objects)
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT submission_id, state, raw_prefix FROM submission WHERE fingerprint = :fp"), {"fp": fp}
        ).mappings().first()
    if existing and existing["state"] in {"READY_FOR_PARSE", "NEEDS_REVIEW"}:
        context.log.info("Fingerprint already registered (%s); no-op", fp)
        return existing["submission_id"]

    root_inbox_prefix = __import__("os").getenv("INBOX_PREFIX", "inbox/")
    raw_prefix = __import__("os").getenv("RAW_PREFIX", "raw/")
    submitter_id, hinted_file_type, _drop_id = _submission_parts(inbox_prefix, root_inbox_prefix)

    classification = classify_submission(submitter_id=submitter_id, object_keys=[obj.key for obj in objects])
    inferred_file_type = classification.file_type or hinted_file_type
    layout_version = classification.layout_version if inferred_file_type else None

    now = datetime.now(timezone.utc)
    submission_id = existing["submission_id"] if existing else str(uuid.uuid4())
    final_file_type = inferred_file_type or "unknown"
    submission_raw_prefix = f"{raw_prefix.rstrip('/')}/{submitter_id}/{final_file_type}/{submission_id}/"

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO submitter (submitter_id, display_name, created_at)
                VALUES (:submitter_id, :display_name, :created_at)
                ON CONFLICT(submitter_id) DO NOTHING
                """
            ),
            {"submitter_id": submitter_id, "display_name": submitter_id, "created_at": now},
        )
        if not existing:
            conn.execute(
                text(
                    """
                    INSERT INTO submission (
                      submission_id, submitter_id, grouping_key, fingerprint, state,
                      inferred_file_type, layout_version, coverage_start_month, coverage_end_month,
                      grouping_method, group_window_start, group_window_end, raw_prefix, created_at, updated_at
                    ) VALUES (
                      :submission_id, :submitter_id, :grouping_key, :fingerprint, :state,
                      :inferred_file_type, :layout_version, :coverage_start_month, :coverage_end_month,
                      :grouping_method, :group_window_start, :group_window_end, :raw_prefix, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "submission_id": submission_id,
                    "submitter_id": submitter_id,
                    "grouping_key": inbox_prefix,
                    "fingerprint": fp,
                    "state": "REGISTERED",
                    "inferred_file_type": inferred_file_type,
                    "layout_version": layout_version,
                    "coverage_start_month": classification.coverage_start_month,
                    "coverage_end_month": classification.coverage_end_month,
                    "grouping_method": grouping_method,
                    "group_window_start": min(obj.last_modified for obj in objects),
                    "group_window_end": max(obj.last_modified for obj in objects),
                    "raw_prefix": submission_raw_prefix,
                    "created_at": now,
                    "updated_at": now,
                },
            )
        conn.execute(
            text("UPDATE inbox_object SET status='CLAIMED', last_seen_at=:now WHERE grouping_key=:grouping_key AND status='SEEN'"),
            {"grouping_key": inbox_prefix, "now": now},
        )

    moved_files: list[ManifestFile] = []
    for obj in objects:
        if obj.key.endswith("/_SUCCESS"):
            continue
        relative = obj.key.removeprefix(inbox_prefix)
        target_key = submission_raw_prefix + relative
        object_store.copy_object(obj.key, target_key)
        object_store.delete_object(obj.key)
        moved_files.append(
            ManifestFile(
                key=target_key,
                size_bytes=obj.size_bytes,
                last_modified=obj.last_modified.isoformat(),
                etag=obj.etag,
            )
        )
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO submission_file (submission_id, object_key, raw_object_key, size_bytes, etag, last_modified, status, created_at)
                    VALUES (:submission_id, :object_key, :raw_object_key, :size_bytes, :etag, :last_modified, :status, :created_at)
                    ON CONFLICT(submission_id, object_key) DO UPDATE SET raw_object_key=:raw_object_key, status=:status
                    """
                ),
                {
                    "submission_id": submission_id,
                    "object_key": obj.key,
                    "raw_object_key": target_key,
                    "size_bytes": obj.size_bytes,
                    "etag": obj.etag,
                    "last_modified": obj.last_modified,
                    "status": "MOVED",
                    "created_at": now,
                },
            )
            conn.execute(
                text("UPDATE inbox_object SET status='MOVED', last_seen_at=:now WHERE object_key=:object_key"),
                {"object_key": obj.key, "now": now},
            )

    final_state = "READY_FOR_PARSE" if inferred_file_type and layout_version else "NEEDS_REVIEW"
    manifest = build_manifest(
        submission_id=submission_id,
        submitter_id=submitter_id,
        state=final_state,
        inferred_file_type=inferred_file_type,
        layout_version=layout_version,
        coverage_start_month=classification.coverage_start_month,
        coverage_end_month=classification.coverage_end_month,
        grouping_method=grouping_method,
        group_window_start=min(obj.last_modified for obj in objects),
        group_window_end=max(obj.last_modified for obj in objects),
        files=moved_files,
    )
    object_store.put_bytes(f"{submission_raw_prefix}_manifest.generated.json", manifest_bytes(manifest))

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE submission SET state=:state, updated_at=:updated_at, raw_prefix=:raw_prefix WHERE submission_id=:submission_id"
            ),
            {
                "submission_id": submission_id,
                "state": final_state,
                "updated_at": datetime.now(timezone.utc),
                "raw_prefix": submission_raw_prefix,
            },
        )

    context.log.info("Submission %s transitioned to %s", submission_id, final_state)
    return submission_id


@job(name="register_submission_from_group_job")
def register_submission_from_group_job():
    register_submission_from_group()
