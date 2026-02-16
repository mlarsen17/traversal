from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from health_platform.intake.constants import INBOX_ROOT, MARKER_FILENAME, RAW_ROOT
from health_platform.intake.filename_conventions import ConventionRegistry, ParsedFilename
from health_platform.intake.object_store import ObjectMetadata, ObjectStore


@dataclass(frozen=True)
class GroupCandidate:
    submitter_id: str
    inbox_prefix: str
    grouping_method: str
    object_keys: list[str]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def discover_inbox_objects(engine, store: ObjectStore, clock=now_utc) -> int:
    ts = clock()
    objs = store.list_objects(f"{INBOX_ROOT}/")
    count = 0
    with engine.begin() as conn:
        for obj in objs:
            parts = obj.key.split("/")
            if len(parts) < 3:
                continue
            submitter_id = parts[1]
            existing = conn.execute(text("SELECT bytes, etag FROM inbox_object WHERE object_key = :object_key"), {"object_key": obj.key}).fetchone()
            if existing:
                changed = existing.bytes != obj.size or (existing.etag or "") != (obj.etag or "")
                conn.execute(
                    text(
                        """
                        UPDATE inbox_object
                        SET last_seen_at=:last_seen_at,
                            last_modified_at=:last_modified_at,
                            bytes=:bytes,
                            etag=:etag,
                            status=CASE WHEN status='MOVED' THEN status ELSE 'SEEN' END
                        WHERE object_key=:object_key
                        """
                    ),
                    {
                        "object_key": obj.key,
                        "last_seen_at": ts,
                        "last_modified_at": obj.last_modified,
                        "bytes": obj.size,
                        "etag": obj.etag,
                    },
                )
                if changed:
                    count += 1
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO inbox_object (object_key, submitter_id, first_seen_at, last_seen_at, last_modified_at, bytes, etag, status)
                        VALUES (:object_key, :submitter_id, :first_seen_at, :last_seen_at, :last_modified_at, :bytes, :etag, 'SEEN')
                        """
                    ),
                    {
                        "object_key": obj.key,
                        "submitter_id": submitter_id,
                        "first_seen_at": ts,
                        "last_seen_at": ts,
                        "last_modified_at": obj.last_modified,
                        "bytes": obj.size,
                        "etag": obj.etag,
                    },
                )
                count += 1
    return count


def _marker_groups(keys: list[str]) -> list[GroupCandidate]:
    markers = [k for k in keys if k.split("/")[-1] == MARKER_FILENAME]
    groups: list[GroupCandidate] = []
    for marker in markers:
        prefix = marker[: -len(MARKER_FILENAME)]
        grouped = [k for k in keys if k.startswith(prefix) and not k.endswith(MARKER_FILENAME)]
        if grouped:
            submitter_id = marker.split("/")[1]
            groups.append(GroupCandidate(submitter_id, prefix.rstrip("/"), "MARKER", sorted(grouped)))
    return groups


def find_closed_groups(engine, quiescence_minutes: int = 10, clock=now_utc) -> list[GroupCandidate]:
    ts = clock()
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT object_key, submitter_id, last_seen_at FROM inbox_object WHERE status='SEEN' ORDER BY object_key")
        ).fetchall()
    keys = [r.object_key for r in rows]
    marker = _marker_groups(keys)
    claimed = {k for g in marker for k in g.object_keys}

    by_submitter: dict[str, list[str]] = {}
    for r in rows:
        if r.object_key.endswith(MARKER_FILENAME) or r.object_key in claimed:
            continue
        last_seen = r.last_seen_at
        if isinstance(last_seen, str):
            last_seen = datetime.fromisoformat(last_seen)
        if ts - last_seen < timedelta(minutes=quiescence_minutes):
            continue
        by_submitter.setdefault(r.submitter_id, []).append(r.object_key)

    for submitter_id, object_keys in by_submitter.items():
        marker.append(GroupCandidate(submitter_id, f"{INBOX_ROOT}/{submitter_id}", "QUIESCENCE", sorted(object_keys)))
    return marker


def _resolve_layout_id(conn, parsed: ParsedFilename) -> str | None:
    if parsed.file_type == "unknown" or not parsed.layout_version:
        return None
    row = conn.execute(
        text(
            "SELECT layout_id FROM layout_registry WHERE file_type=:file_type AND layout_version=:layout_version AND status='ACTIVE'"
        ),
        {"file_type": parsed.file_type, "layout_version": parsed.layout_version},
    ).fetchone()
    return row.layout_id if row else None


def _group_signature(files: list[ObjectMetadata]) -> str:
    payload = "|".join([f"{f.key}:{f.size}:{f.etag or ''}" for f in sorted(files, key=lambda f: f.key)])
    return hashlib.sha256(payload.encode()).hexdigest()


def process_group(
    engine,
    store: ObjectStore,
    registry: ConventionRegistry,
    candidate: GroupCandidate,
    clock=now_utc,
) -> str:
    file_meta = [store.stat_object(k) for k in candidate.object_keys]
    files = [m for m in file_meta if m is not None]
    if not files:
        return "skipped_empty"
    signature = _group_signature(files)

    with engine.begin() as conn:
        existing = conn.execute(text("SELECT submission_id FROM submission WHERE manifest_sha256=:sig"), {"sig": signature}).fetchone()
        if existing:
            return existing.submission_id

        for object_key in candidate.object_keys:
            conn.execute(text("UPDATE inbox_object SET status='CLAIMED' WHERE object_key=:object_key"), {"object_key": object_key})

        parsed_entries = [registry.parse(os.path.basename(f.key)) for f in files]
        parsed = sorted(parsed_entries, key=lambda p: p.confidence, reverse=True)[0]
        status = "READY_FOR_PARSE" if parsed.file_type != "unknown" else "NEEDS_REVIEW"
        submission_id = str(uuid.uuid4())
        raw_prefix = f"{RAW_ROOT}/{candidate.submitter_id}/{parsed.file_type}/{submission_id}"
        manifest_key = f"{raw_prefix}/manifest.generated.json"
        received_at = clock()

        moved_files = []
        for meta in files:
            filename = os.path.basename(meta.key)
            dst_key = f"{raw_prefix}/data/{filename}"
            store.copy_object(meta.key, dst_key)
            store.delete_object(meta.key)
            moved_files.append(
                {
                    "object_key_in_raw": dst_key,
                    "original_object_key": meta.key,
                    "bytes": meta.size,
                    "etag": meta.etag,
                }
            )

        manifest = {
            "submission_id": submission_id,
            "submitter_id": candidate.submitter_id,
            "grouping_method": candidate.grouping_method,
            "received_at": received_at.isoformat(),
            "inbox_prefix": candidate.inbox_prefix,
            "raw_prefix": raw_prefix,
            "inferred": {
                "file_type": parsed.file_type,
                "coverage_start_month": parsed.coverage_start_month,
                "coverage_end_month": parsed.coverage_end_month,
                "layout_version": parsed.layout_version,
            },
            "files": moved_files,
        }
        manifest_bytes = json.dumps(manifest, sort_keys=True, indent=2).encode()
        store.put_bytes(manifest_key, manifest_bytes, content_type="application/json")

        layout_id = _resolve_layout_id(conn, parsed)
        conn.execute(
            text(
                """
                INSERT INTO submission (
                    submission_id, submitter_id, state, file_type, layout_id,
                    coverage_start_month, coverage_end_month, received_at, status,
                    grouping_method, inbox_prefix, raw_prefix, manifest_object_key, manifest_sha256
                ) VALUES (
                    :submission_id, :submitter_id, :state, :file_type, :layout_id,
                    :coverage_start_month, :coverage_end_month, :received_at, :status,
                    :grouping_method, :inbox_prefix, :raw_prefix, :manifest_object_key, :manifest_sha256
                )
                """
            ),
            {
                "submission_id": submission_id,
                "submitter_id": candidate.submitter_id,
                "state": None,
                "file_type": parsed.file_type,
                "layout_id": layout_id,
                "coverage_start_month": parsed.coverage_start_month,
                "coverage_end_month": parsed.coverage_end_month,
                "received_at": received_at,
                "status": status,
                "grouping_method": candidate.grouping_method,
                "inbox_prefix": candidate.inbox_prefix,
                "raw_prefix": raw_prefix,
                "manifest_object_key": manifest_key,
                "manifest_sha256": signature,
            },
        )
        for mf in moved_files:
            conn.execute(
                text(
                    "INSERT INTO submission_file (submission_id, object_key, bytes, etag, sha256) VALUES (:submission_id, :object_key, :bytes, :etag, :sha256)"
                ),
                {
                    "submission_id": submission_id,
                    "object_key": mf["object_key_in_raw"],
                    "bytes": mf["bytes"],
                    "etag": mf["etag"],
                    "sha256": None,
                },
            )

        for object_key in candidate.object_keys:
            conn.execute(text("UPDATE inbox_object SET status='MOVED' WHERE object_key=:object_key"), {"object_key": object_key})
    return submission_id
