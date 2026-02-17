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
    observed_at = clock()
    discovered = 0
    objects = store.list_objects(f"{INBOX_ROOT}/")

    with engine.begin() as conn:
        for obj in objects:
            parts = obj.key.split("/")
            if len(parts) < 3:
                continue

            submitter_id = parts[1]
            existing = conn.execute(
                text(
                    """
                    SELECT bytes, etag
                    FROM inbox_object
                    WHERE object_key = :object_key
                    """
                ),
                {"object_key": obj.key},
            ).fetchone()

            if existing:
                changed = existing.bytes != obj.size or (existing.etag or "") != (obj.etag or "")
                conn.execute(
                    text(
                        """
                        UPDATE inbox_object
                        SET last_seen_at = :last_seen_at,
                            last_changed_at = CASE WHEN :changed THEN :last_changed_at ELSE last_changed_at END,
                            last_modified_at = :last_modified_at,
                            bytes = :bytes,
                            etag = :etag,
                            status = CASE WHEN status = 'MOVED' THEN status ELSE 'SEEN' END
                        WHERE object_key = :object_key
                        """
                    ),
                    {
                        "object_key": obj.key,
                        "last_seen_at": observed_at,
                        "last_changed_at": observed_at,
                        "last_modified_at": obj.last_modified,
                        "bytes": obj.size,
                        "etag": obj.etag,
                        "changed": changed,
                    },
                )
                if changed:
                    discovered += 1
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO inbox_object (
                            object_key,
                            submitter_id,
                            first_seen_at,
                            last_seen_at,
                            last_changed_at,
                            last_modified_at,
                            bytes,
                            etag,
                            status
                        ) VALUES (
                            :object_key,
                            :submitter_id,
                            :first_seen_at,
                            :last_seen_at,
                            :last_changed_at,
                            :last_modified_at,
                            :bytes,
                            :etag,
                            'SEEN'
                        )
                        """
                    ),
                    {
                        "object_key": obj.key,
                        "submitter_id": submitter_id,
                        "first_seen_at": observed_at,
                        "last_seen_at": observed_at,
                        "last_changed_at": observed_at,
                        "last_modified_at": obj.last_modified,
                        "bytes": obj.size,
                        "etag": obj.etag,
                    },
                )
                discovered += 1

    return discovered


def _marker_groups(keys: list[str]) -> list[GroupCandidate]:
    markers = [key for key in keys if key.split("/")[-1] == MARKER_FILENAME]
    groups: list[GroupCandidate] = []
    for marker_key in markers:
        prefix_with_slash = marker_key[: -len(MARKER_FILENAME)]
        grouped = [
            key
            for key in keys
            if key.startswith(prefix_with_slash) and key.split("/")[-1] != MARKER_FILENAME
        ]
        if not grouped:
            continue
        submitter_id = marker_key.split("/")[1]
        groups.append(
            GroupCandidate(
                submitter_id=submitter_id,
                inbox_prefix=prefix_with_slash.rstrip("/"),
                grouping_method="MARKER",
                object_keys=sorted(grouped),
            )
        )
    return groups


def _parse_ts(ts):
    if ts is None or isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(ts)


def find_closed_groups(engine, quiescence_minutes: int = 10, clock=now_utc) -> list[GroupCandidate]:
    threshold = clock() - timedelta(minutes=quiescence_minutes)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT object_key, submitter_id, last_seen_at, last_changed_at
                FROM inbox_object
                WHERE status = 'SEEN'
                ORDER BY object_key
                """
            )
        ).fetchall()

    keys = [row.object_key for row in rows]
    groups = _marker_groups(keys)
    claimed_keys = {key for group in groups for key in group.object_keys}

    by_submitter: dict[str, list[str]] = {}
    for row in rows:
        if row.object_key.split("/")[-1] == MARKER_FILENAME or row.object_key in claimed_keys:
            continue
        last_changed_at = _parse_ts(row.last_changed_at) or _parse_ts(row.last_seen_at)
        if not last_changed_at or last_changed_at > threshold:
            continue
        by_submitter.setdefault(row.submitter_id, []).append(row.object_key)

    for submitter_id, object_keys in by_submitter.items():
        groups.append(
            GroupCandidate(
                submitter_id=submitter_id,
                inbox_prefix=f"{INBOX_ROOT}/{submitter_id}",
                grouping_method="QUIESCENCE",
                object_keys=sorted(object_keys),
            )
        )

    return groups


def _resolve_layout_id(conn, parsed: ParsedFilename) -> str | None:
    if parsed.file_type == "unknown" or not parsed.layout_version:
        return None
    row = conn.execute(
        text(
            """
            SELECT layout_id
            FROM layout_registry
            WHERE file_type = :file_type
              AND layout_version = :layout_version
              AND status = 'ACTIVE'
            """
        ),
        {"file_type": parsed.file_type, "layout_version": parsed.layout_version},
    ).fetchone()
    return row.layout_id if row else None


def _group_fingerprint(prefix: str, files: list[ObjectMetadata]) -> str:
    file_parts = [
        f"{meta.key}:{meta.size}:{meta.etag or ''}"
        for meta in sorted(files, key=lambda value: value.key)
    ]
    payload = f"{prefix}|" + "|".join(file_parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def process_group(
    engine,
    store: ObjectStore,
    registry: ConventionRegistry,
    candidate: GroupCandidate,
    clock=now_utc,
) -> str:
    file_meta = [store.stat_object(key) for key in candidate.object_keys]
    files = [meta for meta in file_meta if meta is not None]
    if not files:
        return "skipped_empty"

    group_fingerprint = _group_fingerprint(candidate.inbox_prefix, files)

    with engine.begin() as conn:
        existing_submission = conn.execute(
            text(
                """
                SELECT submission_id
                FROM submission
                WHERE group_fingerprint = :group_fingerprint
                """
            ),
            {"group_fingerprint": group_fingerprint},
        ).fetchone()
        if existing_submission:
            return existing_submission.submission_id

        for object_key in candidate.object_keys:
            conn.execute(
                text(
                    """
                    UPDATE inbox_object
                    SET status = 'CLAIMED'
                    WHERE object_key = :object_key
                      AND status IN ('SEEN', 'CLAIMED')
                    """
                ),
                {"object_key": object_key},
            )

        parsed_entries = [registry.parse(os.path.basename(meta.key)) for meta in files]
        parsed = sorted(parsed_entries, key=lambda entry: entry.confidence, reverse=True)[0]
        received_at = clock()
        submission_id = str(uuid.uuid4())
        submission_status = "READY_FOR_PARSE" if parsed.file_type != "unknown" else "NEEDS_REVIEW"

        raw_prefix = f"{RAW_ROOT}/{candidate.submitter_id}/{parsed.file_type}/{submission_id}"
        manifest_key = f"{raw_prefix}/manifest.generated.json"

        conn.execute(
            text(
                """
                INSERT INTO submission (
                    submission_id,
                    submitter_id,
                    state,
                    file_type,
                    layout_id,
                    coverage_start_month,
                    coverage_end_month,
                    received_at,
                    status,
                    grouping_method,
                    inbox_prefix,
                    raw_prefix,
                    manifest_object_key,
                    manifest_sha256,
                    group_fingerprint
                ) VALUES (
                    :submission_id,
                    :submitter_id,
                    :state,
                    :file_type,
                    :layout_id,
                    :coverage_start_month,
                    :coverage_end_month,
                    :received_at,
                    :status,
                    :grouping_method,
                    :inbox_prefix,
                    :raw_prefix,
                    :manifest_object_key,
                    :manifest_sha256,
                    :group_fingerprint
                )
                """
            ),
            {
                "submission_id": submission_id,
                "submitter_id": candidate.submitter_id,
                "state": None,
                "file_type": parsed.file_type,
                "layout_id": _resolve_layout_id(conn, parsed),
                "coverage_start_month": parsed.coverage_start_month,
                "coverage_end_month": parsed.coverage_end_month,
                "received_at": received_at,
                "status": "RECEIVED",
                "grouping_method": candidate.grouping_method,
                "inbox_prefix": candidate.inbox_prefix,
                "raw_prefix": raw_prefix,
                "manifest_object_key": manifest_key,
                "manifest_sha256": "",
                "group_fingerprint": group_fingerprint,
            },
        )

        moved_files: list[dict[str, str | int | None]] = []
        for meta in sorted(files, key=lambda value: value.key):
            destination_key = f"{raw_prefix}/data/{os.path.basename(meta.key)}"
            store.copy_object(meta.key, destination_key)
            copied = store.stat_object(destination_key)
            if copied is None:
                raise RuntimeError(f"Copy verification failed for {destination_key}")
            moved_files.append(
                {
                    "object_key_in_raw": destination_key,
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

        manifest_bytes = json.dumps(manifest, sort_keys=True, indent=2).encode("utf-8")
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

        existing_manifest = conn.execute(
            text(
                """
                SELECT submission_id
                FROM submission
                WHERE manifest_sha256 = :manifest_sha256
                  AND manifest_sha256 != ''
                """
            ),
            {"manifest_sha256": manifest_sha256},
        ).fetchone()
        if existing_manifest:
            conn.execute(
                text("DELETE FROM submission WHERE submission_id = :submission_id"),
                {"submission_id": submission_id},
            )
            return existing_manifest.submission_id

        store.put_bytes(manifest_key, manifest_bytes, content_type="application/json")
        if store.stat_object(manifest_key) is None:
            raise RuntimeError(f"Manifest verification failed for {manifest_key}")

        for object_key in candidate.object_keys:
            store.delete_object(object_key)

        for moved_file in moved_files:
            conn.execute(
                text(
                    """
                    INSERT INTO submission_file (submission_id, object_key, bytes, etag, sha256)
                    VALUES (:submission_id, :object_key, :bytes, :etag, :sha256)
                    """
                ),
                {
                    "submission_id": submission_id,
                    "object_key": moved_file["object_key_in_raw"],
                    "bytes": moved_file["bytes"],
                    "etag": moved_file["etag"],
                    "sha256": None,
                },
            )

        conn.execute(
            text(
                """
                UPDATE submission
                SET status = :status,
                    manifest_sha256 = :manifest_sha256
                WHERE submission_id = :submission_id
                """
            ),
            {
                "status": submission_status,
                "manifest_sha256": manifest_sha256,
                "submission_id": submission_id,
            },
        )

        for object_key in candidate.object_keys:
            conn.execute(
                text("UPDATE inbox_object SET status = 'MOVED' WHERE object_key = :object_key"),
                {"object_key": object_key},
            )

        marker_key = f"{candidate.inbox_prefix}/{MARKER_FILENAME}"
        if candidate.grouping_method == "MARKER" and store.stat_object(marker_key) is not None:
            store.delete_object(marker_key)

    return submission_id
