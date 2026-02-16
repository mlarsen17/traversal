from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from health_platform.utils.object_store import ObjectMeta


@dataclass
class GroupCandidate:
    grouping_key: str
    grouping_method: str
    objects: list[ObjectMeta]
    window_start: datetime
    window_end: datetime


def _segments_after_prefix(key: str, prefix: str) -> list[str]:
    normalized = prefix.rstrip("/") + "/"
    trimmed = key[len(normalized) :] if key.startswith(normalized) else key
    return [segment for segment in trimmed.split("/") if segment]


def grouping_key_for_object(key: str, inbox_prefix: str, group_by_depth: int) -> str:
    segments = _segments_after_prefix(key, inbox_prefix)
    group_segments = segments[:group_by_depth]
    return inbox_prefix.rstrip("/") + "/" + "/".join(group_segments) + "/"


def bucket_by_group(objects: list[ObjectMeta], inbox_prefix: str, group_by_depth: int) -> dict[str, list[ObjectMeta]]:
    grouped: dict[str, list[ObjectMeta]] = defaultdict(list)
    for obj in objects:
        grouped[grouping_key_for_object(obj.key, inbox_prefix, group_by_depth)].append(obj)
    return dict(grouped)


def is_closed_group(grouping_key: str, objects: list[ObjectMeta], now: datetime, quiescence_minutes: int) -> tuple[bool, str | None]:
    marker_key = grouping_key.rstrip("/") + "/_SUCCESS"
    if any(obj.key == marker_key for obj in objects):
        return True, "MARKER_FILE"

    quiescence_cutoff = now - timedelta(minutes=quiescence_minutes)
    if objects and all(obj.last_modified <= quiescence_cutoff for obj in objects):
        return True, "QUIESCENCE"

    return False, None


def discover_closed_groups(objects: list[ObjectMeta]) -> list[GroupCandidate]:
    inbox_prefix = os.getenv("INBOX_PREFIX", "inbox/")
    group_by_depth = int(os.getenv("GROUP_BY_DEPTH", "3"))
    quiescence_minutes = int(os.getenv("QUIESCENCE_MINUTES", "15"))
    now = datetime.now(timezone.utc)

    grouped = bucket_by_group(objects, inbox_prefix=inbox_prefix, group_by_depth=group_by_depth)
    candidates: list[GroupCandidate] = []
    for grouping_key, group_objects in grouped.items():
        closed, method = is_closed_group(grouping_key, group_objects, now, quiescence_minutes)
        if not closed:
            continue
        ordered = sorted(group_objects, key=lambda item: item.last_modified)
        candidates.append(
            GroupCandidate(
                grouping_key=grouping_key,
                grouping_method=method or "QUIESCENCE",
                objects=sorted(group_objects, key=lambda item: item.key),
                window_start=ordered[0].last_modified,
                window_end=ordered[-1].last_modified,
            )
        )
    return sorted(candidates, key=lambda item: item.grouping_key)
