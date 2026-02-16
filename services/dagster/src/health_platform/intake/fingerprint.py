from __future__ import annotations

import hashlib

from health_platform.utils.object_store import ObjectMeta


def fingerprint_group(grouping_key: str, objects: list[ObjectMeta]) -> str:
    parts = [grouping_key]
    for obj in sorted(objects, key=lambda item: item.key):
        id_part = obj.etag or f"{int(obj.last_modified.timestamp())}:{obj.size_bytes}"
        parts.append(f"{obj.key}|{id_part}")
    joined = "\n".join(parts).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()
