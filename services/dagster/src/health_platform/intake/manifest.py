from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class ManifestFile:
    key: str
    size_bytes: int
    last_modified: str
    etag: str | None


def build_manifest(
    submission_id: str,
    submitter_id: str,
    state: str,
    inferred_file_type: str | None,
    layout_version: str | None,
    coverage_start_month: str | None,
    coverage_end_month: str | None,
    grouping_method: str,
    group_window_start: datetime,
    group_window_end: datetime,
    files: list[ManifestFile],
) -> dict:
    return {
        "submission_id": submission_id,
        "submitter_id": submitter_id,
        "state": state,
        "inferred_file_type": inferred_file_type,
        "layout_version": layout_version,
        "coverage_start_month": coverage_start_month,
        "coverage_end_month": coverage_end_month,
        "grouping_method": grouping_method,
        "grouping_window": {
            "start": group_window_start.isoformat(),
            "end": group_window_end.isoformat(),
        },
        "files": [asdict(file) for file in files],
    }


def manifest_bytes(payload: dict) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
