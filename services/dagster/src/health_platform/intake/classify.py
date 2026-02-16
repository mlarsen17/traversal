from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Protocol


@dataclass
class ClassificationResult:
    file_type: str | None
    layout_version: str | None
    confidence: float
    coverage_start_month: str | None
    coverage_end_month: str | None


class SubmissionClassifier(Protocol):
    def classify(self, submitter_id: str, object_keys: list[str]) -> ClassificationResult:
        ...


class SubmitterFilenameClassifier:
    """Default P1 classifier using submitter_id + filenames.

    This implementation is intentionally simple and easy to swap by setting
    `CLASSIFIER_IMPL` and adding another classifier implementation.
    """

    _patterns = {
        "medical": re.compile(r"medical|claim", re.IGNORECASE),
        "pharmacy": re.compile(r"pharm|pharmacy|rx", re.IGNORECASE),
        "members": re.compile(r"member", re.IGNORECASE),
        "enrollment": re.compile(r"enroll|elig", re.IGNORECASE),
    }

    def classify(self, submitter_id: str, object_keys: list[str]) -> ClassificationResult:
        scores = {key: 0.0 for key in self._patterns}

        submitter_hint = submitter_id.lower()
        for file_type, pattern in self._patterns.items():
            if pattern.search(submitter_hint):
                scores[file_type] += 0.35

        for key in object_keys:
            key_lower = key.lower()
            for file_type, pattern in self._patterns.items():
                if pattern.search(key_lower):
                    scores[file_type] += 0.80

        best_type = max(scores, key=lambda item: scores[item]) if scores else None
        confidence = scores.get(best_type, 0.0) if best_type else 0.0
        threshold = float(os.getenv("CLASSIFY_CONFIDENCE_THRESHOLD", "0.7"))

        if not best_type or confidence < threshold:
            return ClassificationResult(None, None, confidence, None, None)

        return ClassificationResult(
            file_type=best_type,
            layout_version="v1",
            confidence=min(confidence, 0.99),
            coverage_start_month=None,
            coverage_end_month=None,
        )


def get_classifier() -> SubmissionClassifier:
    impl = os.getenv("CLASSIFIER_IMPL", "submitter_filename").strip().lower()
    if impl == "submitter_filename":
        return SubmitterFilenameClassifier()
    raise ValueError(f"Unsupported CLASSIFIER_IMPL '{impl}'")


def classify_submission(submitter_id: str, object_keys: list[str], classifier: SubmissionClassifier | None = None) -> ClassificationResult:
    return (classifier or get_classifier()).classify(submitter_id=submitter_id, object_keys=object_keys)
