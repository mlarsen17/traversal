from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParsedFilename:
    file_type: str
    coverage_start_month: Optional[str] = None
    coverage_end_month: Optional[str] = None
    layout_version: Optional[str] = None
    confidence: float = 0.0


class FilenameConvention(ABC):
    name: str

    @abstractmethod
    def match(self, filename: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def parse(self, filename: str) -> ParsedFilename:
        raise NotImplementedError


class PrefixCoverageConvention(FilenameConvention):
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.name = f"{prefix}_YYYYMM_YYYYMM"
        self._pattern = re.compile(
            rf"^{re.escape(prefix)}_(\d{{6}})_(\d{{6}})\.(txt|csv|gz)$",
            re.IGNORECASE,
        )

    def match(self, filename: str) -> bool:
        return bool(self._pattern.match(filename))

    def parse(self, filename: str) -> ParsedFilename:
        m = self._pattern.match(filename)
        if not m:
            return ParsedFilename(file_type="unknown", confidence=0.0)
        return ParsedFilename(
            file_type=self.prefix,
            coverage_start_month=m.group(1),
            coverage_end_month=m.group(2),
            layout_version="v1",
            confidence=1.0,
        )


class ConventionRegistry:
    def __init__(self, conventions: list[FilenameConvention] | None = None):
        self._conventions = conventions[:] if conventions else []

    def register(self, convention: FilenameConvention) -> None:
        self._conventions.append(convention)

    def parse(self, filename: str) -> ParsedFilename:
        matches = [c.parse(filename) for c in self._conventions if c.match(filename)]
        if not matches:
            return ParsedFilename(file_type="unknown", confidence=0.0)
        return sorted(matches, key=lambda m: m.confidence, reverse=True)[0]


def default_registry() -> ConventionRegistry:
    registry = ConventionRegistry()
    for name in ["medical", "pharmacy", "members", "enrollment"]:
        registry.register(PrefixCoverageConvention(name))
    return registry
