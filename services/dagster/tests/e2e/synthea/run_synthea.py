from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from urllib.request import urlretrieve

SYNTHEA_VERSION = "v3.1.1"
SYNTHEA_JAR_URL = f"https://github.com/synthetichealth/synthea/releases/download/{SYNTHEA_VERSION}/synthea-with-dependencies.jar"


def _java_bin() -> str:
    java = shutil.which("java")
    if not java:
        raise RuntimeError("java binary not found; required to run Synthea")
    return java


def _jar_path(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    jar_path = cache_dir / f"synthea-{SYNTHEA_VERSION}.jar"
    if not jar_path.exists():
        urlretrieve(SYNTHEA_JAR_URL, jar_path)
    return jar_path


def run_synthea(output_dir: Path, population: int = 10) -> Path:
    java = _java_bin()
    jar_path = _jar_path(Path(".cache") / "synthea")
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        java,
        "-jar",
        str(jar_path),
        "-s",
        "42",
        "-p",
        str(population),
        "--exporter.baseDirectory",
        str(output_dir),
        "--exporter.csv.export",
        "true",
        "--exporter.fhir.export",
        "false",
        "Massachusetts",
    ]
    subprocess.run(cmd, check=True)

    matches = list(output_dir.rglob("encounters.csv"))
    if not matches:
        raise RuntimeError(f"Synthea run did not create encounters.csv under {output_dir}")
    return matches[0].parent
