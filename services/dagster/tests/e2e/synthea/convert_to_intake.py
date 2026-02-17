from __future__ import annotations

import csv
from pathlib import Path

MEDICAL_COLUMNS = [
    "Id",
    "START",
    "STOP",
    "PATIENT",
    "ORGANIZATION",
    "PROVIDER",
    "PAYER",
    "ENCOUNTERCLASS",
    "CODE",
    "DESCRIPTION",
    "BASE_ENCOUNTER_COST",
    "TOTAL_CLAIM_COST",
    "PAYER_COVERAGE",
    "REASONCODE",
    "REASONDESCRIPTION",
]


def convert_synthea_to_intake(encounters_csv: Path, out_csv: Path) -> Path:
    rows: list[dict[str, str]] = []
    with encounters_csv.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            rows.append({column: row.get(column, "") for column in MEDICAL_COLUMNS})

    rows.sort(key=lambda item: (item.get("START", ""), item.get("Id", "")))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=MEDICAL_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return out_csv
