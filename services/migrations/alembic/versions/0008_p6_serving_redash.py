"""p6 serving schema and dashboard views

Revision ID: 0008_p6_serving_redash
Revises: 0007_p5_canonical_gold
Create Date: 2026-03-11 00:00:00.000000
"""

from pathlib import Path
from typing import Sequence, Union

from alembic import op

revision: str = "0008_p6_serving_redash"
down_revision: Union[str, None] = "0007_p5_canonical_gold"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "serving"

_UPGRADE_FILES = [
    "00_schema.sql",
    "10_submission_overview.sql",
    "11_validation_findings.sql",
    "12_submitter_month_status.sql",
    "13_canonical_month_status.sql",
    "14_dashboard_kpis_daily.sql",
    "15_parse_column_health.sql",
    "16_validation_summary.sql",
    "17_submission_timeline.sql",
]

_DOWNGRADE_DROPS = [
    "DROP VIEW IF EXISTS serving.submission_timeline;",
    "DROP VIEW IF EXISTS serving.validation_summary;",
    "DROP VIEW IF EXISTS serving.parse_column_health;",
    "DROP VIEW IF EXISTS serving.dashboard_kpis_daily;",
    "DROP VIEW IF EXISTS serving.canonical_month_status;",
    "DROP VIEW IF EXISTS serving.submitter_month_status;",
    "DROP VIEW IF EXISTS serving.validation_findings;",
    "DROP VIEW IF EXISTS serving.submission_overview;",
    "DROP SCHEMA IF EXISTS serving;",
]


def _read_sql(filename: str) -> str:
    return (_SQL_DIR / filename).read_text(encoding="utf-8")


def upgrade() -> None:
    for filename in _UPGRADE_FILES:
        op.execute(_read_sql(filename))


def downgrade() -> None:
    for ddl in _DOWNGRADE_DROPS:
        op.execute(ddl)
