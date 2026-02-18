"""p2 parse runs and metrics tables

Revision ID: 0004_p2_parse_runs
Revises: 0003_p1_1_quiescence_last_changed_at
Create Date: 2026-02-17 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004_p2_parse_runs"
down_revision: Union[str, None] = "0003_p1_1_quiescence_last_changed_at"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "parse_run",
        sa.Column("parse_run_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column(
            "submission_id",
            sa.Text(),
            sa.ForeignKey("submission.submission_id"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("engine", sa.Text(), nullable=False),
        sa.Column("silver_prefix", sa.Text(), nullable=False),
        sa.Column("report_object_key", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_parse_run_submission", "parse_run", ["submission_id"], unique=False
    )

    op.create_table(
        "parse_file_metrics",
        sa.Column(
            "parse_file_metrics_id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "parse_run_id",
            sa.Text(),
            sa.ForeignKey("parse_run.parse_run_id"),
            nullable=False,
        ),
        sa.Column("raw_object_key", sa.Text(), nullable=False),
        sa.Column("rows_read", sa.BigInteger(), nullable=False),
        sa.Column("rows_written", sa.BigInteger(), nullable=False),
        sa.Column("rows_rejected", sa.BigInteger(), nullable=False),
        sa.Column("bytes_read", sa.BigInteger(), nullable=True),
        sa.Column("etag", sa.Text(), nullable=True),
    )

    op.create_table(
        "parse_column_metrics",
        sa.Column(
            "parse_column_metrics_id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "parse_run_id",
            sa.Text(),
            sa.ForeignKey("parse_run.parse_run_id"),
            nullable=False,
        ),
        sa.Column("column_name", sa.Text(), nullable=False),
        sa.Column("null_count", sa.BigInteger(), nullable=False),
        sa.Column("invalid_count", sa.BigInteger(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("parse_column_metrics")
    op.drop_table("parse_file_metrics")
    op.drop_index("ix_parse_run_submission", table_name="parse_run")
    op.drop_table("parse_run")
