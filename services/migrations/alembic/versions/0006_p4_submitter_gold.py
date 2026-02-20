"""p4 submitter gold merge metadata tables

Revision ID: 0006_p4_submitter_gold
Revises: 0005_p3_validation_engine
Create Date: 2026-02-19 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006_p4_submitter_gold"
down_revision: Union[str, None] = "0005_p3_validation_engine"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "submitter_month_pointer",
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("atomic_month", sa.Text(), nullable=False),
        sa.Column("winning_submission_id", sa.Text(), nullable=False),
        sa.Column("replaced_submission_id", sa.Text(), nullable=True),
        sa.Column("winning_validation_run_id", sa.Text(), nullable=True),
        sa.Column("winner_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "state",
            "file_type",
            "submitter_id",
            "atomic_month",
            name="ux_submitter_month_pointer_scope",
        ),
    )

    op.create_table(
        "submitter_gold_build",
        sa.Column(
            "submitter_gold_build_id", sa.Text(), primary_key=True, nullable=False
        ),
        sa.Column(
            "submission_id",
            sa.Text(),
            sa.ForeignKey("submission.submission_id"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("months_affected_json", sa.Text(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_submitter_gold_build_submission",
        "submitter_gold_build",
        ["submission_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_submitter_gold_build_submission", table_name="submitter_gold_build"
    )
    op.drop_table("submitter_gold_build")
    op.drop_table("submitter_month_pointer")
