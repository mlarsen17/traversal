"""add p1 intake tables

Revision ID: 0002_p1_intake_tables
Revises: 0001_create_bootstrap_table
Create Date: 2026-02-16 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_p1_intake_tables"
down_revision: Union[str, None] = "0001_create_bootstrap_table"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "layout_registry",
        sa.Column("layout_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("layout_version", sa.Text(), nullable=False),
        sa.Column("schema_json", sa.Text(), nullable=False),
        sa.Column("parser_config_json", sa.Text(), nullable=False),
        sa.Column("effective_start_date", sa.Date(), nullable=True),
        sa.Column("effective_end_date", sa.Date(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
    )
    op.create_index(
        "ux_layout_registry_file_type_version",
        "layout_registry",
        ["file_type", "layout_version"],
        unique=True,
    )

    op.create_table(
        "submission",
        sa.Column("submission_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column(
            "layout_id",
            sa.Text(),
            sa.ForeignKey("layout_registry.layout_id"),
            nullable=True,
        ),
        sa.Column("coverage_start_month", sa.Text(), nullable=True),
        sa.Column("coverage_end_month", sa.Text(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("grouping_method", sa.Text(), nullable=False),
        sa.Column("inbox_prefix", sa.Text(), nullable=False),
        sa.Column("raw_prefix", sa.Text(), nullable=False),
        sa.Column("manifest_object_key", sa.Text(), nullable=False),
        sa.Column("manifest_sha256", sa.Text(), nullable=False),
    )
    op.create_index(
        "ix_submission_submitter_type_received",
        "submission",
        ["submitter_id", "file_type", "received_at"],
        unique=False,
    )
    op.create_index("ix_submission_status", "submission", ["status"], unique=False)

    op.create_table(
        "submission_file",
        sa.Column(
            "submission_file_id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "submission_id",
            sa.Text(),
            sa.ForeignKey("submission.submission_id"),
            nullable=False,
        ),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("bytes", sa.BigInteger(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=True),
    )

    op.create_table(
        "inbox_object",
        sa.Column("object_key", sa.Text(), primary_key=True, nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("bytes", sa.BigInteger(), nullable=True),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("inbox_object")
    op.drop_table("submission_file")
    op.drop_index("ix_submission_status", table_name="submission")
    op.drop_index("ix_submission_submitter_type_received", table_name="submission")
    op.drop_table("submission")
    op.drop_index("ux_layout_registry_file_type_version", table_name="layout_registry")
    op.drop_table("layout_registry")
