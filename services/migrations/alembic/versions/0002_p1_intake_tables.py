"""phase 1 intake tables

Revision ID: 0002_p1_intake_tables
Revises: 0001_create_bootstrap_table
Create Date: 2026-02-16 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002_p1_intake_tables"
down_revision: Union[str, None] = "0001_create_bootstrap_table"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "submitter",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("submitter_id", name="uq_submitter_submitter_id"),
    )

    op.create_table(
        "file_type",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("name", name="uq_file_type_name"),
    )

    op.create_table(
        "layout_registry",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("layout_version", sa.Text(), nullable=False),
        sa.Column("schema_json", sa.Text(), nullable=False),
        sa.Column("parser_config_json", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("file_type", "layout_version", name="uq_layout_registry_type_version"),
    )

    op.create_table(
        "inbox_object",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("grouping_key", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.UniqueConstraint("object_key", name="uq_inbox_object_object_key"),
    )
    op.create_index("ix_inbox_object_grouping_key", "inbox_object", ["grouping_key"], unique=False)

    op.create_table(
        "submission",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("submission_id", sa.Text(), nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("grouping_key", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("inferred_file_type", sa.Text(), nullable=True),
        sa.Column("layout_version", sa.Text(), nullable=True),
        sa.Column("coverage_start_month", sa.Text(), nullable=True),
        sa.Column("coverage_end_month", sa.Text(), nullable=True),
        sa.Column("grouping_method", sa.Text(), nullable=False),
        sa.Column("group_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("group_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_prefix", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["submitter_id"], ["submitter.submitter_id"]),
        sa.UniqueConstraint("submission_id", name="uq_submission_submission_id"),
        sa.UniqueConstraint("fingerprint", name="uq_submission_fingerprint"),
    )
    op.create_index("ix_submission_grouping_key", "submission", ["grouping_key"], unique=False)

    op.create_table(
        "submission_file",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("submission_id", sa.Text(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("raw_object_key", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["submission_id"], ["submission.submission_id"]),
        sa.UniqueConstraint("submission_id", "object_key", name="uq_submission_file_key"),
    )


def downgrade() -> None:
    op.drop_table("submission_file")
    op.drop_index("ix_submission_grouping_key", table_name="submission")
    op.drop_table("submission")
    op.drop_index("ix_inbox_object_grouping_key", table_name="inbox_object")
    op.drop_table("inbox_object")
    op.drop_table("layout_registry")
    op.drop_table("file_type")
    op.drop_table("submitter")
