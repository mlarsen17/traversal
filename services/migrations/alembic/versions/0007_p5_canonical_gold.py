"""p5 canonical statewide gold metadata tables

Revision ID: 0007_p5_canonical_gold
Revises: 0006_p4_submitter_gold
Create Date: 2026-02-20 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007_p5_canonical_gold"
down_revision: Union[str, None] = "0006_p4_submitter_gold"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "canonical_schema",
        sa.Column("canonical_schema_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Text(), nullable=False),
        sa.Column("columns_json", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("file_type", "schema_version", name="ux_canonical_schema_file_type_ver"),
    )

    op.create_table(
        "canonical_mapping",
        sa.Column("mapping_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("layout_id", sa.Text(), nullable=False),
        sa.Column("canonical_schema_id", sa.Text(), nullable=False),
        sa.Column("mapping_version", sa.Text(), nullable=False),
        sa.Column("mapping_json", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "layout_id",
            "canonical_schema_id",
            "mapping_version",
            name="ux_canonical_mapping_layout_schema_ver",
        ),
    )
    op.create_index("ix_canonical_mapping_layout", "canonical_mapping", ["layout_id"], unique=False)

    op.create_table(
        "canonical_month_build",
        sa.Column("canonical_month_build_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("atomic_month", sa.Text(), nullable=False),
        sa.Column("canonical_schema_id", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("input_fingerprint", sa.Text(), nullable=False),
        sa.Column("output_prefix", sa.Text(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_canonical_month_build_scope",
        "canonical_month_build",
        ["state", "file_type", "atomic_month", "canonical_schema_id"],
        unique=False,
    )

    op.create_table(
        "canonical_rebuild_queue",
        sa.Column("queue_id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("atomic_month", sa.Text(), nullable=False),
        sa.Column("enqueued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_canonical_rebuild_queue_unprocessed",
        "canonical_rebuild_queue",
        ["processed_at", "state", "file_type", "atomic_month", "queue_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_canonical_rebuild_queue_unprocessed", table_name="canonical_rebuild_queue")
    op.drop_table("canonical_rebuild_queue")
    op.drop_index("ix_canonical_month_build_scope", table_name="canonical_month_build")
    op.drop_table("canonical_month_build")
    op.drop_index("ix_canonical_mapping_layout", table_name="canonical_mapping")
    op.drop_table("canonical_mapping")
    op.drop_table("canonical_schema")
