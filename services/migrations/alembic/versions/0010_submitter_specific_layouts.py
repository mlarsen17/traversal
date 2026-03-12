"""submitter-aware layout identity and routing config

Revision ID: 0010_submitter_specific_layouts
Revises: 0009_p6_serving_perf_indexes
Create Date: 2026-03-12 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0010_submitter_specific_layouts"
down_revision: Union[str, None] = "0009_p6_serving_perf_indexes"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("layout_registry", sa.Column("submitter_id", sa.Text(), nullable=True))
    op.add_column("layout_registry", sa.Column("layout_key", sa.Text(), nullable=True))

    op.execute("UPDATE layout_registry SET submitter_id='*' WHERE submitter_id IS NULL")
    op.execute("UPDATE layout_registry SET layout_key=file_type WHERE layout_key IS NULL")

    with op.batch_alter_table("layout_registry") as batch:
        batch.alter_column("submitter_id", existing_type=sa.Text(), nullable=False)
        batch.alter_column("layout_key", existing_type=sa.Text(), nullable=False)

    op.drop_index("ux_layout_registry_file_type_version", table_name="layout_registry")
    op.create_index(
        "ux_layout_registry_submitter_identity",
        "layout_registry",
        ["submitter_id", "file_type", "layout_key", "layout_version"],
        unique=True,
    )
    op.create_index(
        "ix_layout_registry_submitter_file_type_status",
        "layout_registry",
        ["submitter_id", "file_type", "status"],
        unique=False,
    )

    op.create_table(
        "submitter_file_config",
        sa.Column("routing_config_id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("submitter_id", sa.Text(), nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("filename_pattern", sa.Text(), nullable=True),
        sa.Column("file_extension", sa.Text(), nullable=True),
        sa.Column(
            "default_layout_id",
            sa.Text(),
            sa.ForeignKey("layout_registry.layout_id"),
            nullable=False,
        ),
        sa.Column("delimiter", sa.Text(), nullable=True),
        sa.Column("has_header", sa.Boolean(), nullable=True),
        sa.Column("header_signature_json", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("status", sa.Text(), nullable=False, server_default="ACTIVE"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ux_submitter_file_config_identity",
        "submitter_file_config",
        ["submitter_id", "file_type", "filename_pattern", "file_extension", "priority"],
        unique=True,
    )
    op.create_index(
        "ix_submitter_file_config_lookup",
        "submitter_file_config",
        ["submitter_id", "file_type", "status", "priority"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_submitter_file_config_lookup", table_name="submitter_file_config")
    op.drop_index("ux_submitter_file_config_identity", table_name="submitter_file_config")
    op.drop_table("submitter_file_config")

    op.drop_index(
        "ix_layout_registry_submitter_file_type_status",
        table_name="layout_registry",
    )
    op.drop_index("ux_layout_registry_submitter_identity", table_name="layout_registry")
    op.create_index(
        "ux_layout_registry_file_type_version",
        "layout_registry",
        ["file_type", "layout_version"],
        unique=True,
    )

    with op.batch_alter_table("layout_registry") as batch:
        batch.drop_column("layout_key")
        batch.drop_column("submitter_id")
