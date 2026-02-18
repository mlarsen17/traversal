"""p3 validation engine metadata tables

Revision ID: 0005_p3_validation_engine
Revises: 0004_p2_parse_runs
Create Date: 2026-02-18 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0005_p3_validation_engine"
down_revision: Union[str, None] = "0004_p2_parse_runs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "submission", sa.Column("latest_validation_run_id", sa.Text(), nullable=True)
    )

    op.create_table(
        "validation_rule",
        sa.Column("rule_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("rule_kind", sa.Text(), nullable=False),
        sa.Column("default_severity", sa.Text(), nullable=False),
        sa.Column("default_threshold_type", sa.Text(), nullable=False),
        sa.Column("default_threshold_value", sa.Float(), nullable=False),
        sa.Column("definition_json", sa.Text(), nullable=False),
        sa.Column("sql_template", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "validation_rule_set",
        sa.Column("rule_set_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("file_type", sa.Text(), nullable=False),
        sa.Column("layout_version", sa.Text(), nullable=True),
        sa.Column("effective_start", sa.Date(), nullable=True),
        sa.Column("effective_end", sa.Date(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "validation_rule_set_rule",
        sa.Column(
            "rule_set_id",
            sa.Text(),
            sa.ForeignKey("validation_rule_set.rule_set_id"),
            nullable=False,
        ),
        sa.Column(
            "rule_id",
            sa.Text(),
            sa.ForeignKey("validation_rule.rule_id"),
            nullable=False,
        ),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("severity_override", sa.Text(), nullable=True),
        sa.Column("threshold_type_override", sa.Text(), nullable=True),
        sa.Column("threshold_value_override", sa.Float(), nullable=True),
        sa.Column("params_override_json", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint(
            "rule_set_id", "rule_id", name="pk_validation_rule_set_rule"
        ),
    )

    op.create_table(
        "validation_run",
        sa.Column("validation_run_id", sa.Text(), primary_key=True, nullable=False),
        sa.Column(
            "submission_id",
            sa.Text(),
            sa.ForeignKey("submission.submission_id"),
            nullable=False,
        ),
        sa.Column(
            "rule_set_id",
            sa.Text(),
            sa.ForeignKey("validation_rule_set.rule_set_id"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("engine", sa.Text(), nullable=False, server_default="duckdb"),
        sa.Column("engine_version", sa.Text(), nullable=True),
        sa.Column("silver_prefix", sa.Text(), nullable=False),
        sa.Column("total_rows", sa.BigInteger(), nullable=True),
        sa.Column("report_object_key", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_validation_run_submission",
        "validation_run",
        ["submission_id"],
        unique=False,
    )

    op.create_table(
        "validation_finding",
        sa.Column(
            "validation_finding_id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "validation_run_id",
            sa.Text(),
            sa.ForeignKey("validation_run.validation_run_id"),
            nullable=False,
        ),
        sa.Column(
            "rule_id",
            sa.Text(),
            sa.ForeignKey("validation_rule.rule_id"),
            nullable=False,
        ),
        sa.Column("scope_month", sa.Text(), nullable=True),
        sa.Column("violations_count", sa.BigInteger(), nullable=False),
        sa.Column("denominator_count", sa.BigInteger(), nullable=False),
        sa.Column("violations_rate", sa.Float(), nullable=True),
        sa.Column("sample_object_key", sa.Text(), nullable=True),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_foreign_key(
        "fk_submission_latest_validation_run",
        "submission",
        "validation_run",
        ["latest_validation_run_id"],
        ["validation_run_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_submission_latest_validation_run", "submission", type_="foreignkey"
    )
    op.drop_table("validation_finding")
    op.drop_index("ix_validation_run_submission", table_name="validation_run")
    op.drop_table("validation_run")
    op.drop_table("validation_rule_set_rule")
    op.drop_table("validation_rule_set")
    op.drop_table("validation_rule")
    op.drop_column("submission", "latest_validation_run_id")
