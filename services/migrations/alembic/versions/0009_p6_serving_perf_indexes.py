"""p6 serving performance indexes

Revision ID: 0009_p6_serving_perf_indexes
Revises: 0008_p6_serving_redash
Create Date: 2026-03-11 00:15:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0009_p6_serving_perf_indexes"
down_revision: Union[str, None] = "0008_p6_serving_redash"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_parse_run_submission_started_at
        ON parse_run (submission_id, started_at DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_validation_run_submission_started_at
        ON validation_run (submission_id, started_at DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_validation_finding_run_rule_passed
        ON validation_finding (validation_run_id, rule_id, passed);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_submitter_month_pointer_winning_submission
        ON submitter_month_pointer (winning_submission_id);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_canonical_month_build_scope_started_at
        ON canonical_month_build (state, file_type, atomic_month, started_at DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_canonical_month_build_scope_started_at;")
    op.execute("DROP INDEX IF EXISTS ix_submitter_month_pointer_winning_submission;")
    op.execute("DROP INDEX IF EXISTS ix_validation_finding_run_rule_passed;")
    op.execute("DROP INDEX IF EXISTS ix_validation_run_submission_started_at;")
    op.execute("DROP INDEX IF EXISTS ix_parse_run_submission_started_at;")
