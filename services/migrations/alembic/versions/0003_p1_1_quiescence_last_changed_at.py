"""p1.1 quiescence and idempotency columns

Revision ID: 0003_p1_1_quiescence_last_changed_at
Revises: 0002_p1_intake_tables
Create Date: 2026-02-16 00:30:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_p1_1_quiescence_last_changed_at"
down_revision: Union[str, None] = "0002_p1_intake_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "inbox_object",
        sa.Column("last_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE inbox_object SET last_changed_at = last_seen_at WHERE last_changed_at IS NULL"
    )

    op.add_column(
        "submission", sa.Column("group_fingerprint", sa.Text(), nullable=True)
    )
    op.create_index(
        "ux_submission_group_fingerprint",
        "submission",
        ["group_fingerprint"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_submission_group_fingerprint", table_name="submission")
    op.drop_column("submission", "group_fingerprint")
    op.drop_column("inbox_object", "last_changed_at")
