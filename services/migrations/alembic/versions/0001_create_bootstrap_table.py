"""create bootstrap heartbeat table

Revision ID: 0001_create_bootstrap_table
Revises:
Create Date: 2026-02-15 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_create_bootstrap_table"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "bootstrap_heartbeat",
        sa.Column(
            "id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("bootstrap_heartbeat")
