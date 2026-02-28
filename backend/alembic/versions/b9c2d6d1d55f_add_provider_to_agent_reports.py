"""Add provider column to agent_reports

Revision ID: b9c2d6d1d55f
Revises: 3dea3e43208f
Create Date: 2026-02-27 18:22:30
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b9c2d6d1d55f"
down_revision: Union[str, None] = "3dea3e43208f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "agent_reports",
        sa.Column("provider", sa.String(), nullable=False, server_default="openai"),
    )
    op.alter_column("agent_reports", "provider", server_default=None)


def downgrade() -> None:
    op.drop_column("agent_reports", "provider")
