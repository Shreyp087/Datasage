"""Add ai_incidents value to datasetdomainenum

Revision ID: c31c7d5a8f21
Revises: b9c2d6d1d55f
Create Date: 2026-02-28 00:00:00
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c31c7d5a8f21"
down_revision: Union[str, None] = "b9c2d6d1d55f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE datasetdomainenum ADD VALUE IF NOT EXISTS 'ai_incidents'")


def downgrade() -> None:
    # PostgreSQL enum value removal is non-trivial and unsafe in-place.
    pass
