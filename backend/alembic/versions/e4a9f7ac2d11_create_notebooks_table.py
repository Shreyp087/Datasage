"""Create notebooks table

Revision ID: e4a9f7ac2d11
Revises: c31c7d5a8f21
Create Date: 2026-02-28 00:30:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "e4a9f7ac2d11"
down_revision: Union[str, None] = "c31c7d5a8f21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notebooks",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("dataset_id", sa.UUID(), nullable=True),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("domain", sa.String(length=50), nullable=True),
        sa.Column("cells", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("results", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_public", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("tags", sa.ARRAY(sa.String()), nullable=False, server_default=sa.text("'{}'::varchar[]")),
        sa.Column("snapshot_date", sa.String(length=50), nullable=True),
        sa.Column("snapshot_url", sa.String(length=500), nullable=True),
        sa.Column("run_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["dataset_id"], ["datasets.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_notebooks_user_id"), "notebooks", ["user_id"], unique=False)
    op.create_index(op.f("ix_notebooks_dataset_id"), "notebooks", ["dataset_id"], unique=False)
    op.create_index(op.f("ix_notebooks_domain"), "notebooks", ["domain"], unique=False)
    op.create_index(op.f("ix_notebooks_is_template"), "notebooks", ["is_template"], unique=False)
    op.create_index(op.f("ix_notebooks_is_public"), "notebooks", ["is_public"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_notebooks_is_public"), table_name="notebooks")
    op.drop_index(op.f("ix_notebooks_is_template"), table_name="notebooks")
    op.drop_index(op.f("ix_notebooks_domain"), table_name="notebooks")
    op.drop_index(op.f("ix_notebooks_dataset_id"), table_name="notebooks")
    op.drop_index(op.f("ix_notebooks_user_id"), table_name="notebooks")
    op.drop_table("notebooks")
