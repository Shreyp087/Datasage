"""create merge_operations table

Revision ID: 0003_merge_operations
Revises: e4a9f7ac2d11
Create Date: 2026-03-01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0003_merge_operations"
down_revision: Union[str, None] = "e4a9f7ac2d11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "merge_operations",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("left_dataset_id", sa.UUID(), nullable=False),
        sa.Column("right_dataset_id", sa.UUID(), nullable=False),
        sa.Column("output_dataset_id", sa.UUID(), nullable=True),
        sa.Column("left_col", sa.String(length=255), nullable=False),
        sa.Column("right_col", sa.String(length=255), nullable=False),
        sa.Column("strategy", sa.String(length=50), nullable=False),
        sa.Column("join_type", sa.String(length=20), nullable=False),
        sa.Column("merged_rows", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("matched_rows", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("left_only_rows", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("right_only_rows", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("warnings", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["left_dataset_id"], ["datasets.id"]),
        sa.ForeignKeyConstraint(["right_dataset_id"], ["datasets.id"]),
        sa.ForeignKeyConstraint(["output_dataset_id"], ["datasets.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_merge_operations_user_id"), "merge_operations", ["user_id"], unique=False)
    op.create_index(op.f("ix_merge_operations_created_at"), "merge_operations", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_merge_operations_created_at"), table_name="merge_operations")
    op.drop_index(op.f("ix_merge_operations_user_id"), table_name="merge_operations")
    op.drop_table("merge_operations")
