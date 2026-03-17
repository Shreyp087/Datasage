"""create merge_operations table

Revision ID: 0003_merge_operations
Revises: 0002
Create Date: 2026-02-28
"""

from alembic import op
import sqlalchemy as sa

revision    = "0003_merge_operations"
down_revision = "0002"
branch_labels = None
depends_on    = None


def upgrade():
    op.create_table(
        "merge_operations",
        sa.Column("id",                 sa.String(36),  primary_key=True),
        sa.Column("user_id",            sa.String(36),  nullable=False, index=True),
        sa.Column("left_dataset_id",    sa.String(36),  nullable=False),
        sa.Column("right_dataset_id",   sa.String(36),  nullable=False),
        sa.Column("output_dataset_id",  sa.String(36),  nullable=True),

        # Join config
        sa.Column("left_col",   sa.String(255), nullable=False),
        sa.Column("right_col",  sa.String(255), nullable=False),
        sa.Column("strategy",   sa.String(50),  nullable=False),
        sa.Column("join_type",  sa.String(20),  nullable=False),

        # Quality stats
        sa.Column("merged_rows",      sa.Integer, default=0),
        sa.Column("matched_rows",     sa.Integer, default=0),
        sa.Column("left_only_rows",   sa.Integer, default=0),
        sa.Column("right_only_rows",  sa.Integer, default=0),

        # Warnings JSON
        sa.Column("warnings", sa.Text, default="[]"),

        sa.Column("created_at", sa.DateTime, nullable=True),
    )

    op.create_index(
        "ix_merge_operations_user_created",
        "merge_operations",
        ["user_id", "created_at"],
    )


def downgrade():
    op.drop_index("ix_merge_operations_user_created", "merge_operations")
    op.drop_table("merge_operations")
