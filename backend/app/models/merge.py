from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MergeOperation(Base):
    __tablename__ = "merge_operations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    left_dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    right_dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    output_dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)

    left_col: Mapped[str] = mapped_column(String(255), nullable=False)
    right_col: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy: Mapped[str] = mapped_column(String(50), nullable=False)
    join_type: Mapped[str] = mapped_column(String(20), nullable=False)

    merged_rows: Mapped[int] = mapped_column(Integer, default=0)
    matched_rows: Mapped[int] = mapped_column(Integer, default=0)
    left_only_rows: Mapped[int] = mapped_column(Integer, default=0)
    right_only_rows: Mapped[int] = mapped_column(Integer, default=0)
    warnings: Mapped[list[str]] = mapped_column(JSONB, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, index=True)

    def __repr__(self) -> str:
        return (
            f"<MergeOperation(id={self.id}, left_dataset_id={self.left_dataset_id}, "
            f"right_dataset_id={self.right_dataset_id}, join_type={self.join_type})>"
        )
