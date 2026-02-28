import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import ARRAY, Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Notebook(Base):
    __tablename__ = "notebooks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True, index=True)

    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    domain: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)

    cells: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    results: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    is_template: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    tags: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)

    snapshot_date: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    snapshot_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    run_count: Mapped[int] = mapped_column(Integer, default=0)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)

    def __repr__(self) -> str:
        return f"<Notebook(id={self.id}, title={self.title}, is_template={self.is_template})>"
