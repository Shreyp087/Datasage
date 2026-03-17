"""
DataSage — MergeOperation SQLAlchemy Model
Records every merge applied by users for history/audit.
"""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer, String, Text, JSON
)
from sqlalchemy.orm import relationship

from app.core.database import Base


class MergeOperation(Base):
    __tablename__ = "merge_operations"

    id                 = Column(String(36), primary_key=True)
    user_id            = Column(String(36), nullable=False, index=True)
    left_dataset_id    = Column(String(36), nullable=False)
    right_dataset_id   = Column(String(36), nullable=False)
    output_dataset_id  = Column(String(36), nullable=True)

    # Join config
    left_col           = Column(String(255), nullable=False)
    right_col          = Column(String(255), nullable=False)
    strategy           = Column(String(50),  nullable=False)
    join_type          = Column(String(20),  nullable=False)

    # Quality stats
    merged_rows        = Column(Integer, default=0)
    matched_rows       = Column(Integer, default=0)
    left_only_rows     = Column(Integer, default=0)
    right_only_rows    = Column(Integer, default=0)

    # Warnings stored as JSON list
    _warnings          = Column("warnings", Text, default="[]")

    created_at         = Column(DateTime, default=datetime.utcnow)

    # ── Property to handle JSON serialisation of warnings ─────────────────────
    @property
    def warnings(self) -> list[str]:
        try:
            return json.loads(self._warnings or "[]")
        except (json.JSONDecodeError, TypeError):
            return []

    @warnings.setter
    def warnings(self, value: list[str]) -> None:
        self._warnings = json.dumps(value or [])

    def __repr__(self) -> str:
        return (
            f"<MergeOperation id={self.id!r} "
            f"{self.left_col}↔{self.right_col} "
            f"({self.join_type}) "
            f"rows={self.merged_rows}>"
        )
