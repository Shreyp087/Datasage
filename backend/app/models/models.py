import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Any

from sqlalchemy import Column, String, Float, Integer, BigInteger, DateTime, ForeignKey, Text, Enum as SQLEnum, ARRAY, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import mapped_column, Mapped, relationship

from app.core.database import Base

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class PlanEnum(str, Enum):
    free = "free"
    pro = "pro"
    enterprise = "enterprise"

class DatasetDomainEnum(str, Enum):
    general = "general"
    healthcare = "healthcare"
    finance = "finance"
    education = "education"
    ecommerce = "ecommerce"
    ai_incidents = "ai_incidents"
    other = "other"

class FileFormatEnum(str, Enum):
    csv = "csv"
    excel = "excel"
    json = "json"
    parquet = "parquet"
    tsv = "tsv"
    zip = "zip"

class DatasetStatusEnum(str, Enum):
    uploaded = "uploaded"
    queued = "queued"
    preprocessing = "preprocessing"
    eda_running = "eda_running"
    agents_running = "agents_running"
    complete = "complete"
    failed = "failed"

class SeverityEnum(str, Enum):
    info = "info"
    warning = "warning"
    error = "error"

class JoinTypeEnum(str, Enum):
    inner = "inner"
    left = "left"
    right = "right"
    outer = "outer"

class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    plan: Mapped[PlanEnum] = mapped_column(SQLEnum(PlanEnum), default=PlanEnum.free)
    storage_used_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    
    datasets = relationship("Dataset", back_populates="user")
    
    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, plan={self.plan})>"

class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    domain: Mapped[DatasetDomainEnum] = mapped_column(SQLEnum(DatasetDomainEnum), default=DatasetDomainEnum.general)
    original_filename: Mapped[str] = mapped_column(String, nullable=False)
    storage_path: Mapped[Optional[str]] = mapped_column(String, nullable=True) # MinIO key
    cleaned_storage_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    file_format: Mapped[FileFormatEnum] = mapped_column(SQLEnum(FileFormatEnum), nullable=False)
    row_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    col_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[DatasetStatusEnum] = mapped_column(SQLEnum(DatasetStatusEnum), default=DatasetStatusEnum.uploaded, index=True)
    schema_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True) # column metadata
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    user = relationship("User", back_populates="datasets")
    jobs = relationship("ProcessingJob", back_populates="dataset")
    eda_reports = relationship("EDAReport", back_populates="dataset")
    agent_reports = relationship("AgentReport", back_populates="dataset")
    
    def __repr__(self):
        return f"<Dataset(id={self.id}, name={self.name}, status={self.status})>"

class DatasetGroup(Base):
    __tablename__ = "dataset_groups"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    dataset_ids: Mapped[List[uuid.UUID]] = mapped_column(ARRAY(UUID(as_uuid=True)))
    result_dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    
    def __repr__(self):
        return f"<DatasetGroup(id={self.id}, name={self.name})>"

class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), index=True, nullable=False)
    status: Mapped[DatasetStatusEnum] = mapped_column(SQLEnum(DatasetStatusEnum), default=DatasetStatusEnum.queued, index=True)
    progress_pct: Mapped[float] = mapped_column(Float, default=0.0)
    current_step: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now)
    
    dataset = relationship("Dataset", back_populates="jobs")
    logs = relationship("ProcessingLog", back_populates="job")
    
    def __repr__(self):
        return f"<ProcessingJob(id={self.id}, dataset_id={self.dataset_id}, status={self.status})>"

class ProcessingLog(Base):
    __tablename__ = "processing_logs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("processing_jobs.id"), index=True, nullable=False)
    step_name: Mapped[str] = mapped_column(String, nullable=False)
    action: Mapped[str] = mapped_column(String, nullable=False)
    column_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    before_value: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    after_value: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    severity: Mapped[SeverityEnum] = mapped_column(SQLEnum(SeverityEnum), default=SeverityEnum.info)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    
    job = relationship("ProcessingJob", back_populates="logs")
    
    def __repr__(self):
        return f"<ProcessingLog(job_id={self.job_id}, step={self.step_name}, severity={self.severity})>"

class EDAReport(Base):
    __tablename__ = "eda_reports"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), index=True, nullable=False)
    html_report_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    json_summary: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    profile_stats: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    
    dataset = relationship("Dataset", back_populates="eda_reports")
    
    def __repr__(self):
        return f"<EDAReport(id={self.id}, dataset_id={self.dataset_id})>"

class AgentReport(Base):
    __tablename__ = "agent_reports"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), index=True, nullable=False)
    agent_name: Mapped[str] = mapped_column(String, nullable=False)
    agent_role: Mapped[str] = mapped_column(String, nullable=False)
    report_markdown: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    structured_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    tokens_used: Mapped[int] = mapped_column(Integer, default=0)
    model_used: Mapped[str] = mapped_column(String, nullable=False)
    provider: Mapped[str] = mapped_column(String, nullable=False, default="openai")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    
    dataset = relationship("Dataset", back_populates="agent_reports")
    
    def __repr__(self):
        return f"<AgentReport(agent_name={self.agent_name}, dataset_id={self.dataset_id})>"

class MergeConfig(Base):
    __tablename__ = "merge_configs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), index=True, nullable=False)
    left_dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    right_dataset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=False)
    left_key: Mapped[str] = mapped_column(String, nullable=False)
    right_key: Mapped[str] = mapped_column(String, nullable=False)
    join_type: Mapped[JoinTypeEnum] = mapped_column(SQLEnum(JoinTypeEnum), default=JoinTypeEnum.inner)
    result_dataset_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    
    def __repr__(self):
        return f"<MergeConfig(id={self.id}, join_type={self.join_type})>"
