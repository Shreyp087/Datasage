from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from app.models.models import (
    PlanEnum, DatasetDomainEnum, FileFormatEnum, DatasetStatusEnum,
    SeverityEnum, JoinTypeEnum
)

# ----------------- User Schemas -----------------
class UserBase(BaseModel):
    email: EmailStr
    name: str
    plan: PlanEnum = PlanEnum.free

class UserCreate(UserBase):
    password: str

class UserResponse(UserBase):
    id: uuid.UUID
    storage_used_bytes: int
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

# ----------------- Dataset Schemas -----------------
class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None
    domain: DatasetDomainEnum = DatasetDomainEnum.general
    original_filename: str
    file_format: FileFormatEnum
    file_size_bytes: int

class DatasetCreate(DatasetBase):
    pass

class DatasetResponse(DatasetBase):
    id: uuid.UUID
    user_id: uuid.UUID
    storage_path: Optional[str] = None
    cleaned_storage_path: Optional[str] = None
    row_count: Optional[int] = None
    col_count: Optional[int] = None
    status: DatasetStatusEnum
    schema_json: Optional[Dict[str, Any]] = None
    uploaded_at: datetime
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

# ----------------- DatasetGroup Schemas -----------------
class DatasetGroupBase(BaseModel):
    name: str
    dataset_ids: List[uuid.UUID]

class DatasetGroupCreate(DatasetGroupBase):
    pass

class DatasetGroupResponse(DatasetGroupBase):
    id: uuid.UUID
    user_id: uuid.UUID
    result_dataset_id: Optional[uuid.UUID] = None

    model_config = ConfigDict(from_attributes=True)

# ----------------- ProcessingJob Schemas -----------------
class ProcessingJobResponse(BaseModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    status: DatasetStatusEnum
    progress_pct: float
    current_step: Optional[str] = None
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

# ----------------- ProcessingLog Schemas -----------------
class ProcessingLogResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    step_name: str
    action: str
    column_name: Optional[str] = None
    before_value: Optional[Any] = None
    after_value: Optional[Any] = None
    reason: Optional[str] = None
    severity: SeverityEnum
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

# ----------------- EDAReport Schemas -----------------
class EDAReportResponse(BaseModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    html_report_path: Optional[str] = None
    json_summary: Optional[Dict[str, Any]] = None
    profile_stats: Optional[Dict[str, Any]] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

# ----------------- AgentReport Schemas -----------------
class AgentReportResponse(BaseModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    agent_name: str
    agent_role: str
    report_markdown: Optional[str] = None
    structured_json: Optional[Dict[str, Any]] = None
    tokens_used: int
    model_used: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

# ----------------- MergeConfig Schemas -----------------
class MergeConfigCreate(BaseModel):
    left_dataset_id: uuid.UUID
    right_dataset_id: uuid.UUID
    left_key: str
    right_key: str
    join_type: JoinTypeEnum = JoinTypeEnum.inner

class MergeConfigResponse(MergeConfigCreate):
    id: uuid.UUID
    user_id: uuid.UUID
    result_dataset_id: Optional[uuid.UUID] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
