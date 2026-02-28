import os
import tempfile
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.database import get_db
from app.core.minio_client import minio_client
from app.core.config import settings
from app.models.models import (
    Dataset,
    DatasetDomainEnum,
    DatasetStatusEnum,
    FileFormatEnum,
    ProcessingJob,
    User,
)

router = APIRouter(prefix="/upload", tags=["upload"])


def _resolve_domain(domain: str) -> DatasetDomainEnum:
    normalized = (domain or "").strip().lower()
    if normalized in DatasetDomainEnum._value2member_map_:
        return DatasetDomainEnum(normalized)
    if normalized in {"generic", "general"}:
        return DatasetDomainEnum.general
    return DatasetDomainEnum.other


def _resolve_file_format(filename: str) -> FileFormatEnum:
    ext = Path(filename).suffix.lstrip(".").lower()
    mapping = {
        "csv": FileFormatEnum.csv,
        "json": FileFormatEnum.json,
        "parquet": FileFormatEnum.parquet,
        "xlsx": FileFormatEnum.excel,
        "xls": FileFormatEnum.excel,
        "tsv": FileFormatEnum.tsv,
        "zip": FileFormatEnum.zip,
    }
    if ext not in mapping:
        raise HTTPException(status_code=400, detail=f"Unsupported file format: {ext or 'unknown'}")
    return mapping[ext]


@router.post("/file")
async def upload_file(
    file: UploadFile = File(...),
    domain: str = Form(...),
    name: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Stream upload to temporary disk, store object in MinIO, create Dataset + ProcessingJob,
    then enqueue Celery processing immediately.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    dataset_id = uuid.uuid4()
    original_filename = file.filename
    file_format = _resolve_file_format(original_filename)
    dataset_domain = _resolve_domain(domain)
    storage_key = f"raw/{current_user.id}/{dataset_id}/{original_filename}"

    chunk_size = 8 * 1024 * 1024
    file_size = 0
    temp_file_path: str | None = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{original_filename}") as temp_file:
            temp_file_path = temp_file.name
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                temp_file.write(chunk)
                file_size += len(chunk)

        if file_size == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")
        if file_size > 10 * 1024 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File exceeds 10GB limit")

        bucket = settings.normalized_minio_bucket
        minio_client.fput_object(
            bucket,
            storage_key,
            temp_file_path,
            content_type=file.content_type or "application/octet-stream",
        )

        dataset = Dataset(
            id=dataset_id,
            user_id=current_user.id,
            name=name or Path(original_filename).stem,
            domain=dataset_domain,
            original_filename=original_filename,
            storage_path=storage_key,
            file_format=file_format,
            file_size_bytes=file_size,
            status=DatasetStatusEnum.uploaded,
        )
        db.add(dataset)
        await db.flush()

        job = ProcessingJob(
            dataset_id=dataset_id,
            status=DatasetStatusEnum.queued,
            progress_pct=0.0,
            current_step="queued",
        )
        db.add(job)
        dataset.status = DatasetStatusEnum.queued
        await db.commit()
        await db.refresh(dataset)
        await db.refresh(job)

        from workers.tasks import process_dataset

        queue_name = "fast" if file_size < 100 * 1024 * 1024 else "heavy"
        process_dataset.apply_async(
            args=[str(dataset_id), {"domain": dataset_domain.value}],
            task_id=str(job.id),
            queue=queue_name,
        )

        return {
            "dataset_id": str(dataset_id),
            "job_id": str(job.id),
            "filename": original_filename,
            "file_size_bytes": file_size,
            "storage_key": storage_key,
            "status": "queued",
            "message": "File uploaded successfully. Processing has started.",
        }
    finally:
        await file.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
