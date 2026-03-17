import os
import tempfile
import uuid

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.minio_client import minio_client
from app.models.models import (
    Dataset,
    DatasetDomainEnum,
    DatasetStatusEnum,
    FileFormatEnum,
    ProcessingJob,
    User,
)
from app.pipeline.aiid_ingestor import AIIDIngestor

router = APIRouter(tags=["AIID"])


async def _persist_aiid_dataframe(
    *,
    df,
    snapshot_date: str,
    snapshot_url: str,
    tmpdir: str,
    db: AsyncSession,
    current_user: User,
):
    dataset_uuid = uuid.uuid4()
    csv_path = os.path.join(tmpdir, f"{dataset_uuid}_aiid.csv")
    df.to_csv(csv_path, index=False)

    storage_key = f"raw/{current_user.id}/{dataset_uuid}/aiid_incidents.csv"
    minio_client.fput_object(
        settings.normalized_minio_bucket,
        storage_key,
        csv_path,
        content_type="text/csv",
    )

    dataset = Dataset(
        id=dataset_uuid,
        user_id=current_user.id,
        name=f"AIID Snapshot - {snapshot_date}",
        description=(
            "AI Incident Database snapshot. "
            "Contains documented AI failures and harms."
        ),
        domain=DatasetDomainEnum.ai_incidents,
        original_filename="aiid_incidents.csv",
        storage_path=storage_key,
        file_format=FileFormatEnum.csv,
        file_size_bytes=os.path.getsize(csv_path),
        row_count=len(df),
        col_count=len(df.columns),
        status=DatasetStatusEnum.uploaded,
        schema_json={
            "snapshot_url": snapshot_url,
            "snapshot_date": snapshot_date,
            "source": "AI Incident Database",
            "citation": "McGregor, S. (2021) IAAI-21",
        },
    )
    db.add(dataset)
    await db.flush()

    job = ProcessingJob(
        dataset_id=dataset_uuid,
        status=DatasetStatusEnum.queued,
        progress_pct=0.0,
        current_step="queued",
    )
    db.add(job)
    dataset.status = DatasetStatusEnum.queued
    await db.commit()
    await db.refresh(job)

    from workers.tasks import process_dataset

    process_dataset.apply_async(
        args=[str(dataset_uuid), {"domain": DatasetDomainEnum.ai_incidents.value}],
        task_id=str(job.id),
        queue="heavy",
    )

    return {
        "dataset_id": str(dataset_uuid),
        "job_id": str(job.id),
        "incidents_loaded": len(df),
        "columns": list(df.columns),
        "snapshot_date": snapshot_date,
        "message": "AIID snapshot ingested. Full analysis queued.",
    }


@router.post("/aiid/ingest")
async def ingest_aiid_snapshot(
    snapshot_url: str = Body(
        default=AIIDIngestor.LATEST_SNAPSHOT,
        description="URL to AIID tar.bz2 snapshot",
    ),
    snapshot_date: str = Body(
        default="latest",
        description="Human label for this snapshot e.g. '2026-02-23'",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Download and ingest an AIID snapshot into DataSage.
    Creates a Dataset record with domain='ai_incidents'
    and queues the full processing pipeline.
    """
    ingestor = AIIDIngestor()

    try:
        with tempfile.TemporaryDirectory(prefix="aiid_snapshot_") as tmpdir:
            files = ingestor.fetch_and_extract(snapshot_url, tmpdir)
            df = ingestor.load_incidents_csv(files)
            df = ingestor.normalize(df)

            return await _persist_aiid_dataframe(
                df=df,
                snapshot_date=snapshot_date,
                snapshot_url=snapshot_url,
                tmpdir=tmpdir,
                db=db,
                current_user=current_user,
            )
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"AIID ingestion failed: {exc}") from exc


@router.post("/aiid/ingest/upload")
async def ingest_aiid_snapshot_upload(
    snapshot_file: UploadFile = File(..., description="AIID snapshot tar.bz2 file"),
    snapshot_date: str = Form(
        default="uploaded",
        description="Human label for this snapshot e.g. '2026-02-23'",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Upload and ingest an AIID snapshot tarball directly.
    Supports the same weekly snapshot format from incidentdatabase.ai.
    """
    ingestor = AIIDIngestor()

    filename = snapshot_file.filename or "aiid_snapshot.tar.bz2"
    lowered = filename.lower()
    if not (
        lowered.endswith(".tar.bz2")
        or lowered.endswith(".tbz2")
        or lowered.endswith(".tar")
        or lowered.endswith(".bz2")
    ):
        raise HTTPException(status_code=400, detail="Snapshot file must be a tar/tar.bz2 archive")

    try:
        with tempfile.TemporaryDirectory(prefix="aiid_snapshot_upload_") as tmpdir:
            archive_path = os.path.join(tmpdir, filename)
            bytes_written = 0
            with open(archive_path, "wb") as handle:
                while True:
                    chunk = await snapshot_file.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    bytes_written += len(chunk)

            if bytes_written == 0:
                raise HTTPException(status_code=400, detail="Uploaded snapshot file is empty")

            files = ingestor.extract_archive(archive_path, tmpdir)
            df = ingestor.load_incidents_csv(files)
            df = ingestor.normalize(df)

            return await _persist_aiid_dataframe(
                df=df,
                snapshot_date=snapshot_date,
                snapshot_url=f"uploaded://{filename}",
                tmpdir=tmpdir,
                db=db,
                current_user=current_user,
            )
    except HTTPException:
        await db.rollback()
        raise
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"AIID upload ingestion failed: {exc}") from exc
    finally:
        await snapshot_file.close()
