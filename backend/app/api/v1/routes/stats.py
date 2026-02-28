from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.database import get_db
from app.models.models import Dataset, DatasetStatusEnum, User

router = APIRouter(tags=["stats"])


@router.get("/stats/overview")
async def get_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    total_datasets = await db.scalar(
        select(func.count(Dataset.id)).where(Dataset.user_id == current_user.id)
    )
    total_storage = await db.scalar(
        select(func.sum(Dataset.file_size_bytes)).where(Dataset.user_id == current_user.id)
    )
    total_storage = total_storage or 0

    processing_statuses = [
        DatasetStatusEnum.preprocessing,
        DatasetStatusEnum.eda_running,
        DatasetStatusEnum.agents_running,
        DatasetStatusEnum.queued,
    ]
    processing_count = await db.scalar(
        select(func.count(Dataset.id)).where(
            Dataset.user_id == current_user.id,
            Dataset.status.in_(processing_statuses),
        )
    )
    complete_count = await db.scalar(
        select(func.count(Dataset.id)).where(
            Dataset.user_id == current_user.id,
            Dataset.status == DatasetStatusEnum.complete,
        )
    )

    return {
        "total_datasets": int(total_datasets or 0),
        "storage_used_bytes": int(total_storage),
        "storage_used_mb": round(float(total_storage) / (1024 * 1024), 2),
        "processing_count": int(processing_count or 0),
        "complete_count": int(complete_count or 0),
        "reports_generated": int(complete_count or 0),
    }
