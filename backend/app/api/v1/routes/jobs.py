import asyncio
import json
import uuid

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.models import ProcessingJob

router = APIRouter(tags=["jobs"])


@router.websocket("/ws/jobs/{job_id}")
async def job_progress_ws(websocket: WebSocket, job_id: str) -> None:
    await websocket.accept()
    redis_client = aioredis.from_url(settings.redis_url)

    try:
        while True:
            data = await redis_client.get(f"job:progress:{job_id}")
            if data:
                progress = json.loads(data)
                await websocket.send_json(progress)
                if progress.get("step") in {"complete", "failed"}:
                    break
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
    finally:
        await redis_client.aclose()


@router.get("/jobs/{job_id}/progress")
async def get_job_progress(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    redis_client = aioredis.from_url(settings.redis_url)
    try:
        data = await redis_client.get(f"job:progress:{job_id}")
    finally:
        await redis_client.aclose()

    if data:
        return json.loads(data)

    try:
        parsed_job_id = uuid.UUID(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid job id") from exc

    job = await db.get(ProcessingJob, parsed_job_id)
    if job:
        return {
            "pct": float(job.progress_pct or 0),
            "step": job.current_step or (job.status.value if hasattr(job.status, "value") else str(job.status)),
            "message": job.error_message or "",
        }

    raise HTTPException(status_code=404, detail="Job not found")
