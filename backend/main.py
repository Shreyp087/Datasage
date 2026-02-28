import os
import sys

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.routes import aiid, auth, datasets, jobs, merge, notebooks, stats, upload
from app.core.database import AsyncSessionLocal, get_db
from app.core.minio_client import ensure_bucket_exists
from app.notebooks.seeder import seed_aiid_template

logger.remove()
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    ),
)

app = FastAPI(
    title="DataSage Platform API",
    description="Automated ML Pipeline, EDA, and Agent Orchestration Backend",
    version="1.0.0",
)

default_origins = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]
env_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",")
    if origin.strip()
]
allowed_origins = list(dict.fromkeys(default_origins + env_origins))

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(auth.router, prefix="/api/v1")
app.include_router(upload.router, prefix="/api/v1")
app.include_router(datasets.router, prefix="/api/v1")
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(stats.router, prefix="/api/v1")
app.include_router(merge.router, prefix="/api/v1")
app.include_router(aiid.router, prefix="/api/v1", tags=["AIID"])
app.include_router(notebooks.router, prefix="/api/v1", tags=["Notebooks"])


@app.on_event("startup")
async def startup_event() -> None:
    try:
        ensure_bucket_exists()
    except Exception as exc:
        logger.warning(f"MinIO bucket ensure skipped on startup: {exc}")

    try:
        async with AsyncSessionLocal() as session:
            await seed_aiid_template(session)
            logger.info("Notebook template seed completed.")
    except Exception as exc:
        logger.warning(f"Notebook template seed skipped on startup: {exc}")

    logger.info("Initializing DataSage backend API...")


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/api/v1/health/db")
async def health_db(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        return {"status": "error", "database": str(exc)}
