from typing import List, Dict, Any
import uuid
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.schemas import MergeConfigCreate, MergeConfigResponse
from app.pipeline.merger import suggest_merge_keys, estimate_merged_size, execute_merge, MergeSuggestion
from app.models.models import Dataset

router = APIRouter(prefix="/merge", tags=["merge"])

class SuggestRequest(BaseModel):
    dataset_id_1: str
    dataset_id_2: str

class EstimateRequest(BaseModel):
    dataset_id_1: str
    dataset_id_2: str
    left_key: str
    right_key: str
    join_type: str

# Pseudo loading helper
def load_mock_df(dataset_id: str):
    # In reality, this delegates to minio + loader
    return pd.DataFrame()

@router.post("/suggest", response_model=List[MergeSuggestion])
async def suggest_keys(req: SuggestRequest):
    """
    Auto-detects merge key suitability between datasets.
    """
    df1 = load_mock_df(req.dataset_id_1)
    df2 = load_mock_df(req.dataset_id_2)
    sugg = suggest_merge_keys(df1, df2)
    return sugg

@router.post("/estimate")
async def estimate_merge(req: EstimateRequest):
    """
    Returns heuristical bounds of result rows/memory to prevent user fan-out accidents.
    """
    df1 = load_mock_df(req.dataset_id_1)
    df2 = load_mock_df(req.dataset_id_2)
    res = estimate_merged_size(df1, df2, req.left_key, req.right_key, req.join_type)
    return res

@router.post("/execute")
async def execute_merge_endpoint(req: MergeConfigCreate, db: AsyncSession = Depends(get_db)):
    """
    Queues a Celery job to actually shuffle and join files securely into a Parquet result.
    """
    # Create DB records, pseudo return
    job_id = str(uuid.uuid4())
    return {"status": "enqueued", "job_id": job_id, "message": "Merge job scheduled."}

@router.post("/preview")
async def preview_merge(req: MergeConfigCreate):
    """
    Executes an in-memory limit(100) join for immediate UI validation.
    """
    df1 = load_mock_df(str(req.left_dataset_id))
    df2 = load_mock_df(str(req.right_dataset_id))
    
    # In reality, pull only top 100 rows for preview speed
    res = execute_merge(df1, df2, req.left_key, req.right_key, req.join_type.value)
    
    if isinstance(res["df"], pd.DataFrame):
        preview_data = res["df"].head(100).to_dict(orient="records")
    else:
        preview_data = res["df"].head(100, compute=True).to_dict(orient="records")
        
    return {"preview": preview_data, "warnings": res.get("fan_out_warning")}
