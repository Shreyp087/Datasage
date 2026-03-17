"""
DataSage Merge Studio — FastAPI Routes
=======================================
Endpoints:
  POST /merge/detect          — auto-detect join candidates for two datasets
  POST /merge/preview         — preview a specific join candidate
  POST /merge/apply           — apply join and save merged dataset
  GET  /merge/history         — list past merge operations for current user
  GET  /merge/{merge_id}      — get merge operation detail
  DELETE /merge/{merge_id}    — delete a saved merge
"""

from __future__ import annotations

import io
import uuid
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.auth import get_current_user
from app.models.dataset import Dataset
from app.models.merge import MergeOperation
from app.core.minio_client import get_minio_client
from app.merge.auto_joiner import (
    AutoJoiner, JoinType, MatchStrategy, load_dataset
)

router = APIRouter(prefix="/merge", tags=["Merge Studio"])


# ── Request / Response Schemas ────────────────────────────────────────────────

class DetectRequest(BaseModel):
    left_dataset_id:  str = Field(..., description="UUID of the left dataset")
    right_dataset_id: str = Field(..., description="UUID of the right dataset")
    top_n:            int = Field(default=8, ge=1, le=20)
    sample_rows:      int = Field(default=5000, ge=100, le=50000)


class PreviewRequest(BaseModel):
    left_dataset_id:  str
    right_dataset_id: str
    left_col:         str
    right_col:        str
    strategy:         MatchStrategy
    join_type:        JoinType = JoinType.LEFT
    preview_rows:     int = Field(default=20, ge=1, le=200)


class ApplyRequest(BaseModel):
    left_dataset_id:  str
    right_dataset_id: str
    left_col:         str
    right_col:        str
    strategy:         MatchStrategy
    join_type:        JoinType = JoinType.LEFT
    output_name:      Optional[str] = None
    left_suffix:      str = "_left"
    right_suffix:     str = "_right"


class CandidateOut(BaseModel):
    left_col:        str
    right_col:       str
    join_type:       str
    strategy:        str
    confidence:      float
    label:           str
    match_count:     int
    left_total:      int
    right_total:     int
    left_match_pct:  float
    right_match_pct: float
    merged_rows:     int
    sample_matches:  list
    sample_nulls:    list
    signals:         list


class DetectResponse(BaseModel):
    left_dataset_id:  str
    right_dataset_id: str
    left_name:        str
    right_name:       str
    left_rows:        int
    right_rows:       int
    left_cols:        list[str]
    right_cols:       list[str]
    candidates:       list[CandidateOut]
    detected_at:      str


class PreviewResponse(BaseModel):
    left_col:      str
    right_col:     str
    join_type:     str
    strategy:      str
    total_rows:    int
    matched_rows:  int
    left_only:     int
    right_only:    int
    col_conflicts: list[str]
    warnings:      list[str]
    columns:       list[str]
    preview:       list[dict]


class ApplyResponse(BaseModel):
    merge_id:        str
    output_dataset_id: str
    output_name:     str
    merged_rows:     int
    matched_rows:    int
    left_only_rows:  int
    right_only_rows: int
    col_conflicts:   list[str]
    warnings:        list[str]
    created_at:      str


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _load_df(dataset_id: str, db: AsyncSession) -> tuple[pd.DataFrame, Dataset]:
    """Load a dataset's DataFrame from MinIO via its metadata in DB."""
    result  = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found",
        )
    if dataset.status != "complete":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset {dataset_id} is not ready (status: {dataset.status})",
        )

    # Load parquet from MinIO
    try:
        minio  = get_minio_client()
        bucket = "datasage"
        key    = dataset.parquet_key or f"datasets/{dataset_id}/data.parquet"

        response = minio.get_object(bucket, key)
        data     = response.read()
        df       = pd.read_parquet(io.BytesIO(data))
        return df, dataset

    except Exception as exc:
        # Fallback: try CSV key
        try:
            key = dataset.csv_key or f"datasets/{dataset_id}/data.csv"
            response = minio.get_object(bucket, key)
            data     = response.read()
            df       = pd.read_csv(io.BytesIO(data), low_memory=False)
            return df, dataset
        except Exception as exc2:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load dataset {dataset_id}: {exc2}",
            )


async def _save_merged_df(
    df: pd.DataFrame,
    output_name: str,
    user_id: str,
    db: AsyncSession,
) -> Dataset:
    """Save a merged DataFrame to MinIO and register as a new Dataset."""
    new_id  = str(uuid.uuid4())
    bucket  = "datasage"
    key     = f"datasets/{new_id}/data.parquet"

    # Upload to MinIO
    minio  = get_minio_client()
    buf    = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    size = len(buf.getvalue())
    buf.seek(0)
    minio.put_object(bucket, key, buf, size)

    # Register in DB
    dataset = Dataset(
        id=new_id,
        name=output_name,
        user_id=user_id,
        status="complete",
        row_count=len(df),
        col_count=len(df.columns),
        parquet_key=key,
        domain="merged",
        created_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
    )
    db.add(dataset)
    await db.commit()
    await db.refresh(dataset)
    return dataset


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/detect", response_model=DetectResponse)
async def detect_joins(
    body: DetectRequest,
    db:   AsyncSession = Depends(get_db),
    user = Depends(get_current_user),
):
    """
    Auto-detect optimal join columns between two datasets.
    Returns ranked candidates with confidence scores and preview stats.
    """
    df_left,  ds_left  = await _load_df(body.left_dataset_id,  db)
    df_right, ds_right = await _load_df(body.right_dataset_id, db)

    aj = AutoJoiner(
        df_left,
        df_right,
        left_name=ds_left.name,
        right_name=ds_right.name,
        sample_n=body.sample_rows,
    )

    candidates = aj.detect(top_n=body.top_n)

    return DetectResponse(
        left_dataset_id=body.left_dataset_id,
        right_dataset_id=body.right_dataset_id,
        left_name=ds_left.name,
        right_name=ds_right.name,
        left_rows=len(df_left),
        right_rows=len(df_right),
        left_cols=list(df_left.columns),
        right_cols=list(df_right.columns),
        candidates=[CandidateOut(**c.to_dict()) for c in candidates],
        detected_at=datetime.utcnow().isoformat(),
    )


@router.post("/preview", response_model=PreviewResponse)
async def preview_join(
    body: PreviewRequest,
    db:   AsyncSession = Depends(get_db),
    user = Depends(get_current_user),
):
    """
    Preview the result of a specific join without saving.
    Returns first N rows of merged output + quality stats.
    """
    from app.merge.auto_joiner import (
        JoinCandidate, _apply_strategy, MergeResult
    )

    df_left,  _ = await _load_df(body.left_dataset_id,  db)
    df_right, _ = await _load_df(body.right_dataset_id, db)

    aj = AutoJoiner(df_left, df_right)

    # Build a synthetic candidate from request
    from app.merge.auto_joiner import JoinCandidate
    cand = JoinCandidate(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type,
        strategy=body.strategy,
        confidence=0.0,
    )

    result = aj.apply(cand, join_type=body.join_type)
    df     = result.df

    return PreviewResponse(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type.value,
        strategy=body.strategy.value,
        total_rows=result.merged_rows,
        matched_rows=result.matched_rows,
        left_only=result.left_only_rows,
        right_only=result.right_only_rows,
        col_conflicts=result.col_conflicts,
        warnings=result.warnings,
        columns=list(df.columns),
        preview=df.head(body.preview_rows).fillna("").astype(str).to_dict("records"),
    )


@router.post("/apply", response_model=ApplyResponse)
async def apply_join(
    body: ApplyRequest,
    db:   AsyncSession = Depends(get_db),
    user = Depends(get_current_user),
):
    """
    Apply a join and save the merged dataset.
    Returns the new dataset ID and merge quality stats.
    """
    from app.merge.auto_joiner import JoinCandidate

    df_left,  ds_left  = await _load_df(body.left_dataset_id,  db)
    df_right, ds_right = await _load_df(body.right_dataset_id, db)

    aj = AutoJoiner(df_left, df_right)

    cand = JoinCandidate(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type,
        strategy=body.strategy,
        confidence=0.0,
    )

    result = aj.apply(
        cand,
        join_type=body.join_type,
        suffixes=(body.left_suffix, body.right_suffix),
    )

    # Save merged dataset
    output_name = (
        body.output_name
        or f"{ds_left.name} + {ds_right.name} [{body.join_type.value}]"
    )
    output_ds = await _save_merged_df(
        result.df, output_name, str(user.id), db
    )

    # Record merge operation
    merge_id = str(uuid.uuid4())
    merge_op = MergeOperation(
        id=merge_id,
        user_id=str(user.id),
        left_dataset_id=body.left_dataset_id,
        right_dataset_id=body.right_dataset_id,
        output_dataset_id=output_ds.id,
        left_col=body.left_col,
        right_col=body.right_col,
        strategy=body.strategy.value,
        join_type=body.join_type.value,
        merged_rows=result.merged_rows,
        matched_rows=result.matched_rows,
        left_only_rows=result.left_only_rows,
        right_only_rows=result.right_only_rows,
        warnings=result.warnings,
        created_at=datetime.utcnow(),
    )
    db.add(merge_op)
    await db.commit()

    return ApplyResponse(
        merge_id=merge_id,
        output_dataset_id=output_ds.id,
        output_name=output_name,
        merged_rows=result.merged_rows,
        matched_rows=result.matched_rows,
        left_only_rows=result.left_only_rows,
        right_only_rows=result.right_only_rows,
        col_conflicts=result.col_conflicts,
        warnings=result.warnings,
        created_at=datetime.utcnow().isoformat(),
    )


@router.get("/history")
async def merge_history(
    db:   AsyncSession = Depends(get_db),
    user = Depends(get_current_user),
    limit: int = 20,
    offset: int = 0,
):
    """List all past merge operations for the current user."""
    result = await db.execute(
        select(MergeOperation)
        .where(MergeOperation.user_id == str(user.id))
        .order_by(MergeOperation.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    ops = result.scalars().all()
    return [
        {
            "merge_id":          op.id,
            "left_dataset_id":   op.left_dataset_id,
            "right_dataset_id":  op.right_dataset_id,
            "output_dataset_id": op.output_dataset_id,
            "left_col":          op.left_col,
            "right_col":         op.right_col,
            "strategy":          op.strategy,
            "join_type":         op.join_type,
            "merged_rows":       op.merged_rows,
            "matched_rows":      op.matched_rows,
            "warnings":          op.warnings,
            "created_at":        op.created_at.isoformat(),
        }
        for op in ops
    ]


@router.get("/{merge_id}")
async def get_merge(
    merge_id: str,
    db:       AsyncSession = Depends(get_db),
    user      = Depends(get_current_user),
):
    """Get details of a specific merge operation."""
    result = await db.execute(
        select(MergeOperation).where(
            MergeOperation.id == merge_id,
            MergeOperation.user_id == str(user.id),
        )
    )
    op = result.scalar_one_or_none()
    if not op:
        raise HTTPException(status_code=404, detail="Merge operation not found")
    return op


@router.delete("/{merge_id}", status_code=204)
async def delete_merge(
    merge_id: str,
    db:       AsyncSession = Depends(get_db),
    user      = Depends(get_current_user),
):
    """Delete a merge operation record (does not delete the output dataset)."""
    result = await db.execute(
        select(MergeOperation).where(
            MergeOperation.id == merge_id,
            MergeOperation.user_id == str(user.id),
        )
    )
    op = result.scalar_one_or_none()
    if not op:
        raise HTTPException(status_code=404, detail="Merge operation not found")

    await db.execute(
        delete(MergeOperation).where(MergeOperation.id == merge_id)
    )
    await db.commit()
