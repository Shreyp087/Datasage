from __future__ import annotations

import os
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.minio_client import minio_client
from app.merge.auto_joiner import AutoJoiner, JoinCandidate, JoinType, MatchStrategy
from app.models.merge import MergeOperation
from app.models.models import Dataset, DatasetDomainEnum, DatasetStatusEnum, FileFormatEnum, User
from app.utils.parquet import prepare_dataframe_for_parquet

router = APIRouter(prefix="/merge", tags=["merge"])


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_uuid(raw: str, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field}") from exc


def _dataset_status_value(dataset: Dataset) -> str:
    if hasattr(dataset.status, "value"):
        return str(dataset.status.value)
    return str(dataset.status)


def _dataset_format_value(dataset: Dataset) -> str:
    if hasattr(dataset.file_format, "value"):
        return str(dataset.file_format.value)
    return str(dataset.file_format)


def _storage_is_parquet(storage_path: str, dataset: Dataset) -> bool:
    lowered = storage_path.lower()
    if lowered.endswith(".parquet"):
        return True
    cleaned = dataset.cleaned_storage_path or ""
    return bool(cleaned and cleaned == storage_path)


def _read_dataset_file(local_path: str, dataset: Dataset, storage_path: str) -> pd.DataFrame:
    if _storage_is_parquet(storage_path, dataset):
        return pd.read_parquet(local_path)

    file_format = _dataset_format_value(dataset)
    if file_format == FileFormatEnum.csv.value:
        return pd.read_csv(local_path, low_memory=False)
    if file_format == FileFormatEnum.tsv.value:
        return pd.read_csv(local_path, sep="\t", low_memory=False)
    if file_format == FileFormatEnum.json.value:
        try:
            return pd.read_json(local_path, lines=True)
        except ValueError:
            return pd.read_json(local_path)
    if file_format == FileFormatEnum.excel.value:
        return pd.read_excel(local_path, sheet_name=0, engine="openpyxl")

    # Fall back to pandas format inference by extension if file_format is unknown.
    return pd.read_parquet(local_path) if local_path.lower().endswith(".parquet") else pd.read_csv(local_path, low_memory=False)


async def _load_df(dataset_id: str, db: AsyncSession, user_id: uuid.UUID) -> tuple[pd.DataFrame, Dataset]:
    parsed_id = _parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(Dataset).where(Dataset.id == parsed_id, Dataset.user_id == user_id).limit(1)
    )
    dataset = result.scalars().first()
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")

    if _dataset_status_value(dataset) != DatasetStatusEnum.complete.value:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Dataset {dataset_id} is not ready (status: {_dataset_status_value(dataset)})",
        )

    storage_path = dataset.cleaned_storage_path or dataset.storage_path
    if not storage_path:
        raise HTTPException(status_code=400, detail=f"Dataset {dataset_id} has no storage path")

    suffix = ".parquet" if _storage_is_parquet(storage_path, dataset) else f".{_dataset_format_value(dataset)}"
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp_file.close()
    try:
        if not os.path.isabs(storage_path):
            minio_client.fget_object(settings.normalized_minio_bucket, storage_path, temp_file.name)
        elif os.path.exists(storage_path):
            with open(storage_path, "rb") as src, open(temp_file.name, "wb") as dst:
                dst.write(src.read())
        else:
            raise HTTPException(status_code=400, detail=f"Dataset storage path is invalid: {storage_path}")

        return _read_dataset_file(temp_file.name, dataset, storage_path), dataset
    finally:
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)


async def _save_merged_df(
    *,
    df: pd.DataFrame,
    output_name: str,
    user_id: uuid.UUID,
    domain: DatasetDomainEnum,
    db: AsyncSession,
    merge_meta: dict[str, Any],
) -> Dataset:
    dataset_id = uuid.uuid4()
    storage_key = f"clean/{user_id}/{dataset_id}/{dataset_id}_merged.parquet"

    parquet_ready = prepare_dataframe_for_parquet(df)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_file.close()
    try:
        parquet_ready.to_parquet(temp_file.name, index=False)
        minio_client.fput_object(
            settings.normalized_minio_bucket,
            storage_key,
            temp_file.name,
            content_type="application/octet-stream",
        )
        file_size = os.path.getsize(temp_file.name)
    finally:
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)

    now = _now()
    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-", " "} else "_" for ch in output_name).strip() or "merged_dataset"
    dataset = Dataset(
        id=dataset_id,
        user_id=user_id,
        name=safe_name,
        description="Merged dataset generated by Merge Studio.",
        domain=domain,
        original_filename=f"{safe_name}.parquet",
        storage_path=storage_key,
        cleaned_storage_path=storage_key,
        file_format=FileFormatEnum.parquet,
        row_count=int(len(df)),
        col_count=int(len(df.columns)),
        file_size_bytes=int(file_size),
        status=DatasetStatusEnum.complete,
        schema_json={"source": "Merge Studio", "merge": merge_meta},
        uploaded_at=now,
        completed_at=now,
    )
    db.add(dataset)
    await db.flush()
    return dataset


class DetectRequest(BaseModel):
    left_dataset_id: str = Field(..., description="UUID of the left dataset")
    right_dataset_id: str = Field(..., description="UUID of the right dataset")
    top_n: int = Field(default=8, ge=1, le=20)
    sample_rows: int = Field(default=5000, ge=100, le=50000)


class PreviewRequest(BaseModel):
    left_dataset_id: str
    right_dataset_id: str
    left_col: str
    right_col: str
    strategy: MatchStrategy
    join_type: JoinType = JoinType.LEFT
    preview_rows: int = Field(default=20, ge=1, le=200)


class ApplyRequest(BaseModel):
    left_dataset_id: str
    right_dataset_id: str
    left_col: str
    right_col: str
    strategy: MatchStrategy
    join_type: JoinType = JoinType.LEFT
    output_name: str | None = None
    left_suffix: str = "_left"
    right_suffix: str = "_right"


class CandidateOut(BaseModel):
    left_col: str
    right_col: str
    join_type: str
    strategy: str
    confidence: float
    label: str
    match_count: int
    left_total: int
    right_total: int
    left_match_pct: float
    right_match_pct: float
    est_output_rows: int
    sample_matches: list[Any]
    sample_no_match: list[Any]
    merged_rows: int | None = None
    sample_nulls: list[Any] | None = None
    signals: list[dict[str, Any]]


class DetectResponse(BaseModel):
    left_dataset_id: str
    right_dataset_id: str
    left_name: str
    right_name: str
    left_rows: int
    right_rows: int
    left_cols: list[str]
    right_cols: list[str]
    candidates: list[CandidateOut]
    detected_at: str


class PreviewResponse(BaseModel):
    left_col: str
    right_col: str
    join_type: str
    strategy: str
    total_rows: int
    matched_rows: int
    left_only: int
    right_only: int
    col_conflicts: list[str]
    warnings: list[str]
    columns: list[str]
    preview: list[dict[str, Any]]


class ApplyResponse(BaseModel):
    merge_id: str
    output_dataset_id: str
    output_name: str
    merged_rows: int
    matched_rows: int
    left_only_rows: int
    right_only_rows: int
    col_conflicts: list[str]
    warnings: list[str]
    created_at: str


@router.post("/detect", response_model=DetectResponse)
async def detect_joins(
    body: DetectRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    df_left, ds_left = await _load_df(body.left_dataset_id, db, user.id)
    df_right, ds_right = await _load_df(body.right_dataset_id, db, user.id)

    auto = AutoJoiner(
        df_left,
        df_right,
        left_name=ds_left.name,
        right_name=ds_right.name,
        sample_n=body.sample_rows,
    )
    candidates = auto.detect(top_n=body.top_n)

    return DetectResponse(
        left_dataset_id=body.left_dataset_id,
        right_dataset_id=body.right_dataset_id,
        left_name=ds_left.name,
        right_name=ds_right.name,
        left_rows=int(len(df_left)),
        right_rows=int(len(df_right)),
        left_cols=[str(col) for col in df_left.columns],
        right_cols=[str(col) for col in df_right.columns],
        candidates=[CandidateOut(**candidate.to_dict()) for candidate in candidates],
        detected_at=_now().isoformat(),
    )


@router.post("/preview", response_model=PreviewResponse)
async def preview_join(
    body: PreviewRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    df_left, _ = await _load_df(body.left_dataset_id, db, user.id)
    df_right, _ = await _load_df(body.right_dataset_id, db, user.id)

    if body.left_col not in df_left.columns:
        raise HTTPException(status_code=400, detail=f"Left column not found: {body.left_col}")
    if body.right_col not in df_right.columns:
        raise HTTPException(status_code=400, detail=f"Right column not found: {body.right_col}")

    auto = AutoJoiner(df_left, df_right)
    candidate = JoinCandidate(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type,
        strategy=body.strategy,
        confidence=0.0,
    )
    result = auto.apply(candidate, join_type=body.join_type)
    preview_df = result.df.head(body.preview_rows).copy()
    preview_df = preview_df.where(preview_df.notna(), "")
    preview_rows = preview_df.astype(str).to_dict(orient="records")

    return PreviewResponse(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type.value,
        strategy=body.strategy.value,
        total_rows=int(result.merged_rows),
        matched_rows=int(result.matched_rows),
        left_only=int(result.left_only_rows),
        right_only=int(result.right_only_rows),
        col_conflicts=[str(col) for col in result.col_conflicts],
        warnings=[str(w) for w in result.warnings],
        columns=[str(col) for col in result.df.columns],
        preview=preview_rows,
    )


@router.post("/apply", response_model=ApplyResponse)
async def apply_join(
    body: ApplyRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    df_left, ds_left = await _load_df(body.left_dataset_id, db, user.id)
    df_right, ds_right = await _load_df(body.right_dataset_id, db, user.id)

    if body.left_col not in df_left.columns:
        raise HTTPException(status_code=400, detail=f"Left column not found: {body.left_col}")
    if body.right_col not in df_right.columns:
        raise HTTPException(status_code=400, detail=f"Right column not found: {body.right_col}")

    auto = AutoJoiner(df_left, df_right)
    candidate = JoinCandidate(
        left_col=body.left_col,
        right_col=body.right_col,
        join_type=body.join_type,
        strategy=body.strategy,
        confidence=0.0,
    )
    merge_result = auto.apply(
        candidate,
        join_type=body.join_type,
        suffixes=(body.left_suffix, body.right_suffix),
    )

    output_name = body.output_name or f"{ds_left.name} + {ds_right.name} [{body.join_type.value}]"
    merge_meta = {
        "left_dataset_id": body.left_dataset_id,
        "right_dataset_id": body.right_dataset_id,
        "left_col": body.left_col,
        "right_col": body.right_col,
        "join_type": body.join_type.value,
        "strategy": body.strategy.value,
        "merged_rows": int(merge_result.merged_rows),
        "matched_rows": int(merge_result.matched_rows),
    }
    output_dataset = await _save_merged_df(
        df=merge_result.df,
        output_name=output_name,
        user_id=user.id,
        domain=ds_left.domain if ds_left.domain else DatasetDomainEnum.general,
        db=db,
        merge_meta=merge_meta,
    )

    merge_id = uuid.uuid4()
    merge_op = MergeOperation(
        id=merge_id,
        user_id=user.id,
        left_dataset_id=ds_left.id,
        right_dataset_id=ds_right.id,
        output_dataset_id=output_dataset.id,
        left_col=body.left_col,
        right_col=body.right_col,
        strategy=body.strategy.value,
        join_type=body.join_type.value,
        merged_rows=int(merge_result.merged_rows),
        matched_rows=int(merge_result.matched_rows),
        left_only_rows=int(merge_result.left_only_rows),
        right_only_rows=int(merge_result.right_only_rows),
        warnings=[str(w) for w in merge_result.warnings],
        created_at=_now(),
    )
    db.add(merge_op)
    await db.commit()

    return ApplyResponse(
        merge_id=str(merge_id),
        output_dataset_id=str(output_dataset.id),
        output_name=output_name,
        merged_rows=int(merge_result.merged_rows),
        matched_rows=int(merge_result.matched_rows),
        left_only_rows=int(merge_result.left_only_rows),
        right_only_rows=int(merge_result.right_only_rows),
        col_conflicts=[str(col) for col in merge_result.col_conflicts],
        warnings=[str(w) for w in merge_result.warnings],
        created_at=_now().isoformat(),
    )


@router.get("/history")
async def merge_history(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    result = await db.execute(
        select(MergeOperation)
        .where(MergeOperation.user_id == user.id)
        .order_by(MergeOperation.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    operations = result.scalars().all()
    return [
        {
            "merge_id": str(op.id),
            "left_dataset_id": str(op.left_dataset_id),
            "right_dataset_id": str(op.right_dataset_id),
            "output_dataset_id": str(op.output_dataset_id) if op.output_dataset_id else None,
            "left_col": op.left_col,
            "right_col": op.right_col,
            "strategy": op.strategy,
            "join_type": op.join_type,
            "merged_rows": int(op.merged_rows or 0),
            "matched_rows": int(op.matched_rows or 0),
            "left_only_rows": int(op.left_only_rows or 0),
            "right_only_rows": int(op.right_only_rows or 0),
            "warnings": list(op.warnings or []),
            "created_at": op.created_at.isoformat() if op.created_at else None,
        }
        for op in operations
    ]


@router.get("/{merge_id}")
async def get_merge(
    merge_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    parsed_id = _parse_uuid(merge_id, "merge_id")
    result = await db.execute(
        select(MergeOperation).where(MergeOperation.id == parsed_id, MergeOperation.user_id == user.id).limit(1)
    )
    op = result.scalars().first()
    if not op:
        raise HTTPException(status_code=404, detail="Merge operation not found")

    return {
        "merge_id": str(op.id),
        "user_id": str(op.user_id),
        "left_dataset_id": str(op.left_dataset_id),
        "right_dataset_id": str(op.right_dataset_id),
        "output_dataset_id": str(op.output_dataset_id) if op.output_dataset_id else None,
        "left_col": op.left_col,
        "right_col": op.right_col,
        "strategy": op.strategy,
        "join_type": op.join_type,
        "merged_rows": int(op.merged_rows or 0),
        "matched_rows": int(op.matched_rows or 0),
        "left_only_rows": int(op.left_only_rows or 0),
        "right_only_rows": int(op.right_only_rows or 0),
        "warnings": list(op.warnings or []),
        "created_at": op.created_at.isoformat() if op.created_at else None,
    }
