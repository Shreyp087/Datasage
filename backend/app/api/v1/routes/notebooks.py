from __future__ import annotations

import io
import json
import os
import tempfile
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.minio_client import minio_client
from app.models.models import Dataset, User
from app.models.notebook import Notebook
from app.notebooks.runner import NotebookRunner

router = APIRouter(tags=["notebooks"])


class NotebookCreateRequest(BaseModel):
    title: str
    description: str | None = None
    domain: str | None = None
    dataset_id: str | None = None
    cells: list[dict[str, Any]] = Field(default_factory=list)
    is_template: bool = False
    is_public: bool = False
    tags: list[str] = Field(default_factory=list)


class NotebookUpdateRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    domain: str | None = None
    dataset_id: str | None = None
    cells: list[dict[str, Any]] | None = None
    is_template: bool | None = None
    is_public: bool | None = None
    tags: list[str] | None = None


class NotebookExportRequest(BaseModel):
    format: Literal["html", "pdf", "jupyter"] = "html"


class NotebookCloneRequest(BaseModel):
    dataset_id: str
    title: str | None = None
    description: str | None = None


def _parse_uuid(value: str, name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {name}") from exc


def _normalize_tag_filters(tags: str | None) -> list[str]:
    if not tags:
        return []
    return [item.strip() for item in tags.split(",") if item.strip()]


def _serialize_datetime(value: datetime | None) -> str | None:
    if not value:
        return None
    return value.isoformat()


def _serialize_notebook(notebook: Notebook) -> dict[str, Any]:
    return {
        "id": str(notebook.id),
        "user_id": str(notebook.user_id),
        "dataset_id": str(notebook.dataset_id) if notebook.dataset_id else None,
        "title": notebook.title,
        "description": notebook.description,
        "domain": notebook.domain,
        "cells": notebook.cells or [],
        "results": notebook.results or {},
        "is_template": bool(notebook.is_template),
        "is_public": bool(notebook.is_public),
        "tags": notebook.tags or [],
        "snapshot_date": notebook.snapshot_date,
        "snapshot_url": notebook.snapshot_url,
        "run_count": int(notebook.run_count or 0),
        "last_run_at": _serialize_datetime(notebook.last_run_at),
        "created_at": _serialize_datetime(notebook.created_at),
        "updated_at": _serialize_datetime(notebook.updated_at),
    }


async def _get_dataset_for_user(
    db: AsyncSession,
    dataset_id: str,
    user_id: uuid.UUID,
) -> Dataset:
    parsed = _parse_uuid(dataset_id, "dataset_id")
    dataset = await db.get(Dataset, parsed)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if dataset.user_id != user_id:
        raise HTTPException(status_code=403, detail="Dataset access denied")
    return dataset


async def _get_notebook_or_404(db: AsyncSession, notebook_id: str) -> Notebook:
    parsed = _parse_uuid(notebook_id, "notebook id")
    notebook = await db.get(Notebook, parsed)
    if not notebook:
        raise HTTPException(status_code=404, detail="Notebook not found")
    return notebook


def _ensure_read_access(notebook: Notebook, user_id: uuid.UUID) -> None:
    if notebook.user_id != user_id and not notebook.is_public:
        raise HTTPException(status_code=403, detail="Notebook access denied")


def _ensure_write_access(notebook: Notebook, user_id: uuid.UUID) -> None:
    if notebook.user_id != user_id:
        raise HTTPException(status_code=403, detail="Notebook write access denied")


def _snapshot_meta_from_dataset(dataset: Dataset | None) -> tuple[str | None, str | None]:
    if not dataset or not isinstance(dataset.schema_json, dict):
        return None, None
    return dataset.schema_json.get("snapshot_date"), dataset.schema_json.get("snapshot_url")


def _reset_cell_outputs(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for cell in cells:
        item = deepcopy(cell)
        item["result"] = None
        item["executed_at"] = None
        item.pop("status", None)
        item.pop("error", None)
        cleaned.append(item)
    return cleaned


def _fill_template_placeholders(cells: list[dict[str, Any]], snapshot_date: str | None, snapshot_url: str | None) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    replacements = {
        "{snapshot_date}": snapshot_date or "latest",
        "{snapshot_url}": snapshot_url or "",
        "{generated_date}": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    for cell in cells:
        item = deepcopy(cell)
        for field in ("content", "description"):
            value = item.get(field)
            if isinstance(value, str):
                for key, replacement in replacements.items():
                    value = value.replace(key, replacement)
                item[field] = value
        rendered.append(item)
    return rendered


def _dataset_suffix(dataset: Dataset) -> str:
    if dataset.cleaned_storage_path and dataset.cleaned_storage_path.lower().endswith(".parquet"):
        return ".parquet"
    if dataset.file_format and hasattr(dataset.file_format, "value"):
        fmt = dataset.file_format.value
    else:
        fmt = str(dataset.file_format)
    return f".{fmt or 'csv'}"


def _load_dataset_dataframe(dataset: Dataset) -> pd.DataFrame:
    storage_key = dataset.cleaned_storage_path or dataset.storage_path
    if not storage_key:
        raise HTTPException(status_code=400, detail="Dataset has no storage path")

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=_dataset_suffix(dataset))
    temp_file.close()
    try:
        if not os.path.isabs(storage_key):
            minio_client.fget_object(
                settings.normalized_minio_bucket,
                storage_key,
                temp_file.name,
            )
        elif os.path.exists(storage_key):
            with open(storage_key, "rb") as src, open(temp_file.name, "wb") as dst:
                dst.write(src.read())
        else:
            raise HTTPException(status_code=400, detail=f"Invalid dataset storage path: {storage_key}")

        if (dataset.cleaned_storage_path and dataset.cleaned_storage_path.lower().endswith(".parquet")) or temp_file.name.lower().endswith(".parquet"):
            return pd.read_parquet(temp_file.name)

        fmt = dataset.file_format.value if hasattr(dataset.file_format, "value") else str(dataset.file_format)
        if fmt == "csv" or fmt == "tsv":
            sep = "\t" if fmt == "tsv" else ","
            return pd.read_csv(temp_file.name, sep=sep, low_memory=False)
        if fmt == "json":
            try:
                return pd.read_json(temp_file.name, lines=True)
            except ValueError:
                return pd.read_json(temp_file.name)
        if fmt == "excel":
            return pd.read_excel(temp_file.name, sheet_name=0, engine="openpyxl")

        raise HTTPException(status_code=400, detail=f"Unsupported dataset format for notebook run: {fmt}")
    finally:
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)


def _build_export_html(notebook: Notebook) -> str:
    rows: list[str] = [
        "<html><head><meta charset='utf-8'><title>DataSage Notebook Export</title></head><body>",
        f"<h1>{notebook.title}</h1>",
        f"<p><strong>Domain:</strong> {notebook.domain or 'general'}</p>",
        f"<p><strong>Description:</strong> {notebook.description or ''}</p>",
        "<hr/>",
    ]

    results = notebook.results or {}
    for cell in notebook.cells or []:
        cid = str(cell.get("id") or "")
        rows.append(f"<h2>{cell.get('title') or cid}</h2>")
        rows.append(f"<p><em>{cell.get('description') or ''}</em></p>")
        rows.append(
            "<pre style='background:#f5f5f5;padding:8px;border:1px solid #ddd;'>"
            + json.dumps(results.get(cid) or cell.get("result"), indent=2, default=str)
            + "</pre>"
        )
    rows.append("</body></html>")
    return "\n".join(rows)


def _build_export_pdf(notebook: Notebook) -> bytes:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except Exception as exc:
        raise HTTPException(status_code=500, detail="PDF export requires reportlab dependency") from exc

    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=letter)
    width, height = letter
    x = 40
    y = height - 40

    def line(text: str) -> None:
        nonlocal y
        if y < 50:
            pdf.showPage()
            y = height - 40
        pdf.drawString(x, y, text[:120])
        y -= 14

    line(f"DataSage Notebook: {notebook.title}")
    line(f"Domain: {notebook.domain or 'general'}")
    line("")
    for cell in notebook.cells or []:
        cell_id = str(cell.get("id") or "")
        line(f"[{cell_id}] {cell.get('title') or 'Untitled Cell'}")
        line(f"Type: {cell.get('type')} | Analysis: {cell.get('analysis_type')}")
        output = (notebook.results or {}).get(cell_id) or cell.get("result")
        compact = json.dumps(output, default=str) if output is not None else "No result"
        line(compact)
        line("")

    pdf.save()
    buf.seek(0)
    return buf.getvalue()


def _build_export_ipynb(notebook: Notebook) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    for cell in notebook.cells or []:
        cell_id = str(cell.get("id") or "")
        title = cell.get("title") or cell_id
        desc = cell.get("description") or ""
        result = (notebook.results or {}).get(cell_id) or cell.get("result")

        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {"datasage_cell_id": cell_id},
                "source": [f"## {title}\n", f"{desc}\n"],
            }
        )
        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {"datasage_analysis_type": cell.get("analysis_type")},
                "outputs": [
                    {
                        "output_type": "display_data",
                        "data": {"application/json": result or {}},
                        "metadata": {},
                    }
                ],
                "source": [
                    "# DataSage exported analysis cell\n",
                    f"# analysis_type: {cell.get('analysis_type')}\n",
                    f"config = {json.dumps(cell.get('config', {}), indent=2)}\n",
                ],
            }
        )
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.x"},
            "datasage": {
                "notebook_id": str(notebook.id),
                "title": notebook.title,
                "domain": notebook.domain,
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


@router.post("/notebooks/")
async def create_notebook(
    payload: NotebookCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset: Dataset | None = None
    if payload.dataset_id:
        dataset = await _get_dataset_for_user(db, payload.dataset_id, current_user.id)

    snapshot_date, snapshot_url = _snapshot_meta_from_dataset(dataset)
    notebook = Notebook(
        user_id=current_user.id,
        dataset_id=dataset.id if dataset else None,
        title=payload.title.strip(),
        description=payload.description,
        domain=(payload.domain or "").strip().lower() or None,
        cells=payload.cells,
        results={},
        is_template=payload.is_template,
        is_public=payload.is_public,
        tags=payload.tags,
        snapshot_date=snapshot_date,
        snapshot_url=snapshot_url,
    )
    db.add(notebook)
    await db.commit()
    await db.refresh(notebook)
    return _serialize_notebook(notebook)


@router.get("/notebooks/")
async def list_notebooks(
    domain: str | None = Query(default=None),
    is_template: bool | None = Query(default=None),
    tags: str | None = Query(default=None, description="Comma-separated tags"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    filters = [or_(Notebook.user_id == current_user.id, and_(Notebook.is_public.is_(True), Notebook.is_template.is_(True)))]
    if domain:
        filters.append(Notebook.domain == domain.strip().lower())
    if is_template is not None:
        filters.append(Notebook.is_template.is_(is_template))

    tag_filter = _normalize_tag_filters(tags)
    if tag_filter:
        filters.append(Notebook.tags.overlap(tag_filter))

    result = await db.execute(
        select(Notebook)
        .where(and_(*filters))
        .order_by(Notebook.updated_at.desc().nullslast(), Notebook.created_at.desc())
    )
    notebooks = result.scalars().all()
    return [_serialize_notebook(notebook) for notebook in notebooks]


@router.get("/notebooks/templates")
async def list_template_notebooks(
    domain: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    filters = [Notebook.is_template.is_(True), Notebook.is_public.is_(True)]
    if domain:
        filters.append(Notebook.domain == domain.strip().lower())
    result = await db.execute(select(Notebook).where(and_(*filters)).order_by(Notebook.created_at.desc()))
    templates = result.scalars().all()
    return [_serialize_notebook(template) for template in templates]


@router.get("/notebooks/{notebook_id}")
async def get_notebook(
    notebook_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    notebook = await _get_notebook_or_404(db, notebook_id)
    _ensure_read_access(notebook, current_user.id)
    return _serialize_notebook(notebook)


@router.post("/notebooks/{notebook_id}/run")
async def run_notebook_cells(
    notebook_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    notebook = await _get_notebook_or_404(db, notebook_id)
    _ensure_write_access(notebook, current_user.id)
    if not notebook.dataset_id:
        raise HTTPException(status_code=400, detail="Notebook is not linked to a dataset")

    dataset = await db.get(Dataset, notebook.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Linked dataset not found")
    if dataset.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Dataset access denied")

    from workers.tasks import run_notebook

    task = run_notebook.apply_async(
        args=[str(notebook.id), str(dataset.id)],
        queue="fast",
    )
    return {"job_id": str(task.id)}


@router.post("/notebooks/{notebook_id}/run/{cell_id}")
async def run_single_cell(
    notebook_id: str,
    cell_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    notebook = await _get_notebook_or_404(db, notebook_id)
    _ensure_write_access(notebook, current_user.id)

    if not notebook.dataset_id:
        raise HTTPException(status_code=400, detail="Notebook is not linked to a dataset")
    dataset = await db.get(Dataset, notebook.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Linked dataset not found")
    if dataset.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Dataset access denied")

    cell = next((item for item in (notebook.cells or []) if str(item.get("id")) == cell_id), None)
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    df = _load_dataset_dataframe(dataset)
    runner = NotebookRunner()
    result = runner.run_cell(cell, df)
    executed_at = datetime.now(timezone.utc).isoformat()
    outcome = {
        "status": "success",
        "result": result,
        "executed_at": executed_at,
    }

    all_results = dict(notebook.results or {})
    all_results[cell_id] = outcome
    notebook.results = all_results
    notebook.cells = runner.annotate_cells_with_results(notebook.cells or [], {cell_id: outcome})
    notebook.run_count = int(notebook.run_count or 0) + 1
    notebook.last_run_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(notebook)

    return {"result": result}


@router.put("/notebooks/{notebook_id}")
async def update_notebook(
    notebook_id: str,
    payload: NotebookUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    notebook = await _get_notebook_or_404(db, notebook_id)
    _ensure_write_access(notebook, current_user.id)

    if payload.dataset_id is not None:
        if payload.dataset_id:
            dataset = await _get_dataset_for_user(db, payload.dataset_id, current_user.id)
            notebook.dataset_id = dataset.id
            snapshot_date, snapshot_url = _snapshot_meta_from_dataset(dataset)
            notebook.snapshot_date = snapshot_date
            notebook.snapshot_url = snapshot_url
        else:
            notebook.dataset_id = None

    if payload.title is not None:
        notebook.title = payload.title.strip()
    if payload.description is not None:
        notebook.description = payload.description
    if payload.domain is not None:
        notebook.domain = payload.domain.strip().lower() or None
    if payload.cells is not None:
        notebook.cells = payload.cells
    if payload.is_template is not None:
        notebook.is_template = payload.is_template
    if payload.is_public is not None:
        notebook.is_public = payload.is_public
    if payload.tags is not None:
        notebook.tags = payload.tags

    await db.commit()
    await db.refresh(notebook)
    return _serialize_notebook(notebook)


@router.post("/notebooks/{notebook_id}/export")
async def export_notebook(
    notebook_id: str,
    payload: NotebookExportRequest | None = Body(default=None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    notebook = await _get_notebook_or_404(db, notebook_id)
    _ensure_read_access(notebook, current_user.id)
    export_format = (payload.format if payload else "html").lower()

    basename = f"notebook_{notebook.id}"
    if export_format == "html":
        body = _build_export_html(notebook).encode("utf-8")
        media_type = "text/html"
        filename = f"{basename}.html"
    elif export_format == "pdf":
        body = _build_export_pdf(notebook)
        media_type = "application/pdf"
        filename = f"{basename}.pdf"
    else:
        body = json.dumps(_build_export_ipynb(notebook), ensure_ascii=False, indent=2, default=str).encode("utf-8")
        media_type = "application/x-ipynb+json"
        filename = f"{basename}.ipynb"

    stream = io.BytesIO(body)
    return StreamingResponse(
        stream,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/notebooks/from-template/{template_id}")
async def clone_notebook_template(
    template_id: str,
    payload: NotebookCloneRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    template = await _get_notebook_or_404(db, template_id)
    if not template.is_template:
        raise HTTPException(status_code=400, detail="Source notebook is not a template")
    if template.user_id != current_user.id and not template.is_public:
        raise HTTPException(status_code=403, detail="Template access denied")

    dataset = await _get_dataset_for_user(db, payload.dataset_id, current_user.id)
    snapshot_date, snapshot_url = _snapshot_meta_from_dataset(dataset)

    clone = Notebook(
        user_id=current_user.id,
        dataset_id=dataset.id,
        title=(payload.title or f"{template.title} (Copy)").strip(),
        description=payload.description if payload.description is not None else template.description,
        domain=template.domain,
        cells=_fill_template_placeholders(
            _reset_cell_outputs(template.cells or []),
            snapshot_date=snapshot_date,
            snapshot_url=snapshot_url,
        ),
        results={},
        is_template=False,
        is_public=False,
        tags=list(template.tags or []),
        snapshot_date=snapshot_date,
        snapshot_url=snapshot_url,
    )
    db.add(clone)
    await db.commit()
    await db.refresh(clone)
    return _serialize_notebook(clone)
