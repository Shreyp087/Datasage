import os
import tempfile
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from app.api.deps import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.minio_client import minio_client
from app.models.models import AgentReport, Dataset, EDAReport, ProcessingJob, User
from app.models.notebook import Notebook
from app.notebooks.readme_generator import ReadmeGenerator

router = APIRouter(tags=["datasets"])


def _readme_html_document(*, dataset_name: str, body_html: str) -> str:
    safe_name = "".join(ch for ch in str(dataset_name or "Dataset") if ch.isprintable())
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{safe_name} · README</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Merriweather:wght@700;900&family=Source+Sans+3:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
    :root {{
      --bg: #f6f8fb;
      --card: #ffffff;
      --text: #17202a;
      --muted: #4a5568;
      --accent: #0e5d8f;
      --border: #d7dfe7;
      --code-bg: #f3f6fa;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at 20% 0%, #eef6ff 0%, var(--bg) 45%);
      color: var(--text);
      font-family: 'Source Sans 3', 'Segoe UI', Arial, sans-serif;
      line-height: 1.7;
      padding: 2rem 1rem 3rem;
    }}
    main {{
      max-width: 980px;
      margin: 0 auto;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      box-shadow: 0 10px 28px rgba(9, 30, 66, 0.08);
      padding: 2rem;
    }}
    h1, h2, h3, h4 {{
      font-family: 'Merriweather', Georgia, serif;
      color: #0b2f4a;
      line-height: 1.3;
      margin-top: 1.2em;
    }}
    h1 {{ font-size: 2rem; margin-top: 0.2em; }}
    h2 {{ font-size: 1.45rem; border-bottom: 1px solid #e9eef5; padding-bottom: 0.35rem; }}
    p, li {{ font-size: 1.05rem; }}
    code, pre {{
      font-family: 'JetBrains Mono', 'Consolas', monospace;
      font-size: 0.92rem;
      background: var(--code-bg);
    }}
    pre {{
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 0.9rem;
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 1rem 0;
      font-size: 0.98rem;
    }}
    th, td {{
      border: 1px solid var(--border);
      padding: 0.55rem 0.7rem;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #eaf2fb;
      color: #17384f;
      font-weight: 700;
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
      border-bottom: 1px solid rgba(14, 93, 143, 0.25);
    }}
    blockquote {{
      margin: 1rem 0;
      padding: 0.75rem 1rem;
      border-left: 4px solid #7aa7c8;
      background: #f5faff;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <main>
    {body_html}
  </main>
</body>
</html>"""


def _cleanup_files(paths: list[str]) -> None:
    for path in paths:
        if path and os.path.exists(path):
            os.remove(path)


def _serialize_datetime(value: datetime | None) -> str | None:
    if not value:
        return None
    return value.isoformat()


def _dataset_to_dict(dataset: Dataset) -> dict[str, Any]:
    return {
        "id": str(dataset.id),
        "user_id": str(dataset.user_id),
        "name": dataset.name,
        "description": dataset.description,
        "domain": dataset.domain.value if hasattr(dataset.domain, "value") else str(dataset.domain),
        "original_filename": dataset.original_filename,
        "storage_path": dataset.storage_path,
        "cleaned_storage_path": dataset.cleaned_storage_path,
        "file_format": dataset.file_format.value if hasattr(dataset.file_format, "value") else str(dataset.file_format),
        "row_count": dataset.row_count,
        "col_count": dataset.col_count,
        "file_size_bytes": dataset.file_size_bytes,
        "status": dataset.status.value if hasattr(dataset.status, "value") else str(dataset.status),
        "schema_json": dataset.schema_json,
        "uploaded_at": _serialize_datetime(dataset.uploaded_at),
        "completed_at": _serialize_datetime(dataset.completed_at),
    }


async def get_dataset_or_404(dataset_id: str, user_id: uuid.UUID, db: AsyncSession) -> Dataset:
    try:
        parsed_id = uuid.UUID(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid dataset id") from exc

    result = await db.execute(
        select(Dataset).where(Dataset.id == parsed_id, Dataset.user_id == user_id).limit(1)
    )
    dataset = result.scalars().first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


@router.get("/datasets/")
async def list_datasets(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    skip: int = 0,
    limit: int = 50,
    status: str | None = Query(default=None),
):
    query = select(Dataset).where(Dataset.user_id == current_user.id)
    if status:
        query = query.where(Dataset.status == status)
    query = query.order_by(Dataset.uploaded_at.desc()).offset(skip).limit(limit)

    result = await db.execute(query)
    datasets = result.scalars().all()
    return [_dataset_to_dict(dataset) for dataset in datasets]


@router.get("/datasets/{dataset_id}")
async def get_dataset(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)
    return _dataset_to_dict(dataset)


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)
    await db.delete(dataset)
    await db.commit()
    return {"status": "deleted", "dataset_id": dataset_id}


@router.get("/datasets/{dataset_id}/reports")
async def get_agent_reports(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)
    result = await db.execute(
        select(AgentReport)
        .where(AgentReport.dataset_id == dataset.id)
        .order_by(AgentReport.created_at.asc())
    )
    reports = result.scalars().all()
    return [
        {
            "id": str(report.id),
            "dataset_id": str(report.dataset_id),
            "agent_name": report.agent_name,
            "agent_role": report.agent_role,
            "report_markdown": report.report_markdown,
            "structured_json": report.structured_json,
            "tokens_used": report.tokens_used,
            "model_used": report.model_used,
            "provider": report.provider,
            "created_at": _serialize_datetime(report.created_at),
        }
        for report in reports
    ]


@router.get("/datasets/{dataset_id}/eda")
async def get_eda_report(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)
    result = await db.execute(
        select(EDAReport)
        .where(EDAReport.dataset_id == dataset.id)
        .order_by(EDAReport.created_at.desc())
        .limit(1)
    )
    report = result.scalars().first()
    if not report:
        raise HTTPException(status_code=404, detail="EDA report not generated yet. Check processing status.")

    if not report.html_report_path:
        raise HTTPException(status_code=404, detail="EDA HTML report path is missing.")

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    temp_file.close()

    try:
        minio_client.fget_object(
            settings.normalized_minio_bucket,
            report.html_report_path,
            temp_file.name,
        )
        with open(temp_file.name, "r", encoding="utf-8") as handle:
            html_content = handle.read()
    finally:
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)

    profile_stats = report.profile_stats or {}
    return {
        "dataset_id": str(report.dataset_id),
        "html_report": html_content,
        "json_summary": report.json_summary,
        "quality_score": profile_stats.get("quality_score"),
        "generated_at": _serialize_datetime(report.created_at),
    }


@router.get("/datasets/{dataset_id}/job")
async def get_dataset_job(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)
    result = await db.execute(
        select(ProcessingJob)
        .where(ProcessingJob.dataset_id == dataset.id)
        .order_by(ProcessingJob.created_at.desc())
        .limit(1)
    )
    job = result.scalars().first()
    if not job:
        raise HTTPException(status_code=404, detail="Processing job not found")
    return {
        "id": str(job.id),
        "dataset_id": str(job.dataset_id),
        "status": job.status.value if hasattr(job.status, "value") else str(job.status),
        "progress_pct": float(job.progress_pct or 0),
        "current_step": job.current_step,
        "error_message": job.error_message,
        "started_at": _serialize_datetime(job.started_at),
        "completed_at": _serialize_datetime(job.completed_at),
        "created_at": _serialize_datetime(job.created_at),
    }


@router.get("/datasets/{dataset_id}/download")
async def download_dataset(
    dataset_id: str,
    format: str = "csv",
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)

    if not dataset.cleaned_storage_path:
        raise HTTPException(status_code=400, detail="Dataset has not been processed yet")

    temp_parquet = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    temp_parquet.close()

    minio_client.fget_object(
        settings.normalized_minio_bucket,
        dataset.cleaned_storage_path,
        temp_parquet.name,
    )

    cleanup_paths = [temp_parquet.name]

    if format.lower() == "csv":
        df = pd.read_parquet(temp_parquet.name)
        temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_csv.close()
        df.to_csv(temp_csv.name, index=False)
        cleanup_paths.append(temp_csv.name)
        return FileResponse(
            temp_csv.name,
            filename=f"{dataset.name}_clean.csv",
            media_type="text/csv",
            background=BackgroundTask(_cleanup_files, cleanup_paths),
        )

    return FileResponse(
        temp_parquet.name,
        filename=f"{dataset.name}_clean.parquet",
        media_type="application/octet-stream",
        background=BackgroundTask(_cleanup_files, cleanup_paths),
    )


@router.get("/datasets/{dataset_id}/readme")
async def get_readme(
    dataset_id: str,
    format: str = Query("markdown", description="markdown or html"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await get_dataset_or_404(dataset_id, current_user.id, db)

    eda_result = await db.execute(
        select(EDAReport)
        .where(EDAReport.dataset_id == dataset.id)
        .order_by(EDAReport.created_at.desc())
        .limit(1)
    )
    eda = eda_result.scalars().first()

    agent_results = await db.execute(
        select(AgentReport)
        .where(AgentReport.dataset_id == dataset.id)
        .order_by(AgentReport.created_at.asc())
    )
    agents = agent_results.scalars().all()

    notebook_result = await db.execute(
        select(Notebook)
        .where(Notebook.dataset_id == dataset.id, Notebook.user_id == current_user.id)
        .order_by(Notebook.last_run_at.desc().nullslast(), Notebook.updated_at.desc())
        .limit(1)
    )
    notebook = notebook_result.scalars().first()
    notebook_results = notebook.results if notebook and isinstance(notebook.results, dict) else None

    generator = ReadmeGenerator()
    readme_md = generator.generate(
        dataset=dataset,
        eda_json=eda.json_summary if eda and isinstance(eda.json_summary, dict) else {},
        agent_reports=agents,
        notebook_results=notebook_results,
    )

    fmt = (format or "markdown").strip().lower()
    if fmt == "html":
        import markdown

        html_body = markdown.markdown(readme_md, extensions=["tables", "fenced_code"])
        html_doc = _readme_html_document(dataset_name=dataset.name, body_html=html_body)
        return HTMLResponse(content=html_doc)

    if fmt != "markdown":
        raise HTTPException(status_code=400, detail="format must be 'markdown' or 'html'")

    safe_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in dataset.name)
    return Response(
        content=readme_md,
        media_type="text/markdown",
        headers={"Content-Disposition": f"attachment; filename=README_{safe_name}.md"},
    )
