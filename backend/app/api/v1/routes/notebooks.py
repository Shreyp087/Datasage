from __future__ import annotations

import io
import json
import os
import tempfile
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from html import escape
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
from app.notebooks.templates.dynamic_template import build_dynamic_notebook_template

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


class NotebookGenerateRequest(BaseModel):
    title: str | None = None
    description: str | None = None
    run_now: bool = True
    replace_existing: bool = True


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


def _build_dynamic_notebook_cells(dataset: Dataset, df: pd.DataFrame) -> dict[str, Any]:
    snapshot_date, snapshot_url = _snapshot_meta_from_dataset(dataset)
    domain = (
        dataset.domain.value
        if hasattr(dataset.domain, "value")
        else str(dataset.domain or "general")
    )
    return build_dynamic_notebook_template(
        dataset_name=dataset.name or "Uploaded Dataset",
        domain=domain,
        df=df,
        snapshot_date=snapshot_date,
        snapshot_url=snapshot_url,
    )


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
        "<!doctype html>",
        "<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>DataSage Notebook Export</title>",
        "<style>",
        "@import url('https://fonts.googleapis.com/css2?family=Merriweather:wght@700;900&family=Source+Sans+3:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');",
        "body{margin:0;background:radial-gradient(circle at 15% 0%,#edf6ff 0%,#f6f8fb 45%);color:#17202a;font-family:'Source Sans 3','Segoe UI',Arial,sans-serif;line-height:1.65;padding:2rem 1rem 3rem;}",
        "main{max-width:1040px;margin:0 auto;background:#fff;border:1px solid #d8e2ee;border-radius:16px;box-shadow:0 12px 30px rgba(9,30,66,.08);padding:2rem;}",
        "h1,h2{font-family:'Merriweather',Georgia,serif;color:#0d2f4a;line-height:1.3;}",
        "h1{font-size:2rem;margin:.1rem 0 .2rem;} h2{font-size:1.35rem;margin-top:1.3rem;border-bottom:1px solid #e8eef5;padding-bottom:.3rem;}",
        ".meta{color:#425466;margin:.35rem 0;font-size:1rem;} .desc{color:#2f3e4d;font-size:1.03rem;margin:.65rem 0 1rem;}",
        ".output{margin:.5rem 0 1.2rem;padding:.85rem;border:1px solid #d8e2ee;border-radius:10px;background:#f4f7fb;}",
        "pre{margin:0;white-space:pre-wrap;word-break:break-word;font-family:'JetBrains Mono','Consolas',monospace;font-size:.9rem;line-height:1.5;}",
        "code{font-family:'JetBrains Mono','Consolas',monospace;background:#ecf2f9;padding:.08rem .3rem;border-radius:5px;}",
        "</style></head><body><main>",
        f"<h1>{escape(str(notebook.title or 'Notebook Export'))}</h1>",
        f"<p class='meta'><strong>Domain:</strong> {escape(str(notebook.domain or 'general'))}</p>",
        f"<p class='desc'><strong>Description:</strong> {escape(str(notebook.description or ''))}</p>",
    ]

    results = notebook.results or {}
    for cell in notebook.cells or []:
        cid = str(cell.get("id") or "")
        rows.append(f"<h2>{escape(str(cell.get('title') or cid))}</h2>")
        rows.append(f"<p class='meta'><em>{escape(str(cell.get('description') or ''))}</em></p>")
        outcome = results.get(cid) or cell.get("result")
        payload = outcome.get("result") if isinstance(outcome, dict) and "result" in outcome else outcome

        if isinstance(payload, dict) and payload.get("type") == "text":
            text_content = str(payload.get("content") or "")
            rows.append(
                "<div class='output'><pre>"
                + escape(text_content)
                + "</pre></div>"
            )
            continue

        if isinstance(payload, dict) and payload.get("type") == "narrative":
            text_content = str(payload.get("summary_markdown") or "")
            rows.append(
                "<div class='output'><pre>"
                + escape(text_content)
                + "</pre></div>"
            )
            continue

        if isinstance(payload, dict) and payload.get("type") == "text_list":
            samples = payload.get("samples") or []
            safe_lines = "\n".join([f"- {str(sample)}" for sample in samples])
            rows.append(
                "<div class='output'><pre>"
                + escape(safe_lines)
                + "</pre></div>"
            )
            continue

        if payload is None and isinstance(cell.get("content"), str):
            rows.append(
                "<div class='output'><pre>"
                + escape(str(cell.get("content") or ""))
                + "</pre></div>"
            )
            continue

        rendered = json.dumps(outcome, indent=2, default=str)
        rows.append(
            "<div class='output'><pre>"
            + escape(rendered)
            + "</pre></div>"
        )
    rows.append("</main></body></html>")
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


def _source_lines(text: str) -> list[str]:
    if not text:
        return []
    lines = text.splitlines()
    return [f"{line}\n" for line in lines]


def _analysis_code_for_export(analysis_type: str, config: dict[str, Any]) -> str:
    atype = (analysis_type or "").strip().lower()
    config_json = json.dumps(config or {}, ensure_ascii=False)
    header = [
        "# DataSage reproducible analysis cell",
        f"analysis_type = '{atype}'",
        f"config = json.loads(r'''{config_json}''')",
        "",
    ]

    if atype == "summary":
        body = [
            "summary = {'rows': int(len(df)), 'columns': int(len(df.columns))}",
            "date_field = config.get('date_field')",
            "if date_field and date_field in df.columns:",
            "    parsed = pd.to_datetime(df[date_field], errors='coerce')",
            "    if parsed.notna().any():",
            "        summary['earliest'] = str(parsed.min())",
            "        summary['latest'] = str(parsed.max())",
            "top_values = {}",
            "for field in config.get('top_fields', []):",
            "    if field in df.columns:",
            "        non_null = df[field].dropna()",
            "        top_values[field] = non_null.value_counts().index[0] if not non_null.empty else None",
            "summary['top_values'] = top_values",
            "summary",
        ]
    elif atype == "trend":
        body = [
            "x_field = config.get('x_field', 'year')",
            "group_by = config.get('group_by')",
            "if x_field not in df.columns:",
            "    raise KeyError(f\"Column '{x_field}' not found\")",
            "if group_by and group_by in df.columns:",
            "    trend_df = df.groupby([x_field, group_by], dropna=False).size().reset_index(name='count')",
            "else:",
            "    trend_df = df.groupby(x_field, dropna=False).size().reset_index(name='count')",
            "trend_df = trend_df.sort_values(x_field)",
            "display(trend_df.head(25))",
            "if group_by and group_by in trend_df.columns:",
            "    plot_df = trend_df.pivot(index=x_field, columns=group_by, values='count').fillna(0)",
            "    plot_df.plot(figsize=(10, 4), marker='o')",
            "else:",
            "    trend_df.plot(x=x_field, y='count', kind='line', figsize=(10, 4), marker='o')",
            "plt.title('Trend')",
            "plt.tight_layout()",
            "plt.show()",
        ]
    elif atype == "distribution":
        body = [
            "field = config.get('field')",
            "top_n = int(config.get('top_n', 15))",
            "if not field or field not in df.columns:",
            "    raise KeyError(f\"Column '{field}' not found\")",
            "dist = df[field].value_counts(dropna=False).head(top_n).rename_axis(field).reset_index(name='count')",
            "display(dist)",
            "chart_type = str(config.get('chart_type', 'bar')).lower()",
            "if chart_type in {'horizontal_bar', 'barh'}:",
            "    dist.plot(kind='barh', x=field, y='count', figsize=(10, 4))",
            "else:",
            "    dist.plot(kind='bar', x=field, y='count', figsize=(10, 4))",
            "plt.title(f'Distribution: {field}')",
            "plt.tight_layout()",
            "plt.show()",
        ]
    elif atype == "top_n":
        body = [
            "field = config.get('field')",
            "n = int(config.get('n', 10))",
            "if not field or field not in df.columns:",
            "    raise KeyError(f\"Column '{field}' not found\")",
            "top_df = df[field].value_counts(dropna=False).head(n).rename_axis('entity').reset_index(name='count')",
            "display(top_df)",
            "top_df.plot(kind='barh', x='entity', y='count', figsize=(10, 4))",
            "plt.title(f'Top {n}: {field}')",
            "plt.tight_layout()",
            "plt.show()",
        ]
    elif atype == "heatmap":
        body = [
            "row_field = config.get('row_field')",
            "col_field = config.get('col_field')",
            "top_n = int(config.get('top_n', 8))",
            "if row_field not in df.columns or col_field not in df.columns:",
            "    raise KeyError('row_field or col_field not found')",
            "top_rows = df[row_field].value_counts(dropna=True).head(top_n).index",
            "top_cols = df[col_field].value_counts(dropna=True).head(top_n).index",
            "filtered = df[df[row_field].isin(top_rows) & df[col_field].isin(top_cols)]",
            "pivot = pd.crosstab(filtered[row_field], filtered[col_field])",
            "display(pivot)",
            "if not pivot.empty:",
            "    plt.figure(figsize=(8, 6))",
            "    plt.imshow(pivot.values, aspect='auto')",
            "    plt.xticks(range(len(pivot.columns)), [str(c) for c in pivot.columns], rotation=45, ha='right')",
            "    plt.yticks(range(len(pivot.index)), [str(i) for i in pivot.index])",
            "    plt.title(f'Heatmap: {row_field} x {col_field}')",
            "    plt.colorbar()",
            "    plt.tight_layout()",
            "    plt.show()",
        ]
    elif atype == "correlation":
        body = [
            "x_field = config.get('x_field')",
            "y_field = config.get('y_field')",
            "if x_field and y_field:",
            "    pair = df[[x_field, y_field]].copy()",
            "    pair[x_field] = pd.to_numeric(pair[x_field], errors='coerce')",
            "    pair[y_field] = pd.to_numeric(pair[y_field], errors='coerce')",
            "    pair = pair.dropna()",
            "    corr = pair[x_field].corr(pair[y_field]) if len(pair) > 1 else None",
            "    {'x_field': x_field, 'y_field': y_field, 'correlation': corr, 'sample_size': int(len(pair))}",
            "else:",
            "    numeric_df = df.select_dtypes(include=['number'])",
            "    corr_matrix = numeric_df.corr() if not numeric_df.empty else pd.DataFrame()",
            "    display(corr_matrix)",
        ]
    elif atype == "comparison":
        body = [
            "x_field = config.get('x_field')",
            "y_field = config.get('y_field')",
            "agg = str(config.get('agg', 'count')).lower()",
            "top_n = int(config.get('top_n', 20))",
            "if not x_field or x_field not in df.columns:",
            "    raise KeyError(f\"Column '{x_field}' not found\")",
            "if not y_field or y_field == 'count':",
            "    comp_df = df.groupby(x_field, dropna=False).size().reset_index(name='count').sort_values('count', ascending=False).head(top_n)",
            "    display(comp_df)",
            "else:",
            "    num = pd.to_numeric(df[y_field], errors='coerce')",
            "    tmp = pd.DataFrame({x_field: df[x_field], y_field: num})",
            "    if agg == 'sum':",
            "        comp_df = tmp.groupby(x_field, dropna=False)[y_field].sum(min_count=1).reset_index()",
            "    elif agg == 'median':",
            "        comp_df = tmp.groupby(x_field, dropna=False)[y_field].median().reset_index()",
            "    else:",
            "        comp_df = tmp.groupby(x_field, dropna=False)[y_field].mean().reset_index()",
            "    comp_df = comp_df.sort_values(y_field, ascending=False).head(top_n)",
            "    display(comp_df)",
        ]
    elif atype == "text_sample":
        body = [
            "field = config.get('field', 'title')",
            "n = int(config.get('n', 10))",
            "if field not in df.columns:",
            "    raise KeyError(f\"Column '{field}' not found\")",
            "samples = df[field].dropna().astype(str).sample(min(n, int(df[field].dropna().shape[0])), random_state=42) if df[field].dropna().shape[0] else pd.Series([], dtype='string')",
            "samples.to_frame(name=field)",
        ]
    elif atype == "detailed_summary":
        body = [
            "top_n = int(config.get('top_n', 5))",
            "summary = {'rows': int(len(df)), 'columns': int(len(df.columns))}",
            "for field_key in ['harm_field', 'sector_field', 'deployer_field', 'developer_field']:",
            "    field = config.get(field_key)",
            "    if field and field in df.columns:",
            "        summary[field_key] = df[field].dropna().astype(str).value_counts().head(top_n).to_dict()",
            "year_field = config.get('year_field')",
            "date_field = config.get('date_field')",
            "if year_field and year_field in df.columns:",
            "    years = pd.to_numeric(df[year_field], errors='coerce').dropna().astype(int)",
            "    summary['year_counts'] = years.value_counts().sort_index().to_dict()",
            "elif date_field and date_field in df.columns:",
            "    years = pd.to_datetime(df[date_field], errors='coerce').dt.year.dropna().astype(int)",
            "    summary['year_counts'] = years.value_counts().sort_index().to_dict()",
            "summary",
        ]
    else:
        body = [
            "print('No built-in exporter script for this analysis type yet.')",
            "config",
        ]

    return "\n".join([*header, *body])


def _build_export_ipynb(notebook: Notebook) -> dict[str, Any]:
    cells: list[dict[str, Any]] = [
        {
            "cell_type": "markdown",
            "metadata": {"datasage_style": True},
            "source": [
                "<style>\n",
                "@import url('https://fonts.googleapis.com/css2?family=Merriweather:wght@700;900&family=Source+Sans+3:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');\n",
                ".jp-RenderedMarkdown, .jp-MarkdownOutput { font-family: 'Source Sans 3', 'Segoe UI', Arial, sans-serif !important; }\n",
                ".jp-RenderedMarkdown h1, .jp-RenderedMarkdown h2, .jp-RenderedMarkdown h3, .jp-RenderedMarkdown h4 { font-family: 'Merriweather', Georgia, serif !important; }\n",
                ".jp-CodeCell .cm-content, .jp-OutputArea pre, code { font-family: 'JetBrains Mono', 'Consolas', monospace !important; }\n",
                "</style>\n",
            ],
        }
    ]
    cells.append(
        {
            "cell_type": "markdown",
            "metadata": {"datasage_intro": True},
            "source": [
                "## Data Loading Setup\n",
                "Update `DATASET_PATH` to your local file, run this cell first, then execute the analysis cells.\n",
            ],
        }
    )
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {"datasage_setup": True},
            "outputs": [],
            "source": _source_lines(
                "\n".join(
                    [
                        "import json",
                        "import pandas as pd",
                        "import matplotlib.pyplot as plt",
                        "",
                        "DATASET_PATH = 'path/to/your_uploaded_dataset.csv'",
                        "",
                        "def load_dataset(path: str) -> pd.DataFrame:",
                        "    lower = path.lower()",
                        "    if lower.endswith('.parquet'):",
                        "        return pd.read_parquet(path)",
                        "    if lower.endswith('.json'):",
                        "        try:",
                        "            return pd.read_json(path, lines=True)",
                        "        except ValueError:",
                        "            return pd.read_json(path)",
                        "    if lower.endswith('.tsv'):",
                        "        return pd.read_csv(path, sep='\\t', low_memory=False)",
                        "    return pd.read_csv(path, low_memory=False)",
                        "",
                        "df = load_dataset(DATASET_PATH)",
                        "print(f'Loaded dataset shape: {df.shape}')",
                        "df.head()",
                    ]
                )
            ),
        }
    )

    for cell in notebook.cells or []:
        cell_id = str(cell.get("id") or "")
        title = cell.get("title") or cell_id
        desc = cell.get("description") or ""
        result = (notebook.results or {}).get(cell_id) or cell.get("result")
        analysis_type = str(cell.get("analysis_type") or "")
        cell_type = str(cell.get("type") or "")

        if cell_type.lower() == "text" and not analysis_type:
            markdown_parts = [f"## {title}\n"]
            if desc:
                markdown_parts.append(f"{desc}\n")
            content = cell.get("content")
            if isinstance(content, str) and content.strip():
                markdown_parts.append(content.strip() + "\n")
            cells.append(
                {
                    "cell_type": "markdown",
                    "metadata": {"datasage_cell_id": cell_id},
                    "source": markdown_parts,
                }
            )
            continue

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
                "metadata": {"datasage_analysis_type": analysis_type},
                "outputs": [
                    {
                        "output_type": "display_data",
                        "data": {"application/json": result or {}},
                        "metadata": {},
                    }
                ] if result is not None else [],
                "source": _source_lines(_analysis_code_for_export(analysis_type, cell.get("config", {}) or {})),
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


@router.post("/notebooks/generate/{dataset_id}")
async def generate_dynamic_notebook_for_dataset(
    dataset_id: str,
    payload: NotebookGenerateRequest | None = Body(default=None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = await _get_dataset_for_user(db, dataset_id, current_user.id)
    template_payload = _build_dynamic_notebook_cells(dataset, _load_dataset_dataframe(dataset))
    snapshot_date, snapshot_url = _snapshot_meta_from_dataset(dataset)

    request_payload = payload or NotebookGenerateRequest()
    notebook_title = (request_payload.title or str(template_payload.get("title") or f"{dataset.name} Notebook")).strip()
    notebook_description = request_payload.description if request_payload.description is not None else str(
        template_payload.get("description") or "Dynamic dataset notebook generated from detected schema."
    )
    notebook_domain = str(template_payload.get("domain") or (dataset.domain.value if hasattr(dataset.domain, "value") else dataset.domain or "general")).strip().lower() or None
    notebook_tags = [str(tag) for tag in (template_payload.get("tags") or [])]
    notebook_cells = _reset_cell_outputs(template_payload.get("cells") or [])

    notebook: Notebook | None = None
    if request_payload.replace_existing:
        result = await db.execute(
            select(Notebook)
            .where(
                Notebook.user_id == current_user.id,
                Notebook.dataset_id == dataset.id,
                Notebook.is_template.is_(False),
            )
            .order_by(Notebook.updated_at.desc().nullslast(), Notebook.created_at.desc())
            .limit(1)
        )
        notebook = result.scalar_one_or_none()

    if notebook:
        notebook.title = notebook_title
        notebook.description = notebook_description
        notebook.domain = notebook_domain
        notebook.tags = notebook_tags
        notebook.cells = notebook_cells
        notebook.results = {}
        notebook.snapshot_date = snapshot_date
        notebook.snapshot_url = snapshot_url
    else:
        notebook = Notebook(
            user_id=current_user.id,
            dataset_id=dataset.id,
            title=notebook_title,
            description=notebook_description,
            domain=notebook_domain,
            cells=notebook_cells,
            results={},
            is_template=False,
            is_public=False,
            tags=notebook_tags,
            snapshot_date=snapshot_date,
            snapshot_url=snapshot_url,
        )
        db.add(notebook)

    await db.commit()
    await db.refresh(notebook)

    job_id: str | None = None
    if request_payload.run_now:
        from workers.tasks import run_notebook

        task = run_notebook.apply_async(
            args=[str(notebook.id), str(dataset.id)],
            queue="fast",
        )
        job_id = str(task.id)

    response: dict[str, Any] = {"notebook": _serialize_notebook(notebook)}
    if job_id:
        response["job_id"] = job_id
    return response


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
