import os
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

import dask.dataframe as dd
import pandas as pd
from celery import Task
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.agents.orchestrator import AgentOrchestrator
from app.core.config import settings
from app.core.minio_client import minio_client
from app.eda.engine import build_html_report, compress_for_agents
from app.eda.summarizer import generate_json_summary
from app.eda.visualizer import generate_visualizations
from app.models.models import (
    AgentReport,
    Dataset,
    DatasetStatusEnum,
    EDAReport,
    ProcessingJob,
    ProcessingLog,
    SeverityEnum,
)
from app.models.notebook import Notebook
from app.notebooks.runner import NotebookRunner
from app.pipeline.loader import DatasetLoader
from app.pipeline.preprocessor import PreprocessingOrchestrator
from app.pipeline.steps.base import PipelineContext
from celery_app import celery_app
from workers.progress import update_job_progress

sync_engine = create_engine(settings.sync_database_url, pool_pre_ping=True)
SyncSessionLocal = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dataset_status(value: str) -> DatasetStatusEnum:
    return DatasetStatusEnum(value)


def _as_severity(value: str) -> SeverityEnum:
    try:
        return SeverityEnum(value)
    except Exception:
        return SeverityEnum.info


def _file_type_from_dataset(dataset: Dataset) -> dict[str, Any]:
    fmt = dataset.file_format.value if hasattr(dataset.file_format, "value") else str(dataset.file_format)
    return {
        "format": fmt,
        "delimiter": "\t" if fmt == "tsv" else ",",
        "encoding": "utf-8",
    }


def _load_dataframe_for_notebook(dataset: Dataset, temp_path: str) -> pd.DataFrame:
    storage_key = dataset.cleaned_storage_path or dataset.storage_path
    if not storage_key:
        raise ValueError("Dataset has no storage path for notebook execution")

    if not os.path.isabs(storage_key):
        minio_client.fget_object(
            settings.normalized_minio_bucket,
            storage_key,
            temp_path,
        )
    elif os.path.exists(storage_key):
        with open(storage_key, "rb") as src, open(temp_path, "wb") as dst:
            dst.write(src.read())
    else:
        raise ValueError(f"Dataset storage path is invalid: {storage_key}")

    if (dataset.cleaned_storage_path and dataset.cleaned_storage_path.lower().endswith(".parquet")) or temp_path.lower().endswith(".parquet"):
        return pd.read_parquet(temp_path)

    fmt = dataset.file_format.value if hasattr(dataset.file_format, "value") else str(dataset.file_format)
    if fmt == "csv" or fmt == "tsv":
        sep = "\t" if fmt == "tsv" else ","
        return pd.read_csv(temp_path, sep=sep, low_memory=False)
    if fmt == "json":
        try:
            return pd.read_json(temp_path, lines=True)
        except ValueError:
            return pd.read_json(temp_path)
    if fmt == "excel":
        return pd.read_excel(temp_path, sheet_name=0, engine="openpyxl")
    raise ValueError(f"Unsupported dataset format for notebook execution: {fmt}")


def update_db_status(
    dataset_id: str,
    status: str,
    job_id: str | None = None,
    progress_pct: float | None = None,
    step: str | None = None,
    error: str | None = None,
    tb: str | None = None,
) -> None:
    dataset_uuid = uuid.UUID(dataset_id)
    with SyncSessionLocal() as session:
        dataset = session.get(Dataset, dataset_uuid)
        if dataset:
            dataset.status = _as_dataset_status(status)
            if status == DatasetStatusEnum.complete.value:
                dataset.completed_at = _now()

        if job_id:
            job = session.get(ProcessingJob, uuid.UUID(job_id))
            if job:
                job.status = _as_dataset_status(status)
                if progress_pct is not None:
                    job.progress_pct = progress_pct
                if step is not None:
                    job.current_step = step
                if error:
                    job.error_message = error
                if tb:
                    job.traceback = tb
                if status == DatasetStatusEnum.complete.value:
                    job.completed_at = _now()

        session.commit()


def _record_progress(dataset_id: str, job_id: str, status: str, pct: float, step: str, message: str) -> None:
    update_job_progress(job_id, pct, step, message)
    update_db_status(
        dataset_id=dataset_id,
        status=status,
        job_id=job_id,
        progress_pct=pct,
        step=step,
    )


@celery_app.task(bind=True, max_retries=2, soft_time_limit=1800, time_limit=2000)
def process_dataset(self: Task, dataset_id: str, options: dict | None = None) -> dict[str, Any]:
    job_id = str(self.request.id)
    options = options or {}

    temp_raw_path: str | None = None
    clean_path: str | None = None
    html_report_path: str | None = None
    original_filename = "dataset"

    try:
        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.preprocessing.value,
            5,
            "downloading",
            "Fetching dataset from storage...",
        )

        with SyncSessionLocal() as session:
            dataset = session.get(Dataset, uuid.UUID(dataset_id))
            if not dataset:
                raise ValueError(f"Dataset {dataset_id} not found in database")
            storage_key = dataset.storage_path
            original_filename = dataset.original_filename
            domain = dataset.domain.value if hasattr(dataset.domain, "value") else str(dataset.domain)
            file_type = _file_type_from_dataset(dataset)

        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{original_filename}") as temp_raw_file:
            temp_raw_path = temp_raw_file.name

        if storage_key and not os.path.isabs(storage_key):
            minio_client.fget_object(
                settings.normalized_minio_bucket,
                storage_key,
                temp_raw_path,
            )
        elif storage_key and os.path.exists(storage_key):
            with open(storage_key, "rb") as src, open(temp_raw_path, "wb") as dst:
                dst.write(src.read())
        else:
            raise ValueError(f"Dataset storage path is invalid: {storage_key}")

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.preprocessing.value,
            15,
            "loading",
            "Loading dataset into memory...",
        )

        loader = DatasetLoader()
        load_result = loader.load(temp_raw_path, file_type, options)
        df = load_result.df

        with SyncSessionLocal() as session:
            dataset = session.get(Dataset, uuid.UUID(dataset_id))
            if dataset:
                dataset.row_count = int(load_result.row_count)
                dataset.col_count = int(load_result.col_count)
            session.commit()

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.preprocessing.value,
            25,
            "preprocessing",
            "Cleaning and preprocessing data...",
        )

        context = PipelineContext(
            dataset_id=dataset_id,
            domain=domain,
            job_id=job_id,
            options=options,
        )
        pipeline = PreprocessingOrchestrator()
        df_clean, all_logs = pipeline.run_pipeline(df, context)

        with SyncSessionLocal() as session:
            for log in all_logs:
                session.add(
                    ProcessingLog(
                        job_id=uuid.UUID(job_id),
                        step_name=log.get("step_name", "pipeline"),
                        action=log.get("action", "info"),
                        column_name=log.get("column_name"),
                        before_value=log.get("before_value"),
                        after_value=log.get("after_value"),
                        reason=log.get("reason"),
                        severity=_as_severity(log.get("severity", "info")),
                    )
                )
            session.commit()

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.preprocessing.value,
            55,
            "preprocessing_done",
            f"Preprocessing complete. {len(all_logs)} operations performed.",
        )

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.preprocessing.value,
            60,
            "saving_clean",
            "Saving cleaned dataset...",
        )

        if isinstance(df_clean, dd.DataFrame):
            df_clean_pd = df_clean.compute()
        elif isinstance(df_clean, pd.DataFrame):
            df_clean_pd = df_clean
        else:
            raise ValueError("Preprocessing pipeline did not return a DataFrame")

        with tempfile.NamedTemporaryFile(delete=False, suffix="_clean.parquet") as temp_clean_file:
            clean_path = temp_clean_file.name
        df_clean_pd.to_parquet(clean_path, index=False)

        clean_storage_key = f"clean/{dataset_id}/{dataset_id}_clean.parquet"
        minio_client.fput_object(
            settings.normalized_minio_bucket,
            clean_storage_key,
            clean_path,
            content_type="application/octet-stream",
        )

        with SyncSessionLocal() as session:
            dataset = session.get(Dataset, uuid.UUID(dataset_id))
            if dataset:
                dataset.cleaned_storage_path = clean_storage_key
                dataset.status = DatasetStatusEnum.eda_running
            job = session.get(ProcessingJob, uuid.UUID(job_id))
            if job:
                job.status = DatasetStatusEnum.eda_running
                job.progress_pct = 60
                job.current_step = "saving_clean"
            session.commit()

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.eda_running.value,
            65,
            "eda",
            "Generating EDA report...",
        )

        eda_summary = generate_json_summary(df_clean_pd, domain, context.schema)
        plots = generate_visualizations(df_clean_pd, eda_summary)
        dataset_name = PathSafeName(dataset_id, original_filename).dataset_name
        html_report = build_html_report(
            json_summary=eda_summary,
            processing_logs=all_logs,
            plots=plots,
            dataset_name=dataset_name,
        )
        compressed_json = compress_for_agents(eda_summary)
        compressed_json["dataset_name"] = dataset_name

        with tempfile.NamedTemporaryFile(delete=False, suffix="_eda.html", mode="w", encoding="utf-8") as temp_html_file:
            html_report_path = temp_html_file.name
            temp_html_file.write(html_report)

        html_storage_key = f"reports/{dataset_id}/eda_report.html"
        minio_client.fput_object(
            settings.normalized_minio_bucket,
            html_storage_key,
            html_report_path,
            content_type="text/html",
        )

        with SyncSessionLocal() as session:
            session.add(
                EDAReport(
                    dataset_id=uuid.UUID(dataset_id),
                    html_report_path=html_storage_key,
                    json_summary=eda_summary,
                    profile_stats={
                        "shape": eda_summary.get("shape"),
                        "memory_mb": eda_summary.get("memory_mb"),
                        "quality_score": eda_summary.get("dataset_quality_score"),
                    },
                )
            )
            dataset = session.get(Dataset, uuid.UUID(dataset_id))
            if dataset:
                dataset.status = DatasetStatusEnum.agents_running
                dataset.schema_json = eda_summary.get("columns")
            job = session.get(ProcessingJob, uuid.UUID(job_id))
            if job:
                job.status = DatasetStatusEnum.agents_running
                job.progress_pct = 80
                job.current_step = "agents"
            session.commit()

        _record_progress(
            dataset_id,
            job_id,
            DatasetStatusEnum.agents_running.value,
            80,
            "agents",
            "Running AI agents...",
        )

        orchestrator = AgentOrchestrator()
        agent_results = orchestrator.run_all_agents(
            eda_json=compressed_json,
            domain=domain,
            processing_logs=[log.get("reason") or log.get("action", "") for log in all_logs][:20],
        )

        with SyncSessionLocal() as session:
            for result in agent_results:
                session.add(
                    AgentReport(
                        dataset_id=uuid.UUID(dataset_id),
                        agent_name=result.agent_name,
                        agent_role=result.agent_role,
                        report_markdown=result.report_markdown,
                        structured_json=result.structured_json,
                        tokens_used=result.tokens_used,
                        model_used=result.model_used,
                        provider=result.provider,
                    )
                )

            dataset = session.get(Dataset, uuid.UUID(dataset_id))
            if dataset:
                dataset.status = DatasetStatusEnum.complete
                dataset.completed_at = _now()

            job = session.get(ProcessingJob, uuid.UUID(job_id))
            if job:
                job.status = DatasetStatusEnum.complete
                job.progress_pct = 100
                job.current_step = "complete"
                job.completed_at = _now()

            session.commit()

        update_job_progress(job_id, 100, "complete", "Analysis complete!")
        return {"status": "complete", "dataset_id": dataset_id}

    except MemoryError:
        update_job_progress(
            job_id,
            0,
            "retrying",
            "File too large for memory, switching to chunked mode...",
        )
        options["force_dask"] = True
        raise self.retry(countdown=5, args=[dataset_id, options])
    except Exception as exc:
        error_msg = str(exc)
        tb = traceback.format_exc()
        update_db_status(
            dataset_id=dataset_id,
            status=DatasetStatusEnum.failed.value,
            job_id=job_id,
            progress_pct=0,
            step="failed",
            error=error_msg,
            tb=tb,
        )
        update_job_progress(job_id, 0, "failed", f"Error: {error_msg}")
        raise
    finally:
        for temp_path in [temp_raw_path, clean_path, html_report_path]:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)


@celery_app.task(bind=True, queue="fast")
def run_notebook(self: Task, notebook_id: str, dataset_id: str) -> dict[str, Any]:
    job_id = str(self.request.id)
    notebook_uuid = uuid.UUID(notebook_id)
    dataset_uuid = uuid.UUID(dataset_id)
    temp_path: str | None = None

    try:
        update_job_progress(job_id, 5, "loading_notebook", "Loading notebook and dataset metadata...")
        with SyncSessionLocal() as session:
            notebook = session.get(Notebook, notebook_uuid)
            if not notebook:
                raise ValueError(f"Notebook {notebook_id} not found")

            dataset = session.get(Dataset, dataset_uuid)
            if not dataset:
                raise ValueError(f"Dataset {dataset_id} not found")

            original_filename = dataset.original_filename or "dataset.parquet"

        update_job_progress(job_id, 20, "downloading_dataset", "Downloading dataset from object storage...")
        suffix = ".parquet" if (original_filename or "").lower().endswith(".parquet") else "_notebook_data"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            temp_path = tmp.name

        with SyncSessionLocal() as session:
            dataset = session.get(Dataset, dataset_uuid)
            if not dataset:
                raise ValueError(f"Dataset {dataset_id} not found")
            df = _load_dataframe_for_notebook(dataset, temp_path)

        update_job_progress(job_id, 60, "running_notebook", "Executing notebook cells...")
        with SyncSessionLocal() as session:
            notebook = session.get(Notebook, notebook_uuid)
            if not notebook:
                raise ValueError(f"Notebook {notebook_id} not found")

            runner = NotebookRunner()
            results = runner.run_all(notebook, df)
            notebook.results = results
            notebook.cells = runner.annotate_cells_with_results(notebook.cells or [], results)
            notebook.run_count = int(notebook.run_count or 0) + 1
            notebook.last_run_at = _now()
            notebook.updated_at = _now()
            session.commit()

        update_job_progress(job_id, 100, "complete", "Notebook execution complete.")
        return {"notebook_id": notebook_id, "cells_run": len(results)}
    except Exception as exc:
        update_job_progress(job_id, 0, "failed", f"Notebook run failed: {exc}")
        raise
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


class PathSafeName:
    def __init__(self, dataset_id: str, filename: str):
        self.dataset_id = dataset_id
        self.filename = filename

    @property
    def dataset_name(self) -> str:
        base = os.path.splitext(self.filename)[0].strip()
        if base:
            return base
        return f"dataset_{self.dataset_id}"
