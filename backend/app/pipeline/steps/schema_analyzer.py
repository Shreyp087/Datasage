import json
from typing import Any

import dask.dataframe as dd
import pandas as pd

from .base import PipelineStep, PipelineContext, StepResult
from app.core.domain_profiles import get_domain_profile, to_pipeline_role


def _stable_value_token(value: Any) -> str:
    """Return a deterministic string token for unhashable Python objects."""
    if isinstance(value, dict):
        try:
            return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
        except Exception:
            return repr(value)
    if isinstance(value, set):
        try:
            return json.dumps(sorted(value, key=lambda item: str(item)), default=str, ensure_ascii=False)
        except Exception:
            return repr(value)
    if isinstance(value, (list, tuple)):
        try:
            return json.dumps(value, default=str, ensure_ascii=False)
        except Exception:
            return repr(value)
    return str(value)


def _safe_nunique(series: pd.Series) -> int:
    """
    Count unique non-null values even when cells contain unhashable objects
    (for example dict/list from JSON-like datasets).
    """
    try:
        return int(series.nunique(dropna=True))
    except TypeError:
        non_null = series.dropna()
        if non_null.empty:
            return 0
        return int(non_null.map(_stable_value_token).nunique(dropna=True))

class SchemaAnalyzer(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Detect column roles: id_col, datetime_col, target_col, feature_col, text_col, constant_col
        Roles are stored in context.schema
        """
        logs = []
        is_dask = isinstance(df, dd.DataFrame)
        domain_profile = get_domain_profile(context.domain)
        known_columns = domain_profile.get("known_columns", {})
        
        # Calculate heuristics (we use sample for dask to avoid full compute for text length etc.)
        if is_dask:
            sample_df = df.head(min(10000, len(df)), compute=True)
            total_rows = len(df) # Assume len is cheap or we use known value
        else:
            sample_df = df
            total_rows = len(df)

        sample_rows = len(sample_df)
            
        for col in df.columns:
            profile_meta = known_columns.get(col)
            if isinstance(profile_meta, dict) and profile_meta.get("role"):
                role = to_pipeline_role(str(profile_meta.get("role")))
                context.schema[col] = role
                logs.append({
                    "job_id": context.job_id,
                    "step_name": "SchemaAnalyzer",
                    "action": "role_assignment",
                    "column_name": col,
                    "after_value": {"role": role, "source": "domain_profile"},
                    "severity": "info"
                })
                continue

            role = "feature_col"
            dtype = sample_df[col].dtype
            
            # Constant detection
            nunique = _safe_nunique(sample_df[col])
            if sample_rows > 0 and nunique == 1:
                role = "constant_col"
                context.warnings.append(f"Column '{col}' is constant (1 unique value) and flagged for potential drop.")
            
            # ID Detection
            elif sample_rows > 0 and (nunique / sample_rows) > 0.95:
                if "id" in col.lower() or "key" in col.lower() or "uuid" in col.lower() or pd.api.types.is_integer_dtype(dtype):
                    role = "id_col"
            
            # Text Detection
            elif pd.api.types.is_object_dtype(dtype):
                # Check for datetime parsing
                try:
                    parsed = pd.to_datetime(sample_df[col].dropna(), errors='coerce')
                    valid_pct = parsed.notna().mean()
                    if valid_pct > 0.80:
                        role = "datetime_col"
                    else:
                        # Check text length
                        avg_len = sample_df[col].dropna().astype(str).str.len().mean()
                        if avg_len > 50:
                            role = "text_col"
                except Exception:
                    pass
            
            # Real Datetime Dtype
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                role = "datetime_col"
                
            context.schema[col] = role
            logs.append({
                "job_id": context.job_id,
                "step_name": "SchemaAnalyzer",
                "action": "role_assignment",
                "column_name": col,
                "after_value": {"role": role},
                "severity": "info"
            })
            
        return StepResult(df, logs, [], [])
