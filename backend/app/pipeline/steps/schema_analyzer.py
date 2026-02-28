import pandas as pd
import dask.dataframe as dd
from typing import Any
from .base import PipelineStep, PipelineContext, StepResult
from app.core.domain_profiles import get_domain_profile, to_pipeline_role

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
            nunique = sample_df[col].nunique()
            if nunique == 1:
                role = "constant_col"
                context.warnings.append(f"Column '{col}' is constant (1 unique value) and flagged for potential drop.")
            
            # ID Detection
            elif (nunique / len(sample_df)) > 0.95:
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
