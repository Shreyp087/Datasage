import pandas as pd
import dask.dataframe as dd
from typing import Any
from .base import PipelineStep, PipelineContext, StepResult

class MissingValueHandler(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        null_pct == 0: skip
        < 0.05: auto-impute (num->median, cat->mode, dt->ffill)
        0.05-0.30: impute + binary indicator
        > 0.30: no auto-impute, flag warning
        """
        logs = []
        is_dask = isinstance(df, dd.DataFrame)
        modified = []
        
        # Calculate null counts efficiently
        if is_dask:
            null_counts = df.isnull().sum().compute()
            total_rows = len(df)
        else:
            null_counts = df.isnull().sum()
            total_rows = len(df)
            
        if total_rows == 0:
            return StepResult(df, logs, [], [])

        for col, count in null_counts.items():
            if count == 0:
                continue
                
            null_pct = count / total_rows
            
            if null_pct > 0.30:
                context.warnings.append(f"Column '{col}' has >30% missing values ({null_pct:.1%}). User decision required.")
                continue
                
            # Requires Imputation
            dtype_role = context.schema.get(col, "feature_col")
            fill_val = None
            method = "mode"
            
            # Determine fill value (we compute this via dask/pandas)
            if pd.api.types.is_numeric_dtype(df[col].dtype) and dtype_role != "id_col":
                method = "median"
                if is_dask:
                    # Approx quantile for median
                    fill_val = df[col].quantile(0.5).compute()
                else:
                    fill_val = df[col].median()
            elif dtype_role == "datetime_col" or pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                method = "ffill" # Handled via different fill logic
            else:
                method = "mode"
                if is_dask:
                    # Sample for mode
                    v_counts = df[col].value_counts().compute()
                    fill_val = v_counts.index[0] if len(v_counts) > 0 else "Missing"
                else:
                    modes = df[col].mode()
                    fill_val = modes.iloc[0] if not modes.empty else "Missing"

            # Apply Imputation
            if null_pct >= 0.05:
                # Add indicator column
                ind_col = f"{col}_was_missing"
                if is_dask:
                    df[ind_col] = df[col].isnull()
                else:
                    df[ind_col] = df[col].isnull().astype(int)
                modified.append(ind_col)
                logs.append({"job_id": context.job_id, "step_name": "MissingHandler", "action": "add_indicator", "column_name": col, "severity": "info"})
                
            if method == "ffill":
                if is_dask:
                    df[col] = df[col].ffill()
                else:
                    df[col] = df[col].ffill()
            else:
                if is_dask:
                    df[col] = df[col].fillna(fill_val)
                else:
                    df[col] = df[col].fillna(fill_val)
                    
            modified.append(col)
            logs.append({
                "job_id": context.job_id,
                "step_name": "MissingHandler",
                "action": "impute",
                "column_name": col,
                "after_value": {"method": method, "fill_val": str(fill_val)},
                "severity": "info"
            })
            
        return StepResult(df, logs, [], modified)
