import pandas as pd
import dask.dataframe as dd
import numpy as np
from scipy import stats
from typing import Any
from .base import PipelineStep, PipelineContext, StepResult

class OutlierDetector(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Flags outliers for numeric columns (IQR/Z-score). NEVER removes them.
        Domain rules injected via context.domain.
        """
        logs = []
        warnings = []
        is_dask = isinstance(df, dd.DataFrame)
        
        domain = context.domain
        
        if is_dask:
            # We sample Dask for outliers to strictly avoid global shuffle sorts
            sample_df = df.sample(frac=min(1.0, 50000/len(df))).compute() if len(df) > 0 else pd.DataFrame()
        else:
            sample_df = df
            
        for col in sample_df.columns:
            try:
                col_dtype = sample_df[col].dtype
                # Pandas treats bool as numeric, but IQR/Z-score math on bool can fail.
                if not pd.api.types.is_numeric_dtype(col_dtype) or pd.api.types.is_bool_dtype(col_dtype):
                    continue
                    
                # Skip ids/datetimes
                if context.schema.get(col) in ["id_col", "datetime_col"]:
                    continue
                    
                series = pd.to_numeric(sample_df[col], errors="coerce")
                series = series.replace([np.inf, -np.inf], np.nan).dropna()
                if len(series) < 10:
                    continue
                    
                n = len(series)
                
                # Domain specific flag checks
                if domain == "healthcare":
                    if "age" in col.lower() and (series > 120).any():
                        warnings.append(f"Healthcare anomaly: {col} contains Age > 120.")
                    if (series < 0).any() and any(k in col.lower() for k in ["pressure", "weight", "height"]):
                        warnings.append(f"Healthcare anomaly: {col} contains impossible negative values.")
                        
                elif domain == "finance":
                    if (series < 0).any() and any(k in col.lower() for k in ["price", "volume", "amount"]):
                        warnings.append(f"Finance warning: {col} contains negative prices/amounts.")
                        
                elif domain == "education":
                    if "grade" in col.lower() or "score" in col.lower():
                        if (series > 100).any() or (series < 0).any():
                            warnings.append(f"Education warning: {col} scores fall outside standard 0-100 range.")

                # Distribution check for method selection
                method_used = "IQR"
                
                try:
                    if n < 5000:
                        _, p_val = stats.shapiro(series)
                    else:
                        _, p_val = stats.normaltest(series)
                except Exception:
                    p_val = 0
                
                if p_val > 0.05:
                    # Approx normal -> Z-score
                    method_used = "Z-Score"
                    z = stats.zscore(series)
                    outliers = series[abs(z) > 3.5]
                else:
                    # Not normal -> IQR
                    q1 = series.quantile(0.25)
                    q3 = series.quantile(0.75)
                    iqr = q3 - q1
                    outliers = series[(series < q1 - 3 * iqr) | (series > q3 + 3 * iqr)]
                    
                outlier_count = len(outliers)
                if outlier_count > 0:
                    outlier_pct = outlier_count / n
                    logs.append({
                        "job_id": context.job_id,
                        "step_name": "OutlierDetector",
                        "action": "flag_outliers",
                        "column_name": col,
                        "after_value": {"count": outlier_count, "pct": round(outlier_pct, 4), "method": method_used},
                        "severity": "warning" if outlier_pct > 0.05 else "info"
                    })
            except Exception as exc:
                warnings.append(f"OutlierDetector skipped column '{col}' due to error: {str(exc)}")
                logs.append({
                    "job_id": context.job_id,
                    "step_name": "OutlierDetector",
                    "action": "skip_column",
                    "column_name": col,
                    "after_value": {"reason": str(exc)},
                    "severity": "warning"
                })
                continue

        return StepResult(df, logs, warnings, [])
