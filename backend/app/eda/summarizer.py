import pandas as pd
import dask.dataframe as dd
import numpy as np
from typing import Dict, Any

def _to_float_or_none(value: Any) -> float | None:
    return float(value) if pd.notna(value) else None

def generate_json_summary(df: Any, domain: str, schema: dict) -> Dict[str, Any]:
    """
    Produce the exact JSON structure for AI agents, compatible with Pandas/Dask.
    """
    is_dask = isinstance(df, dd.DataFrame)
    
    if is_dask:
        sample_df = df.sample(frac=0.1, random_state=42).compute()
        total_rows = len(df)
        null_counts = df.isnull().sum().compute()
    else:
        sample_df = df
        total_rows = len(df)
        null_counts = df.isnull().sum()
        
    num_cols = len(df.columns)
    
    # Memory footprint natively available for pandas, approx for dask
    if not is_dask:
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    else:
        memory_mb = sample_df.memory_usage(deep=True).sum() / (1024 * 1024) * 10 # Rough scale
        
    summary = {
        "shape": {"rows": int(total_rows), "cols": num_cols},
        "memory_mb": float(memory_mb),
        "domain": domain,
        "columns": [],
        "high_correlations": [],
        "time_series_detected": False,
        "datetime_columns": [],
        "text_columns": [],
        "potential_target_columns": [],
        "class_balance": {},
        "dataset_quality_score": 100.0,
        "warnings": []
    }
    
    quality_penalty = 0.0
    
    numeric_cols = []
    
    for col in df.columns:
        dt = sample_df[col].dtype
        role = schema.get(col, "feature_col")
        null_count = int(null_counts[col])
        null_pct = float(null_count / total_rows) if total_rows > 0 else 0
        
        uniq_count = int(sample_df[col].nunique())
        uniq_pct = float(uniq_count / len(sample_df)) if len(sample_df) > 0 else 0
        
        if role == "datetime_col":
            summary["datetime_columns"].append(col)
        elif role == "text_col":
            summary["text_columns"].append(col)
            
        top_5_vals = []
        if pd.api.types.is_bool_dtype(dt):
            vc = sample_df[col].value_counts(dropna=False).head(5)
            top_5_vals = [{"value": str(k), "count": int(v)} for k, v in vc.items()]

            if 2 <= uniq_count <= 20:
                dist = {str(k): int(v) for k, v in sample_df[col].value_counts(dropna=False).items()}
                summary["class_balance"][col] = {"distribution": dist}

            dist_type = "categorical"
            stats = {"min": None, "max": None, "mean": None, "median": None, "std": None, "skewness": None, "kurtosis": None}
            outlier_pct = 0.0
            outlier_count = 0

        elif pd.api.types.is_object_dtype(dt) or pd.api.types.is_categorical_dtype(dt):
            vc = sample_df[col].value_counts().head(5)
            top_5_vals = [{"value": str(k), "count": int(v)} for k, v in vc.items()]
            
            # Class Balance Check
            if 2 <= uniq_count <= 20:
                dist = {str(k): int(v) for k, v in sample_df[col].value_counts().items()}
                summary["class_balance"][col] = {"distribution": dist}
                
            dist_type = "categorical" if role != "text_col" else "text"
            stats = {"min": None, "max": None, "mean": None, "median": None, "std": None, "skewness": None, "kurtosis": None}
            outlier_pct = 0.0
            outlier_count = 0
            
        elif pd.api.types.is_numeric_dtype(dt) and not pd.api.types.is_bool_dtype(dt):
            numeric_cols.append(col)
            vc = sample_df[col].value_counts().head(5)
            top_5_vals = [{"value": str(k), "count": int(v)} for k, v in vc.items()]
            
            series = pd.to_numeric(sample_df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
            
            if len(series) > 0:
                s_min, s_max = _to_float_or_none(series.min()), _to_float_or_none(series.max())
                s_mean = _to_float_or_none(series.mean())
                s_median = _to_float_or_none(series.median())
                s_std = _to_float_or_none(series.std())
                s_skew = _to_float_or_none(series.skew())
                s_kurt = _to_float_or_none(series.kurtosis())
                
                if s_skew is not None and s_skew > 1: dist_type = "right_skewed"
                elif s_skew is not None and s_skew < -1: dist_type = "left_skewed"
                elif uniq_count < 10: dist_type = "categorical"
                else: dist_type = "normal"
            else:
                s_min, s_max, s_mean, s_median, s_std, s_skew, s_kurt = [None]*7
                dist_type = "unknown"
                
            # Outlier approx (IQR)
            if len(series) > 10:
                q1, q3 = series.quantile(0.25), series.quantile(0.75)
                iqr = q3 - q1
                outlier_count = int(((series < q1 - 3*iqr) | (series > q3 + 3*iqr)).sum())
                outlier_pct = float(outlier_count / len(series))
                quality_penalty += min(10, outlier_pct * 100) # minus points for excessive outliers
            else:
                outlier_count, outlier_pct = 0, 0.0
                
            stats = {"min": s_min, "max": s_max, "mean": s_mean, "median": s_median, "std": s_std, "skewness": s_skew, "kurtosis": s_kurt}

        else:
            stats = {"min": None, "max": None, "mean": None, "median": None, "std": None, "skewness": None, "kurtosis": None}
            dist_type = "unknown"
            outlier_count, outlier_pct = 0, 0.0
        
        # Determine Potential Target
        if 2 <= uniq_count <= 20 and role != "id_col":
            summary["potential_target_columns"].append(col)
            
        col_summary = {
            "name": col,
            "dtype": str(dt),
            "role": role,
            "null_pct": round(null_pct, 4),
            "unique_count": uniq_count,
            "unique_pct": uniq_pct,
            "top_5_values": top_5_vals,
            "distribution_type": dist_type,
            "outlier_count": outlier_count,
            "outlier_pct": round(outlier_pct, 4)
        }
        col_summary.update(stats)
        summary["columns"].append(col_summary)
        
        quality_penalty += null_pct * 50 # massive penalty for heavily missing columns

    # Correlation Matrix Check
    if len(numeric_cols) > 1:
        corr_matrix = sample_df[numeric_cols].corr()
        # Extract pairs > 0.85
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                c1, c2 = numeric_cols[i], numeric_cols[j]
                val = corr_matrix.loc[c1, c2]
                if pd.notna(val) and abs(val) > 0.85:
                    summary["high_correlations"].append({
                        "col1": c1,
                        "col2": c2,
                        "correlation": float(val),
                        "concern": True
                    })
                    
    # Time Series Check
    if summary["datetime_columns"]:
        summary["time_series_detected"] = True

    # Quality Score Finalize
    score = max(0.0, 100.0 - quality_penalty)
    summary["dataset_quality_score"] = round(score, 1)

    return summary
