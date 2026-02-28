import pandas as pd
import dask.dataframe as dd
from typing import Any
from .base import PipelineStep, PipelineContext, StepResult

class TypeFixer(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Fix numeric/datetime coercions, normalize booleans, strip whitespace.
        For Dask, we use map_partitions where applicable or native dd methods.
        """
        logs = []
        warnings = []
        modified = []
        is_dask = isinstance(df, dd.DataFrame)
        
        cols_to_process = df.columns.tolist()
        
        for col in cols_to_process:
            dtype = df[col].dtype
            
            if pd.api.types.is_object_dtype(dtype):
                # 1. Strip Whitespace
                if is_dask:
                    df[col] = df[col].astype(str).str.strip()
                else:
                    df[col] = df[col].apply(lambda x: str(x).strip() if isinstance(x, str) else x)

                # Fetch a sample for testing coercions
                # to avoid full compute on every text column
                sample = df[col].head(min(1000, len(df)), compute=True) if is_dask else df[col].dropna().head(1000)
                if len(sample) == 0:
                    continue
                    
                # 2. String -> Numeric Coercion check
                num_test = pd.to_numeric(sample, errors='coerce')
                valid_num_pct = num_test.notna().mean()
                if valid_num_pct > 0.95 and valid_num_pct < 1.0: # 1.0 would mean it was naturally convertible
                    # Apply coercion
                    if is_dask:
                        df[col] = dd.to_numeric(df[col], errors='coerce')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    modified.append(col)
                    logs.append({"job_id": context.job_id, "step_name": "TypeFixer", "action": "coerce_numeric", "column_name": col, "severity": "info"})
                    continue

                # 3. Boolean Coercion
                bool_map = {"yes": True, "no": False, "true": True, "false": False, "1": True, "0": False, "1.0": True, "0.0": False}
                unique_vals = set(sample.astype(str).str.lower().unique())
                if unique_vals.issubset(set(bool_map.keys()) | {'nan', 'null', 'none', ''}):
                    if is_dask:
                        df[col] = df[col].astype(str).str.lower().map(bool_map)
                    else:
                        df[col] = df[col].astype(str).str.lower().map(bool_map)
                    modified.append(col)
                    logs.append({"job_id": context.job_id, "step_name": "TypeFixer", "action": "coerce_boolean", "column_name": col, "severity": "info"})
                    continue
                
                # 4. Datetime Coercion (Tried common formats)
                if context.schema.get(col) == "datetime_col":
                    try:
                        if is_dask:
                            df[col] = dd.to_datetime(df[col], errors='coerce')
                        else:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        modified.append(col)
                    except Exception as e:
                        warnings.append(f"Could not convert {col} to datetime despite role: {e}")
                
                # 5. Mixed Case Flag
                # We do this fast check natively. Since it's Dask/Pandas, we just warn if lowercased unique count != normal unique count
                # But that requires compute, so we skip it to save time for large files.
                
        return StepResult(df, logs, warnings, modified)
