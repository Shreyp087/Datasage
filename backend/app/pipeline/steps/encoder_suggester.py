from typing import Any
import pandas as pd
import dask.dataframe as dd
from .base import PipelineStep, PipelineContext, StepResult

class EncoderSuggester(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Suggest encoders based on cardinality.
        Suggestions only – no DF modifications.
        """
        logs = []
        is_dask = isinstance(df, dd.DataFrame)
        
        if is_dask:
            sample_df = df.head(min(10000, len(df)), compute=True)
        else:
            sample_df = df

        for col in sample_df.columns:
            dtype = sample_df[col].dtype
            role = context.schema.get(col)
            
            if role in ["id_col", "constant_col", "target_col"]:
                continue
                
            suggestion = None
            
            if role == "datetime_col":
                suggestion = "feature_extraction (year, month, day, weekday, is_weekend)"
            elif role == "text_col":
                suggestion = "TF-IDF or Embeddings"
            elif pd.api.types.is_object_dtype(dtype) or pd.api.types.is_categorical_dtype(dtype):
                nunique = sample_df[col].nunique()
                if nunique <= 10:
                    suggestion = "One-Hot Encoding"
                elif 10 < nunique <= 50:
                    suggestion = "Label/Ordinal Encoding"
                else:
                    suggestion = "Target/Frequency Encoding"
                    
            if suggestion:
                logs.append({
                    "job_id": context.job_id,
                    "step_name": "EncoderSuggester",
                    "action": "suggest_encoder",
                    "column_name": col,
                    "after_value": {"suggestion": suggestion},
                    "severity": "info"
                })
                
        return StepResult(df, logs, [], [])
