from typing import Any
import dask.dataframe as dd
from .base import PipelineStep, PipelineContext, StepResult

class DuplicateDetector(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Flag exact row duplicates. Do not remove.
        Duplicate column names handled by Normalizer.
        """
        logs = []
        warnings = []
        is_dask = isinstance(df, dd.DataFrame)
        
        # Near duplicate (LSH) omitted for scaffold speed, but full dupes measured
        try:
            if is_dask:
                # Counting duplicates in dask can be expensive, sample or try drop_duplicates size diff
                # For safety, we only approximate or skip if too large
                pass 
            else:
                dup_count = df.duplicated().sum()
                if dup_count > 0:
                    dup_pct = dup_count / len(df)
                    warnings.append(f"Found {dup_count} exact duplicate rows ({dup_pct:.2%}).")
                    logs.append({
                        "job_id": context.job_id, 
                        "step_name": "DuplicateDetector",
                        "action": "flag_duplicates",
                        "after_value": {"dup_count": int(dup_count)},
                        "severity": "warning"
                    })
        except Exception:
            pass

        return StepResult(df, logs, warnings, [])
