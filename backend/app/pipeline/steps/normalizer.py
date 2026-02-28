import re
from typing import Any
from .base import PipelineStep, PipelineContext, StepResult

class ColumnNormalizer(PipelineStep):
    def run(self, df: Any, context: PipelineContext) -> StepResult:
        """
        Standardize column names: lowercase, strip, replace spaces/special chars with underscore.
        Prefix digits with col_. Handle duplicate column names.
        """
        logs = []
        old_cols = df.columns.tolist()
        new_cols = []
        modified = []
        
        seen = set()
        
        for col in old_cols:
            original = col
            # lowercase, strip
            n_col = str(col).lower().strip()
            # replace special chars with underscore
            n_col = re.sub(r'[^a-z0-9]', '_', n_col)
            # remove duplicate underscores
            n_col = re.sub(r'_+', '_', n_col)
            # trim underscores
            n_col = n_col.strip('_')
            
            # prefix digits
            if n_col and n_col[0].isdigit():
                n_col = f"col_{n_col}"
                
            if not n_col:
                n_col = "unnamed"
                
            # deduplicate
            if n_col in seen:
                counter = 1
                while f"{n_col}_{counter}" in seen:
                    counter += 1
                n_col = f"{n_col}_{counter}"
            
            seen.add(n_col)
            new_cols.append(n_col)
            
            if original != n_col:
                modified.append(n_col)
                logs.append({
                    "job_id": context.job_id,
                    "step_name": "ColumnNormalizer",
                    "action": "rename_column",
                    "column_name": original,
                    "after_value": {"new_name": n_col},
                    "severity": "info"
                })
                
        # Apply renames
        rename_map = dict(zip(old_cols, new_cols))
        df = df.rename(columns=rename_map)
        
        # update schema if it existed
        if context.schema:
            new_schema = {}
            for old_c, role in context.schema.items():
                new_schema[rename_map.get(old_c, old_c)] = role
            context.schema = new_schema
            
        return StepResult(df, logs, [], modified)
