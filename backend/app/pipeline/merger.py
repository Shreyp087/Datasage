import pandas as pd
import dask.dataframe as dd
import difflib
from typing import List, Dict, Any, Tuple
from pydantic import BaseModel

class MergeSuggestion(BaseModel):
    left_col: str
    right_col: str
    confidence_score: float
    overlap_pct: float
    join_type_suggestion: str
    explanation: str

def compute_overlap(series1: pd.Series, series2: pd.Series) -> float:
    s1_set = set(series1.dropna().astype(str))
    s2_set = set(series2.dropna().astype(str))
    if not s1_set or not s2_set:
        return 0.0
    intersection = len(s1_set.intersection(s2_set))
    return intersection / min(len(s1_set), len(s2_set))

def suggest_merge_keys(df1: Any, df2: Any) -> List[MergeSuggestion]:
    """
    Auto-detect potential merge keys using naming, dtype, and overlap heuristics.
    """
    suggestions = []
    
    # Extract samples
    if isinstance(df1, dd.DataFrame):
        s1 = df1.head(1000, compute=True)
    else:
        s1 = df1.sample(min(1000, len(df1))) if len(df1) > 1000 else df1

    if isinstance(df2, dd.DataFrame):
        s2 = df2.head(1000, compute=True)
    else:
        s2 = df2.sample(min(1000, len(df2))) if len(df2) > 1000 else df2

    cols1 = s1.columns.tolist()
    cols2 = s2.columns.tolist()

    for c1 in cols1:
        for c2 in cols2:
            score = 0.0
            reasons = []
            
            # 1. Name Match
            if c1.lower() == c2.lower():
                score += 0.8
                reasons.append("Exact column name match")
            else:
                seq = difflib.SequenceMatcher(None, c1.lower(), c2.lower())
                if seq.ratio() > 0.8: # roughly lev < 3 equivalent for short names
                    score += 0.6
                    reasons.append("Similar column name")
                    
            if score == 0:
                continue # Prune search space
                
            # 2. Dtype Match
            if s1[c1].dtype == s2[c2].dtype:
                score += 0.1
                reasons.append("Same data type")
                
            # 3. Overlap Analysis
            overlap = compute_overlap(s1[c1], s2[c2])
            if overlap < 0.20:
                continue # Discard low overlap pairs
                
            if overlap > 0.50:
                score += 0.3
                reasons.append(f"Strong value overlap ({overlap:.1%})")
            else:
                score += 0.1
                reasons.append(f"Moderate value overlap ({overlap:.1%})")
                
            # 4. Cardinality Check
            u1 = s1[c1].nunique() / len(s1)
            u2 = s2[c2].nunique() / len(s2)
            
            # Suggest join type based on uniqueness
            if u1 > 0.9 and u2 > 0.9:
                join_type = "inner"
                reasons.append("Both columns look like Primary Keys (1-to-1)")
            elif u1 > 0.9 and u2 <= 0.9:
                join_type = "right"
                reasons.append("Left table key is PK, Right is FK (1-to-many)")
            elif u2 > 0.9 and u1 <= 0.9:
                join_type = "left"
                reasons.append("Left table key is FK, Right is PK (many-to-1)")
            else:
                join_type = "inner"
                reasons.append("Both columns have low cardinality (many-to-many, potential fan-out warning)")

            suggestions.append(MergeSuggestion(
                left_col=c1,
                right_col=c2,
                confidence_score=round(min(1.0, score), 2),
                overlap_pct=round(overlap, 2),
                join_type_suggestion=join_type,
                explanation="; ".join(reasons)
            ))

    # Return top 3
    suggestions.sort(key=lambda x: x.confidence_score, reverse=True)
    return suggestions[:3]

def estimate_merged_size(df1: Any, df2: Any, left_key: str, right_key: str, join_type: str) -> Dict[str, Any]:
    """
    Heuristical bounding of resulting rows/memory.
    """
    # Extremely simplified estimation for scaffolding
    len_1 = len(df1)
    len_2 = len(df2)
    
    # Assume 1-to-1 or roughly 1-to-many for basic sizing, fan out is hard to perfect without count-min sketches
    if join_type in ["inner", "left"]:
        est_rows = len_1 * 1.5 
    elif join_type == "right":
        est_rows = len_2 * 1.5
    else:
        est_rows = len_1 + len_2
        
    est_cols = len(df1.columns) + len(df2.columns) - 1
    est_mem_mb = (est_rows * est_cols * 8) / (1024 * 1024) # naive 8 byte per cell

    warning = "Estimated output > 10M rows or extreme fan-out. Proceed with caution." if est_rows > 10000000 else None
    
    return {
        "estimated_rows": int(est_rows),
        "estimated_memory_mb": float(est_mem_mb),
        "warning": warning
    }

def execute_merge(df1: Any, df2: Any, left_key: str, right_key: str, join_type: str) -> Dict[str, Any]:
    """
    Executes actual merge, Dask for >1GB handling.
    """
    is_dask = isinstance(df1, dd.DataFrame) or isinstance(df2, dd.DataFrame)
    
    if is_dask:
        # Ensure both are Dask
        if not isinstance(df1, dd.DataFrame): df1 = dd.from_pandas(df1, npartitions=4)
        if not isinstance(df2, dd.DataFrame): df2 = dd.from_pandas(df2, npartitions=4)
        
        merged = dd.merge(df1, df2, left_on=left_key, right_on=right_key, how=join_type, suffixes=('_left', '_right'), shuffle='tasks')
        n_rows = len(merged) # Note: requires compute during saving usually
    else:
        merged = pd.merge(df1, df2, left_on=left_key, right_on=right_key, how=join_type, suffixes=('_left', '_right'))
        n_rows = len(merged)
        
    fan_out_warning = None
    if n_rows > (len(df1) + len(df2)) * 5:
        fan_out_warning = "Significant fan-out occurred (many-to-many join match explosion)."

    added_nulls = merged.isnull().sum().sum() if not is_dask else merged.isnull().sum().sum().compute()
    
    return {
        "df": merged,
        "rows": n_rows,
        "fan_out_warning": fan_out_warning,
        "nulls_introduced": added_nulls
    }
