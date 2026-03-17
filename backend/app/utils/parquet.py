from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def prepare_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize object-typed values into parquet-safe scalars.
    This prevents pyarrow conversion failures for mixed object columns,
    such as datetime objects embedded in otherwise string/object columns.
    """
    normalized = df.copy()
    object_columns = normalized.select_dtypes(include=["object"]).columns

    for column in object_columns:
        normalized[column] = normalized[column].map(_normalize_object_value)

    return normalized


def _normalize_object_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    if isinstance(value, np.datetime64):
        try:
            return pd.Timestamp(value).isoformat()
        except Exception:
            return str(value)

    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")

    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, default=str, ensure_ascii=False)
        except Exception:
            return str(value)

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return value
