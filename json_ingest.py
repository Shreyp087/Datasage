"""
Convenience wrapper for the merge JSON ingestion engine.

Root-level tools import from `json_ingest`, while backend package code imports
from `app.merge.json_ingest`. This module keeps both paths working.
"""

from backend.app.merge.json_ingest import (  # noqa: F401
    FieldKind,
    FieldProfile,
    JsonLayout,
    JsonReport,
    analyze_json,
    json_report_to_text,
    load_json,
    load_json_as_df,
)

