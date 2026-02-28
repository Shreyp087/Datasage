from __future__ import annotations

from typing import Any

PIPELINE_ROLE_ALIASES: dict[str, str] = {
    "id_col": "id_col",
    "id": "id_col",
    "text": "text_col",
    "text_col": "text_col",
    "datetime": "datetime_col",
    "datetime_col": "datetime_col",
    "feature": "feature_col",
    "feature_col": "feature_col",
    "categorical": "feature_col",
    "target": "target_col",
    "target_col": "target_col",
    "constant": "constant_col",
    "constant_col": "constant_col",
}

DOMAIN_PROFILES: dict[str, dict[str, Any]] = {
    "general": {
        "display_name": "General",
        "icon": "📦",
        "color": "#6b7280",
        "description": "General-purpose tabular datasets",
        "known_columns": {},
    },
    "healthcare": {
        "display_name": "Healthcare",
        "icon": "🏥",
        "color": "#ef4444",
        "description": "Clinical, claims, and patient datasets",
        "known_columns": {},
    },
    "finance": {
        "display_name": "Finance",
        "icon": "💹",
        "color": "#22c55e",
        "description": "Financial, transaction, and risk datasets",
        "known_columns": {},
    },
    "education": {
        "display_name": "Education",
        "icon": "🎓",
        "color": "#3b82f6",
        "description": "Education and student datasets",
        "known_columns": {},
    },
    "ecommerce": {
        "display_name": "E-Commerce",
        "icon": "🛒",
        "color": "#f97316",
        "description": "Retail and commerce datasets",
        "known_columns": {},
    },
    "ai_incidents": {
        "display_name": "AI Incident Database",
        "icon": "⚠️",
        "color": "#f59e0b",
        "description": "Documented AI failures and harms - AIID format",
        "known_columns": {
            "incident_id": {"role": "id_col"},
            "title": {"role": "text"},
            "description": {"role": "text"},
            "date": {"role": "datetime"},
            "year": {"role": "feature"},
            "allegeddeployerofaisystem": {"role": "categorical"},
            "allegeddeveloperofaisystem": {"role": "categorical"},
            "allegedharmedornearlyharmedparties": {"role": "categorical"},
            # Compatibility alias for known typo in external docs/config.
            "allegedharmedornearlyharmeddparties": {"role": "categorical"},
            "harm_type": {"role": "categorical"},
            "sector_of_deployment": {"role": "categorical"},
            "technology_purveyor": {"role": "categorical"},
            "ai_system": {"role": "categorical"},
        },
    },
    "other": {
        "display_name": "Other",
        "icon": "🧭",
        "color": "#64748b",
        "description": "Other domain datasets",
        "known_columns": {},
    },
}


def get_domain_profile(domain: str) -> dict[str, Any]:
    return DOMAIN_PROFILES.get((domain or "").strip().lower(), DOMAIN_PROFILES["general"])


def to_pipeline_role(role: str) -> str:
    normalized = (role or "").strip().lower()
    return PIPELINE_ROLE_ALIASES.get(normalized, "feature_col")
