import pandas as pd

from app.notebooks.runner import NotebookRunner
from app.notebooks.templates.dynamic_template import build_dynamic_notebook_template


def _sample_uploaded_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "incident_year": 2024,
                "event_date": "2024-01-10",
                "harm_category": "privacy",
                "deployment_sector": "healthcare",
                "org_name": "Org A",
                "vendor_name": "Vendor X",
                "incident_headline": "Model leaked patient records",
                "source_domain": "news.example",
                "severity_score": 7.2,
                "impact_score": 4.1,
            },
            {
                "incident_year": 2025,
                "event_date": "2025-04-03",
                "harm_category": "bias",
                "deployment_sector": "finance",
                "org_name": "Org B",
                "vendor_name": "Vendor X",
                "incident_headline": "Loan model flagged protected class unfairly",
                "source_domain": "policy.example",
                "severity_score": 8.1,
                "impact_score": 6.3,
            },
            {
                "incident_year": 2025,
                "event_date": "2025-08-19",
                "harm_category": "bias",
                "deployment_sector": "finance",
                "org_name": "Org B",
                "vendor_name": "Vendor Z",
                "incident_headline": "False positive spike in risk screening",
                "source_domain": "journal.example",
                "severity_score": 6.4,
                "impact_score": 5.2,
            },
        ]
    )


def test_dynamic_template_builds_and_executes_without_missing_column_errors():
    df = _sample_uploaded_df()
    template = build_dynamic_notebook_template(
        dataset_name="uploaded_incidents",
        domain="ai_incidents",
        df=df,
        snapshot_date="2026-03-09",
        snapshot_url="https://example.test/snapshot.csv",
    )

    assert template["title"].startswith("Auto Notebook")
    assert len(template["cells"]) >= 6
    titles = [str(cell.get("title") or "") for cell in template["cells"]]
    assert "EDA Template Snippets" in titles
    assert "Suggested Model Execution" in titles
    assert not any("AIID_Research_Notebook" in str(cell.get("content") or "") for cell in template["cells"])

    runner = NotebookRunner()
    notebook_like = type("NotebookLike", (), {"cells": template["cells"]})()
    results = runner.run_all(notebook_like, df)

    assert results
    assert all(entry.get("status") == "success" for entry in results.values())
