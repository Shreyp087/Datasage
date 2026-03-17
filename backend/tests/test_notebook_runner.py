import pandas as pd

from app.notebooks.runner import NotebookRunner


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "incident_id": "1",
                "year": 2024,
                "date": "2024-01-10",
                "harm_type": "Privacy",
                "sector_of_deployment": "Healthcare",
                "allegeddeployerofaisystem_primary": "Org A",
                "allegeddeveloperofaisystem_primary": "Vendor X",
                "title": "Incident one",
            },
            {
                "incident_id": "2",
                "year": 2025,
                "date": "2025-05-17",
                "harm_type": "Bias",
                "sector_of_deployment": "Finance",
                "allegeddeployerofaisystem_primary": "Org B",
                "allegeddeveloperofaisystem_primary": "Vendor X",
                "title": "Incident two",
            },
            {
                "incident_id": "3",
                "year": 2025,
                "date": "2025-11-02",
                "harm_type": "Bias",
                "sector_of_deployment": "Finance",
                "allegeddeployerofaisystem_primary": "Org B",
                "allegeddeveloperofaisystem_primary": "Vendor Z",
                "title": "Incident three",
            },
        ]
    )


def test_run_trend_cell():
    runner = NotebookRunner()
    df = _sample_df()
    cell = {
        "id": "cell_001",
        "type": "analysis",
        "analysis_type": "trend",
        "config": {"x_field": "year", "chart_type": "line"},
    }

    result = runner.run_cell(cell, df)
    assert result["type"] == "chart"
    assert result["chart_type"] == "line"
    assert result["x_field"] == "year"
    assert len(result["data"]) == 2


def test_run_summary_cell():
    runner = NotebookRunner()
    df = _sample_df()
    cell = {
        "id": "cell_summary",
        "type": "analysis",
        "analysis_type": "summary",
        "config": {},
    }

    result = runner.run_cell(cell, df)
    assert result["type"] == "stats"
    assert result["total_incidents"] == 3
    assert result["top_harm_type"] == "Bias"


def test_run_detailed_summary_cell():
    runner = NotebookRunner()
    df = _sample_df()
    cell = {
        "id": "cell_detailed_summary",
        "type": "analysis",
        "analysis_type": "detailed_summary",
        "config": {"top_n": 3},
    }

    result = runner.run_cell(cell, df)
    assert result["type"] == "narrative"
    assert isinstance(result.get("summary_markdown"), str)
    assert "Detailed Snapshot Summary" in result["summary_markdown"]
    assert isinstance(result.get("highlights"), list)
    assert len(result["highlights"]) > 0
    assert isinstance(result.get("coverage"), dict)
    assert "harm_type_pct" in result["coverage"]


def test_run_all_collects_errors_and_success():
    runner = NotebookRunner()
    df = _sample_df()

    class NotebookStub:
        cells = [
            {
                "id": "ok_cell",
                "type": "analysis",
                "analysis_type": "distribution",
                "config": {"field": "harm_type"},
            },
            {
                "id": "bad_cell",
                "type": "analysis",
                "analysis_type": "distribution",
                "config": {"field": "missing_col"},
            },
        ]

    results = runner.run_all(NotebookStub(), df)
    assert results["ok_cell"]["status"] == "success"
    assert results["bad_cell"]["status"] == "error"


def test_text_cell_with_analysis_type_executes_dispatch():
    runner = NotebookRunner()
    df = _sample_df()
    cell = {
        "id": "cell_009",
        "type": "text",
        "analysis_type": "text_sample",
        "config": {"field": "title", "n": 2},
    }
    result = runner.run_cell(cell, df)
    assert result["type"] == "text_list"
    assert len(result["samples"]) == 2


def test_static_text_cell_uses_content_field():
    runner = NotebookRunner()
    df = _sample_df()
    cell = {
        "id": "cell_010",
        "type": "text",
        "title": "Notes",
        "content": "hello world",
    }
    result = runner.run_cell(cell, df)
    assert result["type"] == "text"
    assert result["content"] == "hello world"


def test_summary_and_detailed_summary_support_custom_field_mapping():
    runner = NotebookRunner()
    df = pd.DataFrame(
        [
            {
                "event_date": "2024-01-01",
                "incident_year": 2024,
                "risk_label": "privacy",
                "industry_segment": "healthcare",
                "responsible_org": "Org A",
                "model_vendor": "Vendor X",
            },
            {
                "event_date": "2025-01-01",
                "incident_year": 2025,
                "risk_label": "bias",
                "industry_segment": "finance",
                "responsible_org": "Org B",
                "model_vendor": "Vendor X",
            },
        ]
    )

    summary = runner.run_cell(
        {
            "id": "summary_custom",
            "type": "analysis",
            "analysis_type": "summary",
            "config": {
                "date_field": "event_date",
                "top_fields": ["risk_label"],
                "harm_field": "risk_label",
                "sector_field": "industry_segment",
                "deployer_field": "responsible_org",
                "developer_field": "model_vendor",
            },
        },
        df,
    )
    assert summary["top_harm_type"] in {"privacy", "bias"}
    assert summary["top_sector"] in {"healthcare", "finance"}
    assert summary["unique_deployers"] == 2
    assert summary["unique_developers"] == 1

    narrative = runner.run_cell(
        {
            "id": "narrative_custom",
            "type": "analysis",
            "analysis_type": "detailed_summary",
            "config": {
                "top_n": 5,
                "year_field": "incident_year",
                "date_field": "event_date",
                "harm_field": "risk_label",
                "sector_field": "industry_segment",
                "deployer_field": "responsible_org",
                "developer_field": "model_vendor",
                "primary_label": "Risk label",
                "secondary_label": "Industry segment",
                "deployer_label": "Responsible org",
                "developer_label": "Model vendor",
            },
        },
        df,
    )
    assert narrative["type"] == "narrative"
    assert narrative["field_mapping"]["primary_field"] == "risk_label"
    assert "Risk label coverage" in narrative["summary_markdown"]
