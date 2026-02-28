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
