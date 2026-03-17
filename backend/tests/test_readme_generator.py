from datetime import datetime, timezone

from app.notebooks.readme_generator import ReadmeGenerator


class _Domain:
    value = "ai_incidents"


class _DatasetStub:
    name = "AIID Test Snapshot"
    domain = _Domain()
    file_size_bytes = 1024
    uploaded_at = datetime.now(timezone.utc)
    description = None
    schema_json = {
        "source": "AI Incident Database",
        "snapshot_date": "2026-02-23",
        "snapshot_url": "https://incidentdatabase.ai/research/snapshots/",
    }


def test_readme_includes_detailed_notebook_summary_when_available():
    generator = ReadmeGenerator()
    dataset = _DatasetStub()
    eda_json = {
        "dataset_quality_score": 94,
        "shape": {"rows": 100, "cols": 12},
        "columns": [],
        "warnings": [],
    }
    notebook_results = {
        "cell_002": {
            "result": {
                "total_incidents": 100,
                "date_range": {"earliest": "2015-01-01", "latest": "2026-01-01"},
                "top_harm_type": "Discrimination",
                "top_sector": "Finance",
            }
        },
        "cell_013": {
            "result": {
                "highlights": [
                    "Total documented incidents analyzed: 100.",
                    "Most common harm type: Discrimination (25 incidents, 25.0%).",
                ],
                "coverage": {
                    "harm_type_pct": 80.0,
                    "sector_of_deployment_pct": 75.0,
                    "deployer_pct": 65.0,
                    "developer_pct": 60.0,
                },
            }
        },
    }

    rendered = generator.generate(
        dataset=dataset,
        eda_json=eda_json,
        agent_reports=[],
        notebook_results=notebook_results,
    )

    assert "Detailed Notebook Summary" in rendered
    assert "Most common harm type: Discrimination" in rendered
    assert "Classification Coverage" in rendered
    assert "Developer Coverage: 60.0%" in rendered

