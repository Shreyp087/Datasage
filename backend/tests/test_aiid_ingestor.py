import pandas as pd

from app.core.domain_profiles import get_domain_profile, to_pipeline_role
from app.pipeline.aiid_ingestor import AIIDIngestor


def test_extract_first_list_string():
    ingestor = AIIDIngestor()
    assert ingestor._extract_first("['first', 'second']") == "first"
    assert ingestor._extract_first("plain_value") == "plain_value"
    assert ingestor._extract_first(None) is None


def test_load_incidents_csv_merges_classifications_and_entities(tmp_path):
    ingestor = AIIDIngestor()

    incidents_path = tmp_path / "incidents.csv"
    classifications_path = tmp_path / "classifications.csv"
    entities_path = tmp_path / "entities.csv"

    pd.DataFrame(
        [
            {"incident_id": 1, "title": "Incident One", "description": "Desc 1", "date": "2026-02-23"},
            {"incident_id": 2, "title": "Incident Two", "description": "Desc 2", "date": "2026-02-24"},
        ]
    ).to_csv(incidents_path, index=False)

    pd.DataFrame(
        [
            {"incident_id": 1, "Harm.Type": "Bias", "Sector.of.Deployment": "Hiring"},
        ]
    ).to_csv(classifications_path, index=False)

    pd.DataFrame(
        [
            {"incident_id": 1, "name": "Org A"},
            {"incident_id": 1, "name": "Org B"},
            {"incident_id": 2, "name": "Org C"},
        ]
    ).to_csv(entities_path, index=False)

    files = {
        "incidents.csv": str(incidents_path),
        "classifications.csv": str(classifications_path),
        "entities.csv": str(entities_path),
    }

    merged = ingestor.load_incidents_csv(files)

    assert "Harm.Type" in merged.columns
    assert "entities_involved" in merged.columns

    row_one = merged.loc[merged["incident_id"] == "1"].iloc[0]
    assert row_one["Harm.Type"] == "Bias"
    assert "Org A" in row_one["entities_involved"]
    assert "Org B" in row_one["entities_involved"]


def test_normalize_generates_date_parts_and_primary_columns():
    ingestor = AIIDIngestor()
    df = pd.DataFrame(
        [
            {
                "incident_id": 1,
                "date": "2026-02-23",
                "AllegedDeployerOfAISystem": "['Vendor A', 'Vendor B']",
                "Harm.Type": "Privacy",
            }
        ]
    )

    normalized = ingestor.normalize(df)

    assert "year" in normalized.columns
    assert "month" in normalized.columns
    assert "harm_type" in normalized.columns
    assert "allegeddeployerofaisystem_primary" in normalized.columns
    assert normalized.loc[0, "allegeddeployerofaisystem_primary"] == "Vendor A"
    assert int(normalized.loc[0, "year"]) == 2026
    assert int(normalized.loc[0, "month"]) == 2


def test_ai_incidents_domain_profile_and_role_mapping():
    profile = get_domain_profile("ai_incidents")
    assert profile["display_name"] == "AI Incident Database"
    assert "incident_id" in profile["known_columns"]
    assert to_pipeline_role(profile["known_columns"]["title"]["role"]) == "text_col"
