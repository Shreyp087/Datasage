import pandas as pd
import tarfile

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

    assert "harm_type" in merged.columns
    assert "entities_involved" in merged.columns

    row_one = merged.loc[merged["incident_id"] == "1"].iloc[0]
    assert row_one["harm_type"] == "Bias"
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


def test_load_incidents_csv_handles_snapshot_style_classification_files_and_reports(tmp_path):
    ingestor = AIIDIngestor()

    incidents_path = tmp_path / "incidents.csv"
    cset_v1_path = tmp_path / "classifications_CSETv1.csv"
    cset_v0_path = tmp_path / "classifications_CSETv0.csv"
    reports_path = tmp_path / "reports.csv"

    pd.DataFrame(
        [
            {
                "incident_id": 11,
                "date": "2025-01-02",
                "reports": "[101, 102]",
                "Alleged deployer of AI system": '["Agency A"]',
                "Alleged developer of AI system": '["Vendor A"]',
                "title": "Incident 11",
            },
            {
                "incident_id": 12,
                "date": "2025-01-03",
                "reports": "[103]",
                "Alleged deployer of AI system": '["Agency B"]',
                "Alleged developer of AI system": '["Vendor B"]',
                "title": "Incident 12",
            },
        ]
    ).to_csv(incidents_path, index=False)

    pd.DataFrame(
        [
            {
                "Incident ID": 11,
                "Harm Domain": "Safety",
                "Sector of Deployment": "Transportation",
            },
            {
                "Incident ID": 12,
                "Harm Domain": "Privacy",
                "Sector of Deployment": "Healthcare",
            },
        ]
    ).to_csv(cset_v1_path, index=False)

    pd.DataFrame(
        [
            {
                "Incident ID": 11,
                "Harm Type": "Physical",
            }
        ]
    ).to_csv(cset_v0_path, index=False)

    pd.DataFrame(
        [
            {"report_number": 101, "source_domain": "example.com", "title": "Report A", "date_published": "2025-01-05"},
            {"report_number": 102, "source_domain": "news.org", "title": "Report B", "date_published": "2025-01-10"},
            {"report_number": 103, "source_domain": "news.org", "title": "Report C", "date_published": "2025-01-11"},
        ]
    ).to_csv(reports_path, index=False)

    files = {
        "incidents.csv": str(incidents_path),
        "classifications_CSETv1.csv": str(cset_v1_path),
        "classifications_CSETv0.csv": str(cset_v0_path),
        "reports.csv": str(reports_path),
    }

    merged = ingestor.normalize(ingestor.load_incidents_csv(files))

    assert "harm_type" in merged.columns
    assert "sector_of_deployment" in merged.columns
    assert "report_count" in merged.columns
    assert "report_sources" in merged.columns
    assert "allegeddeployerofaisystem_primary" in merged.columns
    assert "allegeddeveloperofaisystem_primary" in merged.columns

    row_eleven = merged.loc[merged["incident_id"] == "11"].iloc[0]
    assert row_eleven["harm_type"] == "Safety"
    assert row_eleven["sector_of_deployment"] == "Transportation"
    assert int(row_eleven["report_count"]) == 2
    assert "example.com" in str(row_eleven["report_sources"])
    assert "news.org" in str(row_eleven["report_sources"])
    assert row_eleven["allegeddeployerofaisystem_primary"] == "Agency A"
    assert row_eleven["allegeddeveloperofaisystem_primary"] == "Vendor A"


def test_ai_incidents_domain_profile_and_role_mapping():
    profile = get_domain_profile("ai_incidents")
    assert profile["display_name"] == "AI Incident Database"
    assert "incident_id" in profile["known_columns"]
    assert to_pipeline_role(profile["known_columns"]["title"]["role"]) == "text_col"


def test_extract_archive_and_json_fallback(tmp_path):
    ingestor = AIIDIngestor()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    incidents_json = archive_dir / "incidents.json"
    incidents_json.write_text(
        '\n'.join(
            [
                '{"incident_id":"1","title":"Incident One","date":"2026-02-23"}',
                '{"incident_id":"2","title":"Incident Two","date":"2026-02-24"}',
            ]
        ),
        encoding="utf-8",
    )

    archive_path = tmp_path / "snapshot.tar.bz2"
    with tarfile.open(archive_path, "w:bz2") as tar:
        tar.add(incidents_json, arcname="incidents.json")

    extracted_dir = tmp_path / "extracted"
    files = ingestor.extract_archive(str(archive_path), str(extracted_dir))
    df = ingestor.load_incidents_csv(files)

    assert len(df) == 2
    assert "title" in df.columns
