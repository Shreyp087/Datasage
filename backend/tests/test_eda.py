import pytest
import pandas as pd
import json
from app.eda.summarizer import generate_json_summary
from app.eda.engine import compress_for_agents

@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "amount": [10.5, 20.1, 15.0, 10.5, 30.2],
        "category": ["A", "B", "A", "C", "A"],
        "is_active": [True, False, True, True, False]
    })

def test_json_summary_structure(clean_df):
    schema = {"amount": "feature_col", "category": "feature_col", "is_active": "feature_col"}
    summary = generate_json_summary(clean_df, "finance", schema)
    
    assert "shape" in summary
    assert summary["shape"]["rows"] == 5
    assert summary["shape"]["cols"] == 3
    assert summary["domain"] == "finance"
    assert len(summary["columns"]) == 3
    assert summary["dataset_quality_score"] > 0

def test_compress_for_agents_token_limit(clean_df):
    schema = {"amount": "feature_col", "category": "feature_col", "is_active": "feature_col"}
    full_summary = generate_json_summary(clean_df, "finance", schema)
    compressed = compress_for_agents(full_summary)
    
    # Assert stripped big fields
    for col in compressed["columns"]:
        assert "min" not in col # Min/Max stripped to save tokens
        assert "kurtosis" not in col
        
    s = json.dumps(compressed)
    assert len(s) < 16000 # Under rough byte limits for 4k tokens
