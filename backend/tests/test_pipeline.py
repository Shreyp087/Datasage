import pytest
import pandas as pd
import numpy as np
from app.pipeline.preprocessor import PreprocessingPipeline
from app.pipeline.steps.schema_analyzer import SchemaAnalyzer
from app.pipeline.steps.missing_handler import MissingValueHandler

@pytest.fixture
def dirty_df():
    return pd.DataFrame({
        "id": range(100),
        "age": [25, np.nan, 30, 150, np.nan, 22] + [25]*94,
        "name": ["Alice", "Bob"] * 50,
        "date_joined": pd.date_range("2024-01-01", periods=100)
    })

def test_schema_analyzer(dirty_df):
    step = SchemaAnalyzer()
    class DummyContext: job_id="1"; domain="generic"; schema={}; warnings=[]
    ctx = DummyContext()
    res = step.run(dirty_df.copy(), ctx)
    
    assert ctx.schema["id"] == "id_col"
    assert ctx.schema["age"] == "feature_col"
    assert ctx.schema["date_joined"] == "datetime_col"

def test_missing_handler(dirty_df):
    step = MissingValueHandler()
    class DummyContext: job_id="1"; domain="generic"; schema={"age": "feature_col"}; warnings=[]
    ctx = DummyContext()
    
    # 2 missing in 100 rows = 2% (less than 5%, auto impute without flag if logic used, but logic uses >=0.05 for flag)
    # Our logic: < 0.05 impute num->median. >0.05 flag + impute.
    # Actually logic uses null_pct >= 0.05 to add flag. So 0.02 shouldn't add _was_missing.
    df_clean = step.run(dirty_df.copy(), ctx).df
    assert df_clean["age"].isnull().sum() == 0
    assert "age_was_missing" not in df_clean.columns

def test_full_pipeline_orchestrator(dirty_df):
    pipeline = PreprocessingPipeline(dataset_id="test-1", domain="healthcare", job_id="job-1")
    schema_mock = {"id": "id_col", "age": "feature_col", "name": "feature_col"}
    
    res = pipeline.execute(dirty_df.copy(), schema_mock)
    
    assert res.df is not None
    assert res.df["age"].isnull().sum() == 0 # imputed
    assert len(res.logs) > 0 # logged actions
