import pytest
import pandas as pd
import numpy as np
import os
import tempfile
from app.pipeline.loader import DatasetLoader

@pytest.fixture
def sample_csv():
    fd, path = tempfile.mkstemp(suffix=".csv")
    df = pd.DataFrame({
        "id": range(10),
        "name": [f"User{i}" for i in range(10)],
        "score": np.random.randn(10)
    })
    df.to_csv(path, index=False)
    yield path
    os.close(fd)
    os.remove(path)

@pytest.fixture
def sample_parquet():
    fd, path = tempfile.mkstemp(suffix=".parquet")
    df = pd.DataFrame({
        "id": range(10),
        "status": ["active", "inactive"] * 5
    })
    df.to_parquet(path, index=False)
    yield path
    os.close(fd)
    os.remove(path)

def test_loader_csv(sample_csv):
    loader = DatasetLoader(sample_csv)
    res = loader.load()
    assert res.df is not None
    assert len(res.df) == 10
    assert "id_col" in res.inferred_schema.values()

def test_loader_parquet(sample_parquet):
    loader = DatasetLoader(sample_parquet)
    res = loader.load()
    assert res.df is not None
    assert len(res.df) == 10

def test_loader_missing_file():
    with pytest.raises(Exception):
        loader = DatasetLoader("non_existent_file_xyz.csv")
        loader.load()
