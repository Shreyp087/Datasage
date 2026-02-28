import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_rate_limit_health_check():
    # Hit health endpoint 6 times to trigger 5/minute limit
    for _ in range(5):
        client.get("/health")
        
    res = client.get("/health")
    assert res.status_code == 429
    assert "Rate limit exceeded" in res.json()["detail"]

def test_api_upload_init_validation():
    # Missing required fields
    res = client.post("/api/v1/upload/init", json={"filename": "test.csv"})
    assert res.status_code == 422
    
def test_api_merge_suggest():
    res = client.post("/api/v1/merge/suggest", json={
        "dataset_id_1": "123",
        "dataset_id_2": "456"
    })
    # Since it's loading mock dfs of size 0, we expect empty suggestions or 200 OK
    assert res.status_code == 200
    assert isinstance(res.json(), list)
