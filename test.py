import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from scheduler_app.api.server import app

# Patch Mongo client and Redis coordinator globally for all tests
@pytest.fixture(autouse=True)
def patch_mongo_and_redis():
    with patch("scheduler_app.api.client.get_mongo_client") as mock_get_client, \
         patch("scheduler_app.api.client.RedisCoordinator") as mock_redis_coord:

        mock_mongo_client = MagicMock()
        mock_status_collection = MagicMock()
        mock_status_collection.find.return_value = [
            {
                "job_id": "fetch_projects",
                "history": [
                    {"status": "Success", "trace_id": "abc123", "progress": {"done": 100}}
                ]
            }
        ]
        mock_mongo_client.__getitem__.return_value.__getitem__.return_value = mock_status_collection
        mock_get_client.return_value = mock_mongo_client

        mock_redis = MagicMock()
        mock_redis.counter_exists.return_value = False
        mock_redis_coord.return_value = mock_redis

        yield

def test_get_jobs_api():
    client = TestClient(app)
    response = client.get("/api/jobs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_read_root_html():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]