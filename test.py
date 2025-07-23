# ===============================
# Unit Test for API Endpoints
# ===============================

import pytest
from httpx import AsyncClient
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from scheduler_app.api.fastapi_server import app

client = TestClient(app)

@patch("scheduler_app.api.fastapi_server.client")
def test_get_jobs(mock_client):
    mock_client.get_all_jobs.return_value = [{"id": "job1"}, {"id": "job2"}]
    response = client.get("/api/jobs")
    assert response.status_code == 200
    assert len(response.json()) == 2

@patch("scheduler_app.api.fastapi_server.redis_coord")
def test_get_job_progress(mock_redis):
    mock_redis.get_progress.return_value = {"progress": 70}
    response = client.get("/api/progress/test_workflow")
    assert response.status_code == 200
    assert response.json()["progress"] == 70

@patch("scheduler_app.api.fastapi_server.client")
def test_read_root_success(mock_client):
    mock_client.get_all_jobs.return_value = []
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

@patch("scheduler_app.api.fastapi_server.get_logger")
@patch("scheduler_app.api.fastapi_server.client")
def test_read_root_failure(mock_client, mock_logger):
    mock_client.get_all_jobs.side_effect = Exception("Mock failure")
    mock_logger.return_value = MagicMock()
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_pause_job(mock_job):
    request = MagicMock()
    request.app.state.scheduler.pause_job = MagicMock()
    response = client.post("/api/jobs/test_job/pause", json={}, headers={"content-type": "application/json"})
    assert response.status_code == 200

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_resume_job(mock_job):
    request = MagicMock()
    request.app.state.scheduler.resume_job = MagicMock()
    response = client.post("/api/jobs/test_job/resume", json={}, headers={"content-type": "application/json"})
    assert response.status_code == 200

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_delete_job(mock_job):
    request = MagicMock()
    request.app.state.scheduler.remove_job = MagicMock()
    response = client.delete("/api/jobs/test_job")
    assert response.status_code == 200
