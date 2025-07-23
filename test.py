import pytest
from httpx import AsyncClient
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def mock_dependencies():
    with patch("scheduler_app.api.fastapi_server.SchedulerClient") as MockSchedulerClient, \
         patch("scheduler_app.api.fastapi_server.RedisCoordinator") as MockRedisCoordinator:

        mock_client = MockSchedulerClient.return_value
        mock_redis = MockRedisCoordinator.return_value

        mock_client.get_all_jobs.return_value = [{"id": "job1"}, {"id": "job2"}]
        mock_redis.get_progress.return_value = {"progress": 70}

        yield

@patch("scheduler_app.api.fastapi_server.client")
def test_get_jobs(mock_client):
    mock_client.get_all_jobs.return_value = [{"id": "job1"}, {"id": "job2"}]
    from scheduler_app.api.fastapi_server import app
    client = TestClient(app)
    response = client.get("/api/jobs")
    assert response.status_code == 200
    assert len(response.json()) == 2

@patch("scheduler_app.api.fastapi_server.redis_coord")
def test_get_job_progress(mock_redis):
    mock_redis.get_progress.return_value = {"progress": 70}
    from scheduler_app.api.fastapi_server import app
    client = TestClient(app)
    response = client.get("/api/progress/test_workflow")
    assert response.status_code == 200
    assert response.json()["progress"] == 70

@patch("scheduler_app.api.fastapi_server.client")
def test_read_root_success(mock_client):
    mock_client.get_all_jobs.return_value = []
    from scheduler_app.api.fastapi_server import app
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

@patch("scheduler_app.api.fastapi_server.client")
@patch("scheduler_app.api.fastapi_server.get_logger")
def test_read_root_failure(mock_logger, mock_client):
    mock_client.get_all_jobs.side_effect = Exception("Mock failure")
    mock_logger.return_value = MagicMock()
    from scheduler_app.api.fastapi_server import app
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_pause_job(mock_job):
    from scheduler_app.api.fastapi_server import app
    with patch.object(app.state, "scheduler", create=True) as mock_scheduler:
        mock_scheduler.pause_job = MagicMock()
        client = TestClient(app)
        response = client.post("/api/jobs/test_job/pause")
        assert response.status_code == 200

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_resume_job(mock_job):
    from scheduler_app.api.fastapi_server import app
    with patch.object(app.state, "scheduler", create=True) as mock_scheduler:
        mock_scheduler.resume_job = MagicMock()
        client = TestClient(app)
        response = client.post("/api/jobs/test_job/resume")
        assert response.status_code == 200

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_delete_job(mock_job):
    from scheduler_app.api.fastapi_server import app
    with patch.object(app.state, "scheduler", create=True) as mock_scheduler:
        mock_scheduler.remove_job = MagicMock()
        client = TestClient(app)
        response = client.delete("/api/jobs/test_job")
        assert response.status_code == 200

@patch("scheduler_app.api.fastapi_server._job_or_404")
def test_trigger_job(mock_job):
    from scheduler_app.api.fastapi_server import app
    with patch.object(app.state, "scheduler", create=True) as mock_scheduler:
        mock_scheduler.add_job = MagicMock()
        client = TestClient(app)
        response = client.post("/api/jobs/fetch_labels/trigger")
        assert response.status_code == 200
        mock_scheduler.add_job.assert_called_once()
