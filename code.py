# ==============================================================================
# File: requirements.txt (NEW)
# Description: Lists all project dependencies.
# ==============================================================================
# fastapi
# uvicorn
# pydantic-settings
# pymongo
# apscheduler
# redis
# pendulum
# python-dotenv
# requests


# ==============================================================================
# File: .env (Example)
# Description: Environment variables for configuration.
# ==============================================================================
# MONGO_URI=mongodb://user:pass@host:port/medallion
MONGO_DB = medallion
MONGO_SCHEDULER_COLLECTION = apscheduler
MONGO_STATUS_COLLECTION = job_statuses
REDIS_URL = redis: // localhost: 6379 / 0
PRODUCER_API_URL = http: // localhost: 8001

# ==============================================================================
# Directory: scheduler_app/
# Description: The main application package.
# ==============================================================================

# ==============================================================================
# File: scheduler_app/__init__.py
# Description: Makes the directory a Python package.
# ==============================================================================


# ==============================================================================
# File: scheduler_app/utils/config.py (MODIFIED)
# Description: Centralized configuration management using Pydantic.
# ==============================================================================
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Manages all application settings. Loads from environment variables and .env file.
    """
    MONGO_URI: str | None = None
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_USER: str | None = None
    MONGO_PASS: str | None = None
    MONGO_DB: str = "medallion"
    MONGO_SCHEDULER_COLLECTION: str = "apscheduler"
    MONGO_STATUS_COLLECTION: str = "job_statuses"

    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_COUNTER_TTL_SECONDS: int = 86400  # 24 hours

    PRODUCER_API_URL: str = "http://localhost:8001"

    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# ==============================================================================
# File: scheduler_app/utils/logger.py (NEW)
# Description: Sets up a structured, application-wide logger.
# ==============================================================================
import logging
import json
from scheduler_app.utils.config import settings


class JsonFormatter(logging.Formatter):
    """Formats log records as JSON strings."""

    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
        }
        # Add extra context if it exists
        if hasattr(record, 'context'):
            log_record.update(record.context)

        # Add exception info if it exists
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def get_logger(name: str, **context):
    """
    Returns a logger instance with a JSON formatter and optional context.
    """
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times
    if not logger.handlers:
        logger.setLevel(settings.LOG_LEVEL)
        handler = logging.StreamHandler()
        formatter = JsonFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False  # Prevent root logger from duplicating messages

    # Create a new adapter to inject context into log records
    adapter = logging.LoggerAdapter(logger, {'context': context})
    return adapter


# Configure the root logger to catch any unhandled logs
logging.basicConfig(level=settings.LOG_LEVEL, format='%(levelname)s: %(message)s')

# ==============================================================================
# File: scheduler_app/utils/database.py (MODIFIED)
# Description: Handles all database connection logic.
# ==============================================================================
from pymongo import MongoClient
from scheduler_app.utils.config import settings


def get_mongo_client() -> MongoClient:
    """Creates and returns a PyMongo client instance based on the application settings."""
    if settings.MONGO_URI:
        return MongoClient(settings.MONGO_URI)
    if settings.MONGO_USER and settings.MONGO_PASS:
        uri = (f"mongodb://{settings.MONGO_USER}:{settings.MONGO_PASS}@"
               f"{settings.MONGO_HOST}:{settings.MONGO_PORT}/{settings.MONGO_DB}?authSource=admin")
        return MongoClient(uri)
    return MongoClient(settings.MONGO_HOST, settings.MONGO_PORT)


# ==============================================================================
# File: scheduler_app/utils/redis_coordinator.py (MODIFIED)
# Description: Manages distributed counters and pub/sub events for task coordination.
# ==============================================================================
import redis
from scheduler_app.utils.config import settings
from scheduler_app.utils.logger import get_logger

logger = get_logger(__name__)


class RedisCoordinator:
    """
    Manages counters and publishes completion events using Redis.
    """

    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.completion_channel = "workflow_completion_events"
        self._register_scripts()

    def _register_scripts(self):
        lua_script = """
        local completed_key = KEYS[1]
        local total_key = KEYS[2]
        local channel = ARGV[1]
        local workflow_id = ARGV[2]
        if redis.call('EXISTS', total_key) == 0 then return 0 end
        local completed = redis.call('INCR', completed_key)
        local total = tonumber(redis.call('GET', total_key))
        if total > 0 and completed >= total then
            redis.call('PUBLISH', channel, workflow_id)
            return 1
        end
        return 0
        """
        self.finisher_script = self.redis.register_script(lua_script)

    def init_counter(self, workflow_id: str, total_count: int, job_id: str):
        logger.info(f"Initializing Redis counter for job '{job_id}'.", workflow_id=workflow_id, total_items=total_count)
        pipe = self.redis.pipeline()
        pipe.set(f"counter:{workflow_id}:completed", 0, ex=settings.REDIS_COUNTER_TTL_SECONDS)
        pipe.set(f"counter:{workflow_id}:total", total_count, ex=settings.REDIS_COUNTER_TTL_SECONDS)
        pipe.set(f"workflow_info:{workflow_id}", job_id, ex=settings.REDIS_COUNTER_TTL_SECONDS)
        pipe.execute()

    def increment(self, workflow_id: str):
        completed_key = f"counter:{workflow_id}:completed"
        total_key = f"counter:{workflow_id}:total"
        try:
            result = self.finisher_script(keys=[completed_key, total_key], args=[self.completion_channel, workflow_id])
            if result == 1:
                logger.info("Final sub-task completed. Published completion event.", workflow_id=workflow_id)
        except redis.exceptions.ResponseError as e:
            logger.error(f"Lua script error for workflow.", workflow_id=workflow_id, error=str(e))

    def get_progress(self, workflow_id: str) -> dict:
        pipe = self.redis.pipeline()
        pipe.get(f"counter:{workflow_id}:completed")
        pipe.get(f"counter:{workflow_id}:total")
        results = pipe.execute()
        return {"processed": int(results[0] or 0), "expected": int(results[1] or 0)}


# ==============================================================================
# File: scheduler_app/jobs/definitions.py (MODIFIED)
# Description: Defines the jobs and their dependencies.
# ==============================================================================
JOB_DEFINITIONS = [
    {
        "id": "start_repo_ingestion_workflow",
        "name": "Start Repo Ingestion Workflow",
        "trigger": "cron",
        "trigger_args": {"hour": 1},
        "function": "start_repo_ingestion_workflow",
        "is_workflow_starter": True
    },
    {
        "id": "fetch_labels",
        "name": "Fetch All Labels",
        "function": "fetch_labels",
        "triggers_on_completion_of": "start_repo_ingestion_workflow"
    },
]

# ==============================================================================
# File: scheduler_app/jobs/api_caller.py (MODIFIED)
# Description: Handles API requests for jobs.
# ==============================================================================
import requests
from scheduler_app.utils.logger import get_logger


def trigger_api_call(url: str, method: str, trace_id: str, headers: dict = None, json_data: dict = None) -> bool:
    logger = get_logger(__name__, trace_id=trace_id)
    logger.info(f"Triggering API call: {method} {url}")
    request_headers = headers.copy() if headers else {}
    request_headers['X-Trace-ID'] = trace_id
    try:
        response = requests.request(method=method.upper(), url=url, headers=request_headers, json=json_data, timeout=30)
        response.raise_for_status()
        logger.info(f"API call successful.", url=url, status_code=response.status_code)
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed.", url=url, error=str(e))
        return False


# ==============================================================================
# File: scheduler_app/jobs/functions.py (MODIFIED)
# Description: The functions for the scheduler's jobs.
# ==============================================================================
from scheduler_app.jobs.api_caller import trigger_api_call
from scheduler_app.utils.config import settings
from scheduler_app.utils.logger import get_logger


def fetch_all_repos_from_mongo(workflow_id: str):
    logger = get_logger(__name__, workflow_id=workflow_id)
    logger.info("Gathering all repo data from MongoDB to start label fetching.")
    return []


def start_repo_ingestion_workflow(trace_id: str, job_id: str):
    logger = get_logger(__name__, trace_id=trace_id, job_id=job_id)
    logger.info("Calling Producer to start repo ingestion workflow.")
    producer_url = f"{settings.PRODUCER_API_URL}/produce/fetch-repos"
    return trigger_api_call(
        url=producer_url, method="POST", trace_id=trace_id,
        json_data={"workflow_id": trace_id, "job_id": job_id}
    )


def fetch_labels(trace_id: str, job_id: str):
    logger = get_logger(__name__, trace_id=trace_id, job_id=job_id)
    logger.info("All repos processed. Now starting fetch_labels workflow.")
    repos_data = fetch_all_repos_from_mongo(workflow_id=trace_id)
    producer_url = f"{settings.PRODUCER_API_URL}/produce/fetch-labels"
    logger.info(f"Calling producer to start label ingestion for {len(repos_data)} repos.")
    return trigger_api_call(
        url=producer_url, method="POST", trace_id=trace_id,
        json_data={"repos": repos_data, "workflow_id": trace_id, "job_id": job_id}
    )


JOB_FUNCTIONS = {
    'start_repo_ingestion_workflow': start_repo_ingestion_workflow,
    'fetch_labels': fetch_labels,
}

# ==============================================================================
# File: conceptual_celery_worker.py (For Reference)
# ==============================================================================
# from celery import Celery
# from celery.signals import task_success
# from scheduler_app.utils.redis_coordinator import RedisCoordinator
# from scheduler_app.utils.config import settings
# from scheduler_app.utils.logger import get_logger

# celery_app = Celery('tasks', broker=settings.REDIS_URL)

# @celery_app.task(bind=True)
# def process_single_repo(self, repo_url: str, workflow_id: str):
#     logger = get_logger(__name__, workflow_id=workflow_id, repo_url=repo_url)
#     logger.info("Worker processing repo.")
#     # ... your actual logic ...
#     return {"status": "success", "repo": repo_url}

# @task_success.connect(sender=process_single_repo)
# def on_repo_process_success(sender=None, **kwargs):
#     task_kwargs = sender.request.kwargs
#     workflow_id = task_kwargs.get('workflow_id')
#     if not workflow_id:
#         get_logger(__name__).error("Could not find workflow_id in successful task signal's kwargs.")
#         return
#     get_logger(__name__, workflow_id=workflow_id).info("Success signal received for task.")
#     redis_coord = RedisCoordinator()
#     redis_coord.increment(workflow_id=workflow_id)


# ==============================================================================
# File: scheduler_app/core/scheduler.py (MODIFIED)
# Description: The core logic for the scheduler application.
# ==============================================================================
import uuid
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from scheduler_app.jobs.definitions import JOB_DEFINITIONS
from scheduler_app.jobs.functions import JOB_FUNCTIONS
from scheduler_app.utils.database import get_mongo_client
from scheduler_app.utils.config import settings
from scheduler_app.utils.logger import get_logger


def task_wrapper(job_id: str, *args):
    """A robust wrapper that manages the full lifecycle of a job run."""
    # Each job run, including dependent ones, gets its own unique trace_id.
    trace_id = str(uuid.uuid4())
    logger = get_logger(__name__, job_id=job_id, trace_id=trace_id)

    mongo_client = None
    try:
        mongo_client = get_mongo_client()
        status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
        logger.info("Task wrapper executing.")
        run_record = {"trace_id": trace_id, "status": "Running",
                      "timestamp_utc": datetime.now(timezone.utc).isoformat()}
        status_collection.update_one({"job_id": job_id}, {"$push": {"history": {"$each": [run_record], "$slice": -50}},
                                                          "$set": {"job_id": job_id}}, upsert=True)

        job_id_to_function = {job["id"]: job["function"] for job in JOB_DEFINITIONS}
        func_name = job_id_to_function.get(job_id)
        if not func_name: raise ValueError(f"No function name defined for job_id: {job_id}")

        func = JOB_FUNCTIONS.get(func_name)
        if not func: raise ValueError(f"No function found for name: {func_name}")

        if not func(trace_id=trace_id, job_id=job_id): raise Exception("Custom function reported a failure.")

        status_collection.update_one({"job_id": job_id, "history.trace_id": trace_id}, {
            "$set": {"history.$.status": "Success",
                     "history.$.end_timestamp_utc": datetime.now(timezone.utc).isoformat()}})
    except Exception as e:
        logger.error("Execution failed.", error=str(e))
        if mongo_client:
            status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
            status_collection.update_one({"job_id": job_id, "history.trace_id": trace_id}, {
                "$set": {"history.$.status": "Failure",
                         "history.$.end_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                         "history.$.exception": str(e)}})
    finally:
        if mongo_client: mongo_client.close()


class SchedulerManager:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.job_definitions = {job['id']: job for job in JOB_DEFINITIONS}
        self.mongo_client = get_mongo_client()
        self.scheduler = self._initialize_scheduler()

    def _initialize_scheduler(self) -> BackgroundScheduler:
        jobstores = {
            'default': MongoDBJobStore(database=settings.MONGO_DB, collection=settings.MONGO_SCHEDULER_COLLECTION,
                                       client=self.mongo_client)}
        executors = {'default': ThreadPoolExecutor(20), 'processpool': ProcessPoolExecutor(5)}
        job_defaults = {'coalesce': False, 'max_instances': 1}
        scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                        timezone='UTC')
        scheduler.add_listener(self.job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        return scheduler

    def job_event_listener(self, event):
        logger = get_logger(__name__, job_id=event.job_id)
        if event.exception:
            logger.error(f"APScheduler-level error.", error=str(event.exception))
        else:
            logger.info(f"APScheduler successfully triggered job.")

    def schedule_all_jobs(self):
        for job_id, job_def in self.job_definitions.items():
            if 'trigger' in job_def:
                job_kwargs = {'id': job_id, 'name': job_def.get('name', job_id),
                              'func': 'scheduler_app.core.scheduler:task_wrapper', 'args': [job_id],
                              'trigger': job_def['trigger'], 'executor': job_def.get('executor', 'default'),
                              'max_instances': job_def.get('max_instances', 1), 'replace_existing': True,
                              **job_def.get('trigger_args', {})}
                self.scheduler.add_job(**job_kwargs)
                self.logger.info(f"Scheduled job.", job_id=job_id)

    def start(self):
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()


# ==============================================================================
# File: scheduler_app/api/client.py (MODIFIED - Renamed from scheduler_client.py)
# Description: A client to interact with the MongoDB job store for READ-ONLY operations.
# ==============================================================================
import pendulum
from apscheduler.jobstores.mongodb import MongoDBJobStore
from scheduler_app.utils.database import get_mongo_client
from scheduler_app.utils.config import settings
from scheduler_app.jobs.definitions import JOB_DEFINITIONS


class SchedulerClient:
    def __init__(self):
        self.mongo_client = get_mongo_client()
        self.jobstore = MongoDBJobStore(database=settings.MONGO_DB, collection=settings.MONGO_SCHEDULER_COLLECTION,
                                        client=self.mongo_client)
        self.status_collection = self.mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]

    def get_all_jobs(self):
        jobs = self.jobstore.get_all_jobs()
        statuses = {s['job_id']: s for s in self.status_collection.find()}
        job_defs = {job['id']: job for job in JOB_DEFINITIONS}
        job_list = []
        for job in jobs:
            status_info = statuses.get(job.id, {})
            last_run = status_info.get("history", [{}])[-1]
            last_status = last_run.get("status", "Unknown")

            if job.next_run_time is None:
                ui_status = "Paused"
            elif last_status == "Running":
                ui_status = "Running"
            elif last_status == "Failure":
                ui_status = "Failing"
            else:
                ui_status = "Scheduled"

            job_list.append({
                "id": job.id, "name": job.name,
                "next_run_time_utc": job.next_run_time.isoformat() if job.next_run_time else "N/A",
                "next_run_time_human": pendulum.instance(
                    job.next_run_time).diff_for_humans() if job.next_run_time else "Paused",
                "last_status": last_status,
                "ui_status": ui_status,
                "last_workflow_id": last_run.get("trace_id"),
                "is_workflow_starter": job_defs.get(job.id, {}).get("is_workflow_starter", False)
            })
        return job_list


# ==============================================================================
# File: scheduler_app/api/server.py (MODIFIED - Renamed from api_server.py)
# Description: The public-facing FastAPI server for the combined pod.
# ==============================================================================
import uuid
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from scheduler_app.api.client import SchedulerClient
from scheduler_app.utils.redis_coordinator import RedisCoordinator
from scheduler_app.utils.logger import get_logger

app = FastAPI(title="Scheduler Management API")
client = SchedulerClient()
redis_coord = RedisCoordinator()
templates = Jinja2Templates(directory="templates")


def _job_or_404(request: Request, job_id: str):
    scheduler = request.app.state.scheduler
    job = scheduler.get_job(job_id)
    if not job: raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    logger = get_logger(__name__)
    try:
        jobs = client.get_all_jobs()
        return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})
    except Exception as e:
        logger.error("Failed to load jobs for UI.", error=str(e))
        return templates.TemplateResponse("index.html", {"request": request, "jobs": [], "error": str(e)})


@app.get("/api/jobs")
async def get_jobs(): return client.get_all_jobs()


@app.get("/api/progress/{workflow_id}")
async def get_job_progress(workflow_id: str):
    return redis_coord.get_progress(workflow_id)


@app.post("/api/jobs/{job_id}/pause")
async def pause_job_endpoint(request: Request, job_id: str):
    _job_or_404(request, job_id)
    request.app.state.scheduler.pause_job(job_id)
    return {"status": "success", "message": f"Job '{job_id}' paused."}


@app.post("/api/jobs/{job_id}/resume")
async def resume_job_endpoint(request: Request, job_id: str):
    _job_or_404(request, job_id)
    request.app.state.scheduler.resume_job(job_id)
    return {"status": "success", "message": f"Job '{job_id}' resumed."}


@app.post("/api/jobs/{job_id}/trigger")
async def trigger_job_endpoint(request: Request, job_id: str):
    _job_or_404(request, job_id)
    run_id = f"{job_id}_manual_{uuid.uuid4()}"
    request.app.state.scheduler.add_job('scheduler_app.core.scheduler:task_wrapper', trigger='date', args=[job_id],
                                        id=run_id, name=f"{job_id} (Manual Run)", replace_existing=False)
    return {"status": "success", "message": f"Job '{job_id}' triggered for immediate execution."}


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(request: Request, job_id: str):
    _job_or_404(request, job_id)
    request.app.state.scheduler.remove_job(job_id)
    return {"status": "success", "message": f"Job '{job_id}' deleted."}


# ==============================================================================
# File: main.py (MODIFIED)
# Description: The single entry point for the combined pod.
# ==============================================================================
import uvicorn
import threading
import redis
from contextlib import asynccontextmanager
from scheduler_app.core.scheduler import SchedulerManager
from scheduler_app.api.server import app as fastapi_app
from scheduler_app.utils.config import settings
from scheduler_app.utils.redis_coordinator import RedisCoordinator
from scheduler_app.jobs.definitions import JOB_DEFINITIONS
from scheduler_app.utils.logger import get_logger

logger = get_logger(__name__)
scheduler_manager = SchedulerManager()
redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)


def pubsub_listener_loop():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(RedisCoordinator().completion_channel)
    logger.info("Pub/Sub listener started.", channel=RedisCoordinator().completion_channel)
    for message in pubsub.listen():
        if message["type"] == "message":
            completed_workflow_id = message["data"]
            logger.info("Received completion event.", workflow_id=completed_workflow_id)

            workflow_info_key = f"workflow_info:{completed_workflow_id}"
            completed_job_id = redis_client.get(workflow_info_key)

            if not completed_job_id:
                logger.warning("Could not find original job for completed workflow.", workflow_id=completed_workflow_id)
                continue

            for job_def in JOB_DEFINITIONS:
                if job_def.get("triggers_on_completion_of") == completed_job_id:
                    next_job_id = job_def["id"]
                    logger.info(f"Triggering dependent job.", next_job_id=next_job_id,
                                completed_job_id=completed_job_id)
                    try:
                        run_id = f"{next_job_id}_for_{completed_workflow_id}"
                        # IMPORTANT: We do NOT pass the old trace_id. The wrapper will generate a new one.
                        scheduler_manager.scheduler.add_job(
                            'scheduler_app.core.scheduler:task_wrapper', trigger='date', args=[next_job_id],
                            id=run_id, name=f"{job_def['name']} (after {completed_job_id})", replace_existing=False
                        )
                    except Exception as e:
                        logger.error(f"Error while triggering dependent job.", next_job_id=next_job_id, error=str(e))

            redis_client.delete(workflow_info_key)


@asynccontextmanager
async def lifespan(app):
    app.state.scheduler = scheduler_manager.scheduler
    scheduler_manager.schedule_all_jobs()
    scheduler_manager.start()
    pubsub_thread = threading.Thread(target=pubsub_listener_loop, daemon=True)
    pubsub_thread.start()
    yield
    logger.info("Shutting down scheduler...")
    scheduler_manager.shutdown()


fastapi_app.router.lifespan_context = lifespan

if __name__ == "__main__":
    logger.info("Starting combined Scheduler and API server...")
    uvicorn.run("scheduler_app.main:fastapi_app", host="0.0.0.0", port=8000, reload=True)


# ==============================================================================
# Directory: tests/
# Description: Contains all unit tests for the application.
# ==============================================================================

# ==============================================================================
# File: tests/__init__.py
# Description: Makes the directory a Python package.
# ==============================================================================


# ==============================================================================
# File: tests/test_redis_coordinator.py (NEW)
# Description: Unit tests for the RedisCoordinator class.
# ==============================================================================
import unittest
from unittest.mock import MagicMock, patch
from scheduler_app.utils.redis_coordinator import RedisCoordinator


class TestRedisCoordinator(unittest.TestCase):

    @patch('redis.Redis.from_url')
    def setUp(self, mock_redis_from_url):
        self.mock_redis = MagicMock()
        mock_redis_from_url.return_value = self.mock_redis
        self.coordinator = RedisCoordinator()

    def test_init_counter(self):
        """Test that init_counter sets the correct keys in Redis."""
        mock_pipeline = self.mock_redis.pipeline.return_value
        self.coordinator.init_counter(workflow_id="test-123", total_count=100, job_id="job-abc")

        # Check that pipeline was used
        self.mock_redis.pipeline.assert_called_once()

        # Check that the correct set calls were made on the pipeline
        mock_pipeline.set.assert_any_call("counter:test-123:completed", 0, ex=86400)
        mock_pipeline.set.assert_any_call("counter:test-123:total", 100, ex=86400)
        mock_pipeline.set.assert_any_call("workflow_info:test-123", "job-abc", ex=86400)
        mock_pipeline.execute.assert_called_once()

    def test_increment_in_progress(self):
        """Test that increment does not publish when not complete."""
        # Mock the Lua script to return 0 (in-progress)
        self.coordinator.finisher_script = MagicMock(return_value=0)

        self.coordinator.increment(workflow_id="test-456")

        # Assert the script was called correctly
        self.coordinator.finisher_script.assert_called_once_with(
            keys=['counter:test-456:completed', 'counter:test-456:total'],
            args=['workflow_completion_events', 'test-456']
        )

    def test_increment_on_completion(self):
        """Test that increment publishes on completion."""
        # Mock the Lua script to return 1 (completed)
        self.coordinator.finisher_script = MagicMock(return_value=1)

        self.coordinator.increment(workflow_id="test-789")

        # Assert the script was called correctly
        self.coordinator.finisher_script.assert_called_once_with(
            keys=['counter:test-789:completed', 'counter:test-789:total'],
            args=['workflow_completion_events', 'test-789']
        )


if __name__ == '__main__':
    unittest.main()

# ==============================================================================
# File: tests/test_api.py (NEW)
# Description: Unit tests for the FastAPI server endpoints.
# ==============================================================================
import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from scheduler_app.api.server import app


class TestApi(unittest.TestCase):

    def setUp(self):
        self.client = TestClient(app)

    @patch('scheduler_app.api.server.client')  # Mock the SchedulerClient
    def test_get_jobs_success(self, mock_scheduler_client):
        """Test the /api/jobs endpoint on success."""
        mock_jobs = [
            {"id": "job-1", "name": "Test Job 1"},
            {"id": "job-2", "name": "Test Job 2"}
        ]
        mock_scheduler_client.get_all_jobs.return_value = mock_jobs

        response = self.client.get("/api/jobs")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), mock_jobs)
        mock_scheduler_client.get_all_jobs.assert_called_once()

    @patch('scheduler_app.api.server.redis_coord')  # Mock the RedisCoordinator
    def test_get_progress_success(self, mock_redis_coord):
        """Test the /api/progress/{workflow_id} endpoint."""
        mock_progress = {"processed": 50, "expected": 100}
        mock_redis_coord.get_progress.return_value = mock_progress

        response = self.client.get("/api/progress/wf-123")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), mock_progress)
        mock_redis_coord.get_progress.assert_called_once_with("wf-123")


if __name__ == '__main__':
    unittest.main()
