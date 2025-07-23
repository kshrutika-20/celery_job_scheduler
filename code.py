# ==============================================================================
# File: requirements.txt
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
# pytest
# pytest-mock


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
# File: scheduler_app/utils/config.py
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
    TIMEZONE: str = "Asia/Kolkata"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# ==============================================================================
# File: scheduler_app/utils/logger.py
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
        if hasattr(record, 'context'):
            log_record.update(record.context)
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_record)


def get_logger(name: str, **context):
    """
    Returns a logger instance with a JSON formatter and optional context.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(settings.LOG_LEVEL)
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return logging.LoggerAdapter(logger, {'context': context})


logging.basicConfig(level=settings.LOG_LEVEL, format='%(levelname)s: %(message)s')

# ==============================================================================
# File: scheduler_app/utils/database.py
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


class RedisCoordinator:
    """
    Manages counters and publishes completion events using Redis.
    """

    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.completion_channel = "workflow_completion_events"
        self._register_scripts()

    def _register_scripts(self):
        """Registers Lua scripts for atomic operations."""
        lua_script = """
        local key_to_incr = KEYS[1]
        local succeeded_key = KEYS[2]
        local failed_key = KEYS[3]
        local total_key = KEYS[4]
        local channel = ARGV[1]
        local workflow_id = ARGV[2]
        if redis.call('EXISTS', total_key) == 0 then return 0 end
        redis.call('INCR', key_to_incr)
        local succeeded = tonumber(redis.call('GET', succeeded_key))
        local failed = tonumber(redis.call('GET', failed_key))
        local total = tonumber(redis.call('GET', total_key))
        if total > 0 and (succeeded + failed) >= total then
            if redis.call('PUBLISH', channel, workflow_id) > 0 then return 1 end
        end
        return 0
        """
        self.finisher_script = self.redis.register_script(lua_script)

    def add_to_total(self, workflow_id: str, count_to_add: int, job_id: str):
        """
        Atomically increments the total expected count for a workflow.
        This is the new core method for the Producer.
        """
        logger = get_logger(__name__, workflow_id=workflow_id, job_id=job_id)
        logger.info(f"Adding {count_to_add} to total count.")

        pipe = self.redis.pipeline()
        total_key = f"counter:{workflow_id}:total"

        # Atomically add to the total
        pipe.incrby(total_key, count_to_add)

        # Set expiry only if the key is new (i.e., this is the first batch)
        pipe.expire(total_key, settings.REDIS_COUNTER_TTL_SECONDS, nx=True)
        pipe.expire(f"counter:{workflow_id}:succeeded", settings.REDIS_COUNTER_TTL_SECONDS, nx=True)
        pipe.expire(f"counter:{workflow_id}:failed", settings.REDIS_COUNTER_TTL_SECONDS, nx=True)
        pipe.expire(f"workflow_info:{workflow_id}", settings.REDIS_COUNTER_TTL_SECONDS, nx=True)

        # Ensure the other keys exist if this is the first call
        pipe.setnx(f"counter:{workflow_id}:succeeded", 0)
        pipe.setnx(f"counter:{workflow_id}:failed", 0)
        pipe.setnx(f"workflow_info:{workflow_id}", job_id)

        pipe.execute()

    def _increment(self, workflow_id: str, counter_type: str):
        """Internal method to call the atomic Lua script for workers."""
        key_to_incr = f"counter:{workflow_id}:{counter_type}"
        succeeded_key = f"counter:{workflow_id}:succeeded"
        failed_key = f"counter:{workflow_id}:failed"
        total_key = f"counter:{workflow_id}:total"
        try:
            result = self.finisher_script(
                keys=[key_to_incr, succeeded_key, failed_key, total_key],
                args=[self.completion_channel, workflow_id]
            )
            if result == 1:
                get_logger(__name__, workflow_id=workflow_id).info(
                    "Final sub-task completed. Published completion event.")
        except redis.exceptions.ResponseError as e:
            get_logger(__name__, workflow_id=workflow_id).error("Lua script error.", error=str(e))

    def increment_success(self, workflow_id: str):
        self._increment(workflow_id, "succeeded")

    def increment_failure(self, workflow_id: str):
        self._increment(workflow_id, "failed")

    def get_progress(self, workflow_id: str) -> dict:
        pipe = self.redis.pipeline()
        pipe.get(f"counter:{workflow_id}:succeeded")
        pipe.get(f"counter:{workflow_id}:failed")
        pipe.get(f"counter:{workflow_id}:total")
        succeeded, failed, total = pipe.execute()
        return {"succeeded": int(succeeded or 0), "failed": int(failed or 0), "total": int(total or 0)}

    def counter_exists(self, workflow_id: str) -> bool:
        """Checks if a counter is still active for a given workflow."""
        return bool(self.redis.exists(f"counter:{workflow_id}:total"))


# ==============================================================================
# File: scheduler_app/jobs/definitions.py
# Description: Defines the jobs and their dependencies.
# ==============================================================================
JOB_DEFINITIONS = [
    {
        "id": "fetch_projects",
        "name": "Fetch All Projects",
        "trigger": "cron",
        "trigger_args": {"hour": 6, "timezone": "Asia/Kolkata"},
        "function": "fetch_projects",
        "is_workflow_starter": True
    },
    {
        "id": "fetch_project_permissions",
        "name": "Fetch Project Permissions",
        "function": "fetch_project_permissions",
        "triggers_on_completion_of": ["fetch_projects"],
        "is_workflow_starter": True
    },
    {
        "id": "fetch_repo",
        "name": "Fetch All Repositories",
        "function": "fetch_repo",
        "triggers_on_completion_of": ["fetch_projects"],
        "is_workflow_starter": True
    },
    {
        "id": "fetch_labels",
        "name": "Fetch All Labels",
        "function": "fetch_labels",
        "triggers_on_completion_of": ["fetch_repo"],
        "is_workflow_starter": True
    },
    {
        "id": "fetch_repo_permissions",
        "name": "Fetch Repository Permissions",
        "function": "fetch_repo_permissions",
        "triggers_on_start_of": {
            "job_id": "fetch_labels",
            "delay_minutes": 30
        },
        "is_workflow_starter": True
    },
]

# ==============================================================================
# File: scheduler_app/jobs/api_caller.py
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
        logger.info("API call successful.", url=url, status_code=response.status_code)
        return True
    except requests.exceptions.RequestException as e:
        logger.error("API call failed.", url=url, error=str(e))
        return False


# ==============================================================================
# File: scheduler_app/jobs/functions.py
# Description: The functions for the scheduler's jobs.
# ==============================================================================
from scheduler_app.jobs.api_caller import trigger_api_call
from scheduler_app.utils.config import settings
from scheduler_app.utils.logger import get_logger


def generic_producer_call(trace_id: str, job_id: str, producer_endpoint: str):
    """A generic function to call a producer endpoint."""
    logger = get_logger(__name__, trace_id=trace_id, job_id=job_id)
    logger.info(f"Calling Producer at '{producer_endpoint}'.")
    producer_url = f"{settings.PRODUCER_API_URL}/{producer_endpoint}"
    return trigger_api_call(
        url=producer_url, method="POST", trace_id=trace_id,
        json_data={"workflow_id": trace_id, "job_id": job_id}
    )


def fetch_projects(trace_id: str, job_id: str):
    return generic_producer_call(trace_id, job_id, "produce/fetch-projects")


def fetch_project_permissions(trace_id: str, job_id: str):
    return generic_producer_call(trace_id, job_id, "produce/fetch-project-permissions")


def fetch_repo(trace_id: str, job_id: str):
    return generic_producer_call(trace_id, job_id, "produce/fetch-repos")


def fetch_labels(trace_id: str, job_id: str):
    return generic_producer_call(trace_id, job_id, "produce/fetch-labels")


def fetch_repo_permissions(trace_id: str, job_id: str):
    return generic_producer_call(trace_id, job_id, "produce/fetch-repo-permissions")


JOB_FUNCTIONS = {
    'fetch_projects': fetch_projects,
    'fetch_project_permissions': fetch_project_permissions,
    'fetch_repo': fetch_repo,
    'fetch_labels': fetch_labels,
    'fetch_repo_permissions': fetch_repo_permissions,
}

# ==============================================================================
# File: conceptual_producer.py (NEW - FOR REFERENCE)
# Description: Shows how your separate Producer service should be implemented.
# ==============================================================================
# from fastapi import FastAPI
# from pydantic import BaseModel
# from scheduler_app.utils.redis_coordinator import RedisCoordinator
# from scheduler_app.utils.config import settings
# from scheduler_app.utils.logger import get_logger

# app = FastAPI()
# redis_coord = RedisCoordinator(redis_url=settings.REDIS_URL)

# class WorkflowRequest(BaseModel):
#     workflow_id: str
#     job_id: str

# @app.post("/produce/fetch-repos")
# def produce_fetch_repos_tasks(request: WorkflowRequest):
#     """
#     This endpoint lives in your separate Producer repo.
#     It's called by the scheduler.
#     """
#     logger = get_logger(__name__, workflow_id=request.workflow_id, job_id=request.job_id)
#     logger.info("Received request to produce fetch-repo tasks.")

#     # 1. Get the list of items to process from the source (e.g., Bitbucket)
#     # This might involve multiple paginated API calls.
#     repo_urls = ["http://repo1.com", "http://repo2.com", "http://repo3.com"] # MOCK
#     num_tasks = len(repo_urls)

#     # 2. Atomically add the number of new tasks to the workflow's total count.
#     # This is the crucial step that solves the race condition.
#     redis_coord.add_to_total(
#         workflow_id=request.workflow_id,
#         count_to_add=num_tasks,
#         job_id=request.job_id
#     )

#     # 3. Enqueue the individual tasks for the Celery workers.
#     for url in repo_urls:
#         # Your logic to push to a Redis list that Celery monitors.
#         # The message MUST contain the workflow_id.
#         # enqueue_celery_task("process_single_repo", repo_url=url, workflow_id=request.workflow_id)
#         pass

#     logger.info(f"Enqueued {num_tasks} tasks for workflow.")
#     return {"status": "ok", "tasks_enqueued": num_tasks}


# ==============================================================================
# File: conceptual_celery_worker.py (For Reference)
# ==============================================================================
# from celery import Celery
# from celery.signals import task_success, task_failure
# from scheduler_app.utils.redis_coordinator import RedisCoordinator
# from scheduler_app.utils.config import settings
# from scheduler_app.utils.logger import get_logger

# celery_app = Celery('tasks', broker=settings.REDIS_URL)

# @celery_app.task(bind=True)
# def process_single_item(self, item_url: str, workflow_id: str):
#     logger = get_logger(__name__, workflow_id=workflow_id, item_url=item_url)
#     logger.info("Worker processing item.")
#     # ... your actual logic ...
#     return {"status": "success", "item": item_url}

# @task_success.connect(sender=process_single_item)
# def on_item_process_success(sender=None, **kwargs):
#     workflow_id = sender.request.kwargs.get('workflow_id')
#     if not workflow_id: return
#     RedisCoordinator().increment_success(workflow_id=workflow_id)

# @task_failure.connect(sender=process_single_item)
# def on_item_process_failure(sender=None, **kwargs):
#     workflow_id = sender.request.kwargs.get('workflow_id')
#     if not workflow_id: return
#     RedisCoordinator().increment_failure(workflow_id=workflow_id)


# ==============================================================================
# File: scheduler_app/core/scheduler.py
# Description: The core logic for the scheduler application.
# ==============================================================================
import uuid
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from scheduler_app.jobs.definitions import JOB_DEFINITIONS
from scheduler_app.jobs.functions import JOB_FUNCTIONS
from scheduler_app.utils.database import get_mongo_client
from scheduler_app.utils.config import settings
from scheduler_app.utils.logger import get_logger

# --- Singleton Pattern for SchedulerManager ---
_scheduler_manager_instance = None


def get_scheduler_manager():
    """Returns the singleton instance of the SchedulerManager."""
    global _scheduler_manager_instance
    if _scheduler_manager_instance is None:
        _scheduler_manager_instance = SchedulerManager()
    return _scheduler_manager_instance


def task_wrapper(job_id: str, parent_trace_id: str | None = None):
    """A robust wrapper that manages the full lifecycle of a job run."""
    trace_id = str(uuid.uuid4())
    logger = get_logger(__name__, job_id=job_id, trace_id=trace_id, parent_trace_id=parent_trace_id)

    mongo_client = None
    try:
        scheduler_manager = get_scheduler_manager()
        scheduler_manager.schedule_on_start_dependents(job_id, trace_id)

        mongo_client = get_mongo_client()
        status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
        logger.info("Task wrapper executing.")
        run_record = {"trace_id": trace_id, "status": "Running",
                      "timestamp_utc": datetime.now(timezone.utc).isoformat(), "parent_trace_id": parent_trace_id}
        status_collection.update_one({"job_id": job_id}, {"$push": {"history": {"$each": [run_record], "$slice": -50}},
                                                          "$set": {"job_id": job_id}}, upsert=True)

        job_id_to_function = {job["id"]: job["function"] for job in JOB_DEFINITIONS}
        func_name = job_id_to_function.get(job_id)
        if not func_name: raise ValueError(f"No function name defined for job_id: {job_id}")

        func = JOB_FUNCTIONS.get(func_name)
        if not func: raise ValueError(f"No function found for name: {func_name}")

        if not func(trace_id=trace_id, job_id=job_id): raise Exception("Custom function reported a failure.")

        job_def = next((item for item in JOB_DEFINITIONS if item["id"] == job_id), None)
        if not job_def.get("is_workflow_starter"):
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
                                        timezone=settings.TIMEZONE)
        scheduler.add_listener(self.job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        return scheduler

    def job_event_listener(self, event):
        logger = get_logger(__name__, job_id=event.job_id)
        if event.exception:
            logger.error("APScheduler-level error.", error=str(event.exception))
        else:
            logger.info("APScheduler successfully triggered job.")

    def schedule_all_jobs(self):
        for job_id, job_def in self.job_definitions.items():
            if 'trigger' in job_def:
                job_kwargs = {'id': job_id, 'name': job_def.get('name', job_id),
                              'func': 'scheduler_app.core.scheduler:task_wrapper', 'args': [job_id],
                              'trigger': job_def['trigger'], 'executor': job_def.get('executor', 'default'),
                              'max_instances': job_def.get('max_instances', 1), 'replace_existing': True,
                              **job_def.get('trigger_args', {})}
                self.scheduler.add_job(**job_kwargs)
                self.logger.info("Scheduled job.", job_id=job_id)

    def schedule_on_start_dependents(self, parent_job_id: str, parent_trace_id: str):
        """
        Schedules dependent jobs that are configured to run with a delay after the parent job starts.
        """
        for job_def in self.job_definitions.values():
            dependency_info = job_def.get("triggers_on_start_of")
            if dependency_info and dependency_info.get("job_id") == parent_job_id:
                next_job_id = job_def["id"]
                delay_minutes = dependency_info.get("delay_minutes", 0)
                run_time = datetime.now(self.scheduler.timezone) + timedelta(minutes=delay_minutes)
                run_id = f"{next_job_id}_for_{parent_trace_id}"

                logger = get_logger(__name__, parent_job_id=parent_job_id, next_job_id=next_job_id,
                                    delay_minutes=delay_minutes)
                logger.info("Scheduling a delayed dependent job.")

                try:
                    self.scheduler.add_job(
                        'scheduler_app.core.scheduler:task_wrapper', trigger='date', run_date=run_time,
                        args=[next_job_id, parent_trace_id],
                        id=run_id, name=f"{job_def['name']} (after {parent_job_id})",
                        replace_existing=False
                    )
                except Exception as e:
                    logger.error("Failed to schedule delayed dependent job.", error=str(e))

    def start(self):
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()


# ==============================================================================
# File: scheduler_app/api/client.py
# Description: A client to interact with the MongoDB job store for READ-ONLY operations.
# ==============================================================================
import pendulum
from apscheduler.jobstores.mongodb import MongoDBJobStore
from scheduler_app.utils.database import get_mongo_client
from scheduler_app.utils.config import settings
from scheduler_app.jobs.definitions import JOB_DEFINITIONS
from scheduler_app.utils.redis_coordinator import RedisCoordinator


class SchedulerClient:
    def __init__(self):
        self.mongo_client = get_mongo_client()
        self.redis_coord = RedisCoordinator()
        self.jobstore = MongoDBJobStore(database=settings.MONGO_DB, collection=settings.MONGO_SCHEDULER_COLLECTION,
                                        client=self.mongo_client)
        self.status_collection = self.mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]

    def get_all_jobs(self):
        live_jobs = {job.id: job for job in self.jobstore.get_all_jobs()}
        statuses = {s['job_id']: s for s in self.status_collection.find()}

        all_jobs_view = []
        for job_def in JOB_DEFINITIONS:
            job_id = job_def["id"]
            status_info = statuses.get(job_id, {})
            history = status_info.get("history", [])
            last_run = history[-1] if history else {}
            last_status_from_mongo = last_run.get("status", "Unknown")
            last_workflow_id = last_run.get("trace_id")

            live_job_instance = live_jobs.get(job_id)

            last_completed_progress = {}
            for run in reversed(history):
                if "progress" in run:
                    last_completed_progress = run["progress"]
                    break

            # Determine the most accurate UI status
            if live_job_instance and live_job_instance.next_run_time is None:
                ui_status = "Paused"
            elif last_status_from_mongo == "Running" and job_def.get(
                    "is_workflow_starter") and self.redis_coord.counter_exists(last_workflow_id):
                ui_status = "Running"
            elif last_status_from_mongo == "Complete w/ Errors":
                ui_status = "Complete w/ Errors"
            elif last_status_from_mongo == "Success":
                ui_status = "Success"
            elif last_status_from_mongo == "Failure":
                ui_status = "Failing"
            elif live_job_instance:
                ui_status = "Scheduled"
            else:
                ui_status = "Dependent"

            view_model = {
                "id": job_id,
                "name": job_def.get("name", job_id),
                "last_status": last_status_from_mongo,
                "ui_status": ui_status,
                "last_workflow_id": last_workflow_id,
                "is_workflow_starter": job_def.get("is_workflow_starter", False),
                "last_progress": last_completed_progress,
                "trigger": str(live_job_instance.trigger) if live_job_instance else "Dependency",
                "next_run_time_utc": live_job_instance.next_run_time.isoformat() if live_job_instance and live_job_instance.next_run_time else "N/A",
                "next_run_time_human": pendulum.instance(live_job_instance.next_run_time).in_timezone(
                    settings.TIMEZONE).diff_for_humans() if live_job_instance and live_job_instance.next_run_time else "N/A",
                "next_run_time_local": pendulum.instance(live_job_instance.next_run_time).in_timezone(
                    settings.TIMEZONE).to_datetime_string() if live_job_instance and live_job_instance.next_run_time else "N/A",
            }
            all_jobs_view.append(view_model)

        return all_jobs_view


# ==============================================================================
# File: scheduler_app/api/server.py
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
# File: main.py
# Description: The single entry point for the combined pod.
# ==============================================================================
import uvicorn
import threading
import redis
from contextlib import asynccontextmanager
from datetime import datetime
from scheduler_app.core.scheduler import get_scheduler_manager
from scheduler_app.api.server import app as fastapi_app
from scheduler_app.utils.config import settings
from scheduler_app.utils.redis_coordinator import RedisCoordinator
from scheduler_app.jobs.definitions import JOB_DEFINITIONS
from scheduler_app.utils.logger import get_logger
from scheduler_app.utils.database import get_mongo_client

logger = get_logger(__name__)
redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)


def pubsub_listener_loop():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(RedisCoordinator().completion_channel)
    logger.info("Pub/Sub listener started.", channel=RedisCoordinator().completion_channel)

    for message in pubsub.listen():
        if message["type"] == "message":
            completed_workflow_id = message["data"]
            logger.info("Received completion event.", workflow_id=completed_workflow_id)

            # --- Persist Final Progress to MongoDB ---
            redis_coord = RedisCoordinator()
            progress_data = redis_coord.get_progress(completed_workflow_id)

            final_status = "Success"
            if progress_data["failed"] > 0:
                final_status = "Complete w/ Errors"

            mongo_client = get_mongo_client()
            status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
            status_collection.update_one(
                {"history.trace_id": completed_workflow_id},
                {"$set": {
                    "history.$.progress": progress_data,
                    "history.$.status": final_status,
                    "history.$.end_timestamp_utc": datetime.now(timezone.utc).isoformat()
                }}
            )
            mongo_client.close()
            # --- End Persistence Logic ---

            workflow_info_key = f"workflow_info:{completed_workflow_id}"
            completed_job_id = redis_client.get(workflow_info_key)

            if not completed_job_id:
                logger.warning("Could not find original job for completed workflow.", workflow_id=completed_workflow_id)
                continue

            for job_def in JOB_DEFINITIONS:
                # A job can be triggered by the completion of ANY of its listed dependencies
                if completed_job_id in job_def.get("triggers_on_completion_of",
                                                   []) and "triggers_on_start_of" not in job_def:
                    next_job_id = job_def["id"]
                    logger.info("Triggering dependent job on completion.", next_job_id=next_job_id,
                                completed_job_id=completed_job_id)
                    try:
                        run_id = f"{next_job_id}_for_{completed_workflow_id}"
                        scheduler_manager = get_scheduler_manager()
                        scheduler_manager.scheduler.add_job(
                            'scheduler_app.core.scheduler:task_wrapper', trigger='date',
                            args=[next_job_id, completed_workflow_id],  # Pass parent_trace_id
                            id=run_id, name=f"{job_def['name']} (after {completed_job_id})",
                            replace_existing=False
                        )
                    except Exception as e:
                        logger.error("Error while triggering dependent job.", next_job_id=next_job_id, error=str(e))

            redis_client.delete(workflow_info_key)


@asynccontextmanager
async def lifespan(app):
    scheduler_manager = get_scheduler_manager()
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

    