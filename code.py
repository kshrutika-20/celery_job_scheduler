# This is a multi-file example for a SINGLE-POD deployment.
# The structure has been refactored for this purpose.
# 1. config.py
# 2. database.py
# 3. redis_coordinator.py
# 4. job_definitions.py
# 5. api_caller.py
# 6. custom_functions.py
# 7. conceptual_celery_worker.py (For Reference)
# 8. scheduler_core.py
# 9. scheduler_client.py
# 10. api_server.py
# 11. main.py
# 12. templates/index.html

# ==============================================================================
# File: config.py
# Description: Centralized configuration management using Pydantic.
# ==============================================================================
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """
    Manages all application settings. Loads from environment variables and .env file.
    """
    # MongoDB connection settings
    MONGO_URI: str | None = None
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_USER: str | None = None
    MONGO_PASS: str | None = None
    MONGO_DB: str = "medallion"
    MONGO_SCHEDULER_COLLECTION: str = "apscheduler"
    MONGO_STATUS_COLLECTION: str = "job_statuses"

    # Redis connection settings
    REDIS_URL: str = "redis://localhost:6379/0"

    # URL for your separate Producer service
    PRODUCER_API_URL: str = "http://localhost:8001"  # Example URL

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# ==============================================================================
# File: database.py
# Description: Handles all database connection logic.
# ==============================================================================
from pymongo import MongoClient
from config import settings


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
# File: redis_coordinator.py
# Description: Manages distributed counters and pub/sub events for task coordination.
# ==============================================================================
import redis
import logging
from config import settings


class RedisCoordinator:
    """
    Manages counters and publishes completion events using Redis.
    Uses a Lua script for atomic increment-and-check operations.
    """

    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.completion_channel = "workflow_completion_events"
        self._register_scripts()

    def _register_scripts(self):
        """Registers Lua scripts for atomic operations."""
        lua_script = """
        local completed_key = KEYS[1]
        local total_key = KEYS[2]
        local channel = ARGV[1]
        local workflow_id = ARGV[2]

        if redis.call('EXISTS', total_key) == 0 then
            return 0
        end

        local completed = redis.call('INCR', completed_key)
        local total = tonumber(redis.call('GET', total_key))

        if total > 0 and completed >= total then
            redis.call('PUBLISH', channel, workflow_id)
            redis.call('DEL', completed_key, total_key)
            return 1
        end
        return completed
        """
        self.finisher_script = self.redis.register_script(lua_script)

    def init_counter(self, workflow_id: str, total_count: int):
        """Initializes the counters for a new fan-out job."""
        logging.info(f"[{workflow_id}] Initializing Redis counter. Total items: {total_count}")
        pipe = self.redis.pipeline()
        pipe.set(f"counter:{workflow_id}:completed", 0)
        pipe.set(f"counter:{workflow_id}:total", total_count)
        pipe.execute()

    def increment(self, workflow_id: str):
        """Atomically increments a counter and publishes on completion."""
        completed_key = f"counter:{workflow_id}:completed"
        total_key = f"counter:{workflow_id}:total"
        try:
            result = self.finisher_script(keys=[completed_key, total_key], args=[self.completion_channel, workflow_id])
            if result == 1:
                logging.info(f"[{workflow_id}] Final sub-task completed. Published completion event.")
        except redis.exceptions.ResponseError as e:
            logging.error(f"Lua script error for workflow {workflow_id}: {e}")

    def get_progress(self, workflow_id: str) -> dict:
        """Gets the current progress of a fan-out job."""
        pipe = self.redis.pipeline()
        pipe.get(f"counter:{workflow_id}:completed")
        pipe.get(f"counter:{workflow_id}:total")
        results = pipe.execute()
        return {"processed": int(results[0] or 0), "expected": int(results[1] or 0)}


# ==============================================================================
# File: job_definitions.py
# Description: Defines the jobs to be scheduled.
# ==============================================================================
JOB_DEFINITIONS = [
    {"id": "start_repo_ingestion_workflow", "name": "Start Repo Ingestion Workflow", "trigger": "cron",
     "trigger_args": {"hour": 1}, "function": "start_repo_ingestion_workflow"},
    {"id": "fetch_labels", "name": "Fetch All Labels", "function": "fetch_labels"},
]

# ==============================================================================
# File: api_caller.py
# Description: Handles API requests for jobs. (No changes)
# ==============================================================================
import requests
import logging


def trigger_api_call(url: str, method: str, trace_id: str, headers: dict = None, json_data: dict = None) -> bool:
    logging.info(f"[{trace_id}] Triggering API call: {method} {url}")
    request_headers = headers.copy() if headers else {}
    request_headers['X-Trace-ID'] = trace_id
    try:
        response = requests.request(method=method.upper(), url=url, headers=request_headers, json=json_data, timeout=30)
        response.raise_for_status()
        logging.info(f"[{trace_id}] API call successful for {url}. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"[{trace_id}] API call failed for {url}. Error: {e}")
        return False


# ==============================================================================
# File: custom_functions.py
# Description: The functions for the scheduler's jobs.
# ==============================================================================
import logging
from api_caller import trigger_api_call
from config import settings


def fetch_all_repos_from_mongo(workflow_id: str):
    logging.info(f"[{workflow_id}] Gathering all repo data from MongoDB to start label fetching.")
    return []


def start_repo_ingestion_workflow(trace_id: str):
    """
    Calls the Producer API to kick off the distributed workflow, passing the trace_id.
    """
    logging.info(f"[{trace_id}] Calling Producer to start repo ingestion workflow.")
    producer_url = f"{settings.PRODUCER_API_URL}/produce/fetch-repos"
    return trigger_api_call(
        url=producer_url, method="POST", trace_id=trace_id,
        json_data={"workflow_id": trace_id}
    )


def fetch_labels(trace_id: str):
    """
    Triggered by the Pub/Sub listener. Gathers results and starts the next workflow.
    """
    logging.info(f"[{trace_id}] All repos processed. Now starting fetch_labels workflow.")
    repos_data = fetch_all_repos_from_mongo(workflow_id=trace_id)
    producer_url = f"{settings.PRODUCER_API_URL}/produce/fetch-labels"
    logging.info(f"[{trace_id}] Calling producer to start label ingestion for {len(repos_data)} repos.")
    return trigger_api_call(
        url=producer_url, method="POST", trace_id=trace_id,
        json_data={"repos": repos_data, "workflow_id": trace_id}
    )


JOB_FUNCTIONS = {
    'start_repo_ingestion_workflow': start_repo_ingestion_workflow,
    'fetch_labels': fetch_labels,
}

# ==============================================================================
# File: conceptual_celery_worker.py (For Reference)
# Description: Shows how the Celery worker and signal handler would be implemented.
# ==============================================================================
# from celery import Celery
# from celery.signals import task_success
# from redis_coordinator import RedisCoordinator
# from config import settings

# celery_app = Celery('tasks', broker=settings.REDIS_URL)

# @celery_app.task(bind=True)
# def process_single_repo(self, repo_url: str, workflow_id: str):
#     """
#     The core task that does the work for one repository.
#     The `workflow_id` is passed in its arguments.
#     """
#     logging.info(f"[{workflow_id}] Worker processing repo: {repo_url}")
#     # ... your actual logic to fetch data from Bitbucket and save it to MongoDB ...
#     return {"status": "success", "repo": repo_url}

# @task_success.connect(sender=process_single_repo)
# def on_repo_process_success(sender=None, **kwargs):
#     """
#     This signal handler runs automatically after any `process_single_repo`
#     task completes successfully. It increments the Redis counter.
#     """
#     # The task's original arguments are in the sender's request context.
#     task_kwargs = sender.request.kwargs
#     workflow_id = task_kwargs.get('workflow_id')

#     if not workflow_id:
#         logging.error("Could not find workflow_id in successful task signal's kwargs.")
#         return

#     logging.info(f"Success signal received for task in workflow [{workflow_id}]")
#     redis_coord = RedisCoordinator()
#     redis_coord.increment(workflow_id=workflow_id)


# ==============================================================================
# File: scheduler_core.py
# Description: The core logic for the scheduler application. (No changes)
# ==============================================================================
import logging, uuid
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from job_definitions import JOB_DEFINITIONS
from custom_functions import JOB_FUNCTIONS
from database import get_mongo_client
from config import settings


def task_wrapper(job_id: str, *args):
    trace_id = str(uuid.uuid4())
    if args: trace_id = args[0]

    mongo_client = None
    try:
        mongo_client = get_mongo_client()
        status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
        logging.info(f"[{trace_id}] Task wrapper executing for job_id: {job_id}")
        run_record = {"trace_id": trace_id, "status": "Running", "timestamp_utc": datetime.now(timezone.utc)}
        status_collection.update_one({"job_id": job_id}, {"$push": {"history": {"$each": [run_record], "$slice": -50}},
                                                          "$set": {"job_id": job_id}}, upsert=True)
        job_id_to_function = {job["id"]: job["function"] for job in JOB_DEFINITIONS if "function" in job}
        func_name = job_id_to_function.get(job_id)
        if not func_name: raise ValueError(f"No function name defined for job_id: {job_id}")
        func = JOB_FUNCTIONS.get(func_name)
        if not func: raise ValueError(f"No function found for name: {func_name}")
        if not func(trace_id=trace_id): raise Exception("Custom function reported a failure.")
        status_collection.update_one({"job_id": job_id, "history.trace_id": trace_id}, {
            "$set": {"history.$.status": "Success", "history.$.end_timestamp_utc": datetime.now(timezone.utc)}})
    except Exception as e:
        logging.error(f"[{trace_id}] Execution failed for job_id: {job_id}. Error: {e}")
        if mongo_client:
            status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
            status_collection.update_one({"job_id": job_id, "history.trace_id": trace_id}, {
                "$set": {"history.$.status": "Failure", "history.$.end_timestamp_utc": datetime.now(timezone.utc),
                         "history.$.exception": str(e)}})
    finally:
        if mongo_client: mongo_client.close()


class SchedulerManager:
    def __init__(self):
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
        if event.exception:
            logging.error(f"APScheduler-level error for job {event.job_id}: {event.exception}")
        else:
            logging.info(f"APScheduler successfully triggered job {event.job_id}.")

    def schedule_all_jobs(self):
        for job_id, job_def in self.job_definitions.items():
            if 'trigger' in job_def:
                job_kwargs = {'id': job_id, 'name': job_def.get('name', job_id), 'func': 'scheduler_core:task_wrapper',
                              'args': [job_id], 'trigger': job_def['trigger'],
                              'executor': job_def.get('executor', 'default'),
                              'max_instances': job_def.get('max_instances', 1), 'replace_existing': True,
                              **job_def.get('trigger_args', {})}
                self.scheduler.add_job(**job_kwargs)

    def start(self):
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()


# ==============================================================================
# File: scheduler_client.py (MODIFIED)
# Description: A client to interact with the MongoDB job store for READ-ONLY operations.
# ==============================================================================
from datetime import datetime
import pendulum
from apscheduler.jobstores.mongodb import MongoDBJobStore
from database import get_mongo_client
from config import settings


class SchedulerClient:
    def __init__(self):
        self.mongo_client = get_mongo_client()
        self.jobstore = MongoDBJobStore(database=settings.MONGO_DB, collection=settings.MONGO_SCHEDULER_COLLECTION,
                                        client=self.mongo_client)
        self.status_collection = self.mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]

    def get_all_jobs(self):
        jobs = self.jobstore.get_all_jobs()
        statuses = {s['job_id']: s for s in self.status_collection.find()}
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
                "trigger": str(job.trigger), "last_status": last_status,
                "last_run_utc": last_run.get("timestamp_utc", "N/A"),
                "ui_status": ui_status,
                "last_workflow_id": last_run.get("trace_id")  # Expose the last workflow ID
            })
        return job_list


# ==============================================================================
# File: api_server.py (MODIFIED)
# Description: The public-facing FastAPI server for the combined pod.
# ==============================================================================
import logging
import uuid
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from scheduler_client import SchedulerClient
from redis_coordinator import RedisCoordinator

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
    try:
        jobs = client.get_all_jobs()
        return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})
    except Exception as e:
        logging.error(f"Failed to load jobs for UI: {e}")
        return templates.TemplateResponse("index.html", {"request": request, "jobs": [], "error": str(e)})


@app.get("/api/jobs")
async def get_jobs(): return client.get_all_jobs()


@app.get("/api/progress/{workflow_id}")
async def get_job_progress(workflow_id: str):
    """Correctly uses the unique workflow_id to get progress."""
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
    request.app.state.scheduler.add_job('scheduler_core:task_wrapper', trigger='date', args=[job_id], id=run_id,
                                        name=f"{job_id} (Manual Run)", replace_existing=False)
    return {"status": "success", "message": f"Job '{job_id}' triggered for immediate execution."}


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(request: Request, job_id: str):
    _job_or_404(request, job_id)
    request.app.state.scheduler.remove_job(job_id)
    return {"status": "success", "message": f"Job '{job_id}' deleted."}


# ==============================================================================
# File: main.py
# Description: The single entry point for the combined pod. (No changes)
# ==============================================================================
import logging
import uvicorn
import threading
import redis
from contextlib import asynccontextmanager
from scheduler_core import SchedulerManager
from api_server import app as fastapi_app
from config import settings
from redis_coordinator import RedisCoordinator

scheduler_manager = SchedulerManager()
redis_client = redis.Redis.from_url(settings.REDIS_URL)


def pubsub_listener_loop():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(RedisCoordinator().completion_channel)
    logging.info("Pub/Sub listener started, subscribed to '{}'".format(RedisCoordinator().completion_channel))
    for message in pubsub.listen():
        if message["type"] == "message":
            completed_workflow_id = message["data"].decode()
            logging.info(f"Received completion event for workflow: {completed_workflow_id}")
            try:
                logging.info(f"Triggering dependent job 'fetch_labels' for completed workflow {completed_workflow_id}")
                run_id = f"fetch_labels_for_{completed_workflow_id}"
                scheduler_manager.scheduler.add_job(
                    'scheduler_core:task_wrapper', trigger='date', args=['fetch_labels', completed_workflow_id],
                    id=run_id, name=f"Fetch Labels (after {completed_workflow_id})", replace_existing=False
                )
            except Exception as e:
                logging.error(f"Error while triggering dependent job 'fetch_labels': {e}")


@asynccontextmanager
async def lifespan(app):
    app.state.scheduler = scheduler_manager.scheduler
    scheduler_manager.schedule_all_jobs()
    scheduler_manager.start()
    pubsub_thread = threading.Thread(target=pubsub_listener_loop, daemon=True)
    pubsub_thread.start()
    yield
    logging.info("Shutting down scheduler...")
    scheduler_manager.shutdown()


fastapi_app.router.lifespan_context = lifespan

if __name__ == "__main__":
    print("Starting combined Scheduler and API server...")
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)

