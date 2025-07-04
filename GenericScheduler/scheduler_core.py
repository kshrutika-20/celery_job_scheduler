import os
import logging
import uuid
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from job_definitions import JOB_DEFINITIONS
from custom_functions import JOB_FUNCTIONS
from pymongo import MongoClient

load_dotenv(dotenv_path='.env')


# This function must be at the top level to be pickleable by APScheduler.
def task_wrapper(job_id: str):
    """
    A robust wrapper that manages the full lifecycle of a job run,
    including status tracking and traceability.
    It retrieves job details and calls the mapped function.
    """
    trace_id = str(uuid.uuid4())
    mongo_client = None

    # Establish a new MongoDB client for this job run to ensure process/thread safety.
    try:
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", 27017))
        db_name = os.getenv("MONGO_DB", "apscheduler_db")

        if os.getenv("MONGO_USER") and os.getenv("MONGO_PASS"):
            uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASS')}@{host}:{port}/{db_name}?authSource=admin"
            mongo_client = MongoClient(uri)
        else:
            mongo_client = MongoClient(host, port)

        status_collection = mongo_client[db_name].job_statuses

        # 1. Push a new "Running" status to the history array.
        # This also creates the document if it doesn't exist.
        # We use $slice to keep the history array bounded to the last 50 runs.
        logging.info(f"[{trace_id}] Task wrapper executing for job_id: {job_id}")
        run_record = {
            "trace_id": trace_id,
            "status": "Running",
            "timestamp_utc": datetime.now(timezone.utc),
            "exception": None
        }
        status_collection.update_one(
            {"job_id": job_id},
            {
                "$push": {
                    "history": {
                        "$each": [run_record],
                        "$slice": -50
                    }
                },
                "$set": {"job_id": job_id}  # Ensure job_id is set on upsert
            },
            upsert=True
        )

        # 2. Execute the actual job function
        job_id_to_function = {job["id"]: job["function"] for job in JOB_DEFINITIONS if "function" in job}
        func_name = job_id_to_function.get(job_id)
        if not func_name:
            raise ValueError(f"No function name defined for job_id: {job_id}")

        func = JOB_FUNCTIONS.get(func_name)
        if not func:
            raise ValueError(f"No function found in JOB_FUNCTIONS for name: {func_name}")

        result = func(trace_id=trace_id)
        if not result:
            raise Exception("The triggered API call or custom function reported a failure.")

        # 3. If successful, update the status of the current run to "Success"
        status_collection.update_one(
            {"job_id": job_id, "history.trace_id": trace_id},
            {"$set": {
                "history.$.status": "Success",
                "history.$.end_timestamp_utc": datetime.now(timezone.utc)
            }}
        )

    except Exception as e:
        logging.error(f"[{trace_id}] Execution failed for job_id: {job_id}. Error: {e}")
        # 4. If any exception occurs, update the status to "Failure"
        if mongo_client:
            status_collection = mongo_client[os.getenv("MONGO_DB", "apscheduler_db")].job_statuses
            status_collection.update_one(
                {"job_id": job_id, "history.trace_id": trace_id},
                {"$set": {
                    "history.$.status": "Failure",
                    "history.$.end_timestamp_utc": datetime.now(timezone.utc),
                    "history.$.exception": str(e)
                }}
            )
    finally:
        # 5. Always close the client connection
        if mongo_client:
            mongo_client.close()


class SchedulerManager:
    """Manages the lifecycle of the APScheduler instance."""

    def __init__(self):
        """
            Initializes the SchedulerManager.
            """
        self.job_definitions = self._load_job_definitions()
        self.mongo_client = self._get_mongo_client()
        self.scheduler = self._initialize_scheduler()

    def _get_mongo_client(self):
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", 27017))
        db_name = os.getenv("MONGO_DB", "apscheduler_db")
        if os.getenv("MONGO_USER") and os.getenv("MONGO_PASS"):
            uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASS')}@{host}:{port}/{db_name}?authSource=admin"
            return MongoClient(uri)
        return MongoClient(host, port)

    def _load_job_definitions(self) -> dict:
        """Loads job definitions from the imported JOBS list."""
        return {job['id']: job for job in JOB_DEFINITIONS}

    def _initialize_scheduler(self) -> BackgroundScheduler:
        """Configures and initializes the BackgroundScheduler."""
        db_name = os.getenv("MONGO_DB", "apscheduler_db")
        collection_name = os.getenv("MONGO_COLLECTION", "jobs")
        jobstores = {'default': MongoDBJobStore(database=db_name, collection=collection_name, client=self.mongo_client)}
        executors = {'default': ThreadPoolExecutor(20), 'processpool': ProcessPoolExecutor(5)}
        job_defaults = {'coalesce': False, 'max_instances': 1}
        scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                        timezone='UTC')
        scheduler.add_listener(self.job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        return scheduler

    def job_event_listener(self, event):
        """Listens for job execution events to handle logging and dependencies."""
        if event.exception:
            logging.error(
                f"APScheduler-level error for job {event.job_id}. This might be a serialization or configuration issue. Error: {event.exception}")
        else:
            logging.info(f"APScheduler successfully triggered job {event.job_id}.")
            self._trigger_dependent_jobs(event.job_id)

    def _trigger_dependent_jobs(self, parent_job_id: str):
        """
            Finds and schedules jobs that depend on the successfully completed parent job.
            """
        for job_id, job_def in self.job_definitions.items():
            if parent_job_id in job_def.get('dependencies', []):
                self.scheduler.modify_job(
                    job_id, next_run_time=datetime.now(timezone.utc) + timedelta(seconds=1)
                )

    def schedule_all_jobs(self):
        """
            Reads all job definitions and adds/updates them in the scheduler.
            Expects job format to have 'trigger' as a string and 'trigger_args' as a dict.
            """
        for job_id, job_def in self.job_definitions.items():
            job_kwargs = {
                'id': job_id,
                'name': job_def.get('name', job_id),
                'func': 'scheduler_core:task_wrapper',
                'args': [job_id],
                'trigger': job_def['trigger'],
                'executor': job_def.get('executor', 'default'),
                'max_instances': job_def.get('max_instances', 1),
                'replace_existing': True,
                **job_def.get('trigger_args', {})
            }
            self.scheduler.add_job(**job_kwargs)

    def start(self):
        """Starts the scheduler."""
        logging.info("Starting scheduler...")
        self.scheduler.start()
        logging.info("Scheduler started successfully.")

    def shutdown(self):
        """Shuts down the scheduler gracefully."""
        logging.info("Shutting down scheduler...")
        self.scheduler.shutdown()
        logging.info("Scheduler shut down successfully.")
