import logging
import uuid
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from job_definitions import JOB_DEFINITIONS
from custom_functions import JOB_FUNCTIONS
from database import get_mongo_client
from config import settings


def task_wrapper(job_id: str):
    """A robust wrapper that manages the full lifecycle of a job run."""
    trace_id = str(uuid.uuid4())
    mongo_client = None
    try:
        mongo_client = get_mongo_client()
        status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]

        logging.info(f"[{trace_id}] Task wrapper executing for job_id: {job_id}")
        run_record = {"trace_id": trace_id, "status": "Running", "timestamp_utc": datetime.now(timezone.utc)}
        status_collection.update_one(
            {"job_id": job_id},
            {"$push": {"history": {"$each": [run_record], "$slice": -50}}, "$set": {"job_id": job_id}},
            upsert=True
        )

        job_id_to_function = {job["id"]: job["function"] for job in JOB_DEFINITIONS if "function" in job}
        func_name = job_id_to_function.get(job_id)
        if not func_name: raise ValueError(f"No function name defined for job_id: {job_id}")
        func = JOB_FUNCTIONS.get(func_name)
        if not func: raise ValueError(f"No function found for name: {func_name}")

        if not func(trace_id=trace_id): raise Exception("Custom function reported a failure.")

        status_collection.update_one(
            {"job_id": job_id, "history.trace_id": trace_id},
            {"$set": {"history.$.status": "Success", "history.$.end_timestamp_utc": datetime.now(timezone.utc)}}
        )
    except Exception as e:
        logging.error(f"[{trace_id}] Execution failed for job_id: {job_id}. Error: {e}")
        if mongo_client:
            status_collection = mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
            status_collection.update_one(
                {"job_id": job_id, "history.trace_id": trace_id},
                {"$set": {"history.$.status": "Failure", "history.$.end_timestamp_utc": datetime.now(timezone.utc),
                          "history.$.exception": str(e)}}
            )
    finally:
        if mongo_client: mongo_client.close()


class SchedulerManager:
    """Manages the lifecycle of the APScheduler instance."""

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
        # wakeup_interval is removed as it's not the correct mechanism for this architecture.
        scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults,
                                        timezone='UTC')
        scheduler.add_listener(self.job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        return scheduler

    def job_event_listener(self, event):
        if event.exception:
            logging.error(f"APScheduler-level error for job {event.job_id}: {event.exception}")
        else:
            logging.info(f"APScheduler successfully triggered job {event.job_id}.")
            self._trigger_dependent_jobs(event.job_id)

    def _trigger_dependent_jobs(self, parent_job_id: str):
        for job_id, job_def in self.job_definitions.items():
            if parent_job_id in job_def.get('dependencies', []):
                self.scheduler.modify_job(job_id, next_run_time=datetime.now(timezone.utc) + timedelta(seconds=1))

    def schedule_all_jobs(self):
        for job_id, job_def in self.job_definitions.items():
            job_kwargs = {
                'id': job_id, 'name': job_def.get('name', job_id), 'func': 'scheduler_core:task_wrapper',
                'args': [job_id], 'trigger': job_def['trigger'], 'executor': job_def.get('executor', 'default'),
                'max_instances': job_def.get('max_instances', 1), 'replace_existing': True,
                **job_def.get('trigger_args', {})
            }
            self.scheduler.add_job(**job_kwargs)

    def start(self):
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()