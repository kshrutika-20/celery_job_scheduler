import os
import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from job_definitions import JOB_DEFINITIONS  # <-- IMPORTING from Python file
from custom_functions import JOB_FUNCTIONS

# Build a lookup table from job ID to function name
JOB_ID_TO_FUNCTION = {
    job["id"]: job["function"]
    for job in JOB_DEFINITIONS
    if "function" in job
}

def task_wrapper(job_id: str):
    """
    A wrapper function that is the actual target for every APScheduler job.
    It retrieves job details and calls the mapped function.
    """
    logging.info(f"Task wrapper executing for job_id: {job_id}")

    func_name = JOB_ID_TO_FUNCTION.get(job_id)
    if not func_name:
        logging.error(f"No function name defined for job_id: {job_id}.")
        return

    func = JOB_FUNCTIONS.get(func_name)
    if not func:
        logging.error(f"No function found in JOB_FUNCTIONS for name: {func_name}")
        return

    try:
        func()
    except Exception as e:
        logging.error(f"Execution failed for job_id: {job_id}. Error: {e}")

class SchedulerManager:
    """
    Manages the lifecycle of the APScheduler, loads jobs from a configuration file,
    and handles job dependencies.
    """

    def __init__(self, db_path: str = 'sqlite:///jobs.sqlite'):
        """
        Initializes the SchedulerManager.

        Args:
            db_path (str): Database connection string for the job store.
        """
        self.job_definitions = self._load_job_definitions()
        self.scheduler = self._initialize_scheduler()

    def _load_job_definitions(self) -> dict:
        """Loads job definitions from the imported JOBS list."""
        logging.info("Loading job definitions from job_definitions.py module")
        # Convert the list of jobs to a dictionary for easy lookup by job ID
        return {job['id']: job for job in JOB_DEFINITIONS}

    def _initialize_scheduler(self) -> BackgroundScheduler:
        """Configures and initializes the BackgroundScheduler."""
        logging.info("Initializing scheduler with MongoDB job store...")

        mongo_config = {
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", 27017)),
            "database": os.getenv("MONGO_DB", "apscheduler_db"),
            "collection": os.getenv("MONGO_COLLECTION", "jobs")
        }

        # Optional: Add authentication if credentials are set
        if os.getenv("MONGO_USER") and os.getenv("MONGO_PASS"):
            mongo_config["username"] = os.getenv("MONGO_USER")
            mongo_config["password"] = os.getenv("MONGO_PASS")

        jobstores = {
            'default': MongoDBJobStore(**mongo_config)
        }
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': False,  # Run every instance of a job
            'max_instances': 1  # Default to 1 to prevent overlap
        }
        scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        scheduler.add_listener(self.job_event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        return scheduler

    def job_event_listener(self, event):
        """Listens for job execution events to handle logging and dependencies."""
        job = self.scheduler.get_job(event.job_id)
        if not job:
            logging.warning(f"Event received for a non-existent job_id: {event.job_id}")
            return

        if event.exception:
            logging.error(f"Job {event.job_id} crashed: {event.exception}")
        else:
            logging.info(f"Job {event.job_id} executed successfully.")
            # Check if this job has any dependents
            self._trigger_dependent_jobs(event.job_id)

    def _trigger_dependent_jobs(self, parent_job_id: str):
        """
        Finds and schedules jobs that depend on the successfully completed parent job.
        """
        for job_id, job_def in self.job_definitions.items():
            if job_def.get('depends_on') == parent_job_id:
                logging.info(f"Parent job '{parent_job_id}' completed. Triggering dependent job '{job_id}'.")
                self.scheduler.modify_job(job_id,
                                          next_run_time=datetime.now(self.scheduler.timezone) + timedelta(seconds=1))

    def schedule_all_jobs(self):
        """
        Reads all job definitions and adds/updates them in the scheduler.
        Expects job format to have 'trigger' as a string and 'trigger_args' as a dict.
        """
        logging.info("Scheduling all jobs from definitions...")
        existing_job_ids = {job.id for job in self.scheduler.get_jobs()}
        defined_job_ids = set(self.job_definitions.keys())

        for job_id, job_def in self.job_definitions.items():
            try:
                trigger_type = job_def['trigger']
                trigger_args = job_def.get('trigger_args', {})

                job_kwargs = {
                    'id': job_id,
                    'name': job_def.get('description', job_id),
                    'func': task_wrapper,
                    'args': [job_id],
                    'trigger': trigger_type,
                    'executor': job_def.get('executor', 'default'),
                    'max_instances': job_def.get('max_instances', 1),
                    'replace_existing': True,
                    **trigger_args
                }

                self.scheduler.add_job(**job_kwargs)
                logging.info(f"Scheduled job '{job_id}' with trigger: {trigger_type} and args: {trigger_args}")

            except Exception as e:
                logging.error(f"Failed to schedule job '{job_id}': {e}")

        # Remove obsolete jobs no longer defined
        for job_id in existing_job_ids - defined_job_ids:
            logging.info(f"Removing obsolete job '{job_id}' from scheduler.")
            self.scheduler.remove_job(job_id)

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