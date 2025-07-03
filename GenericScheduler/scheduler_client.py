import os
from dotenv import load_dotenv
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

load_dotenv(dotenv_path='.env')


class SchedulerClient:
    """Provides an interface to manage jobs in the MongoDB store directly."""

    def __init__(self):
        mongo_config = {
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", 27017)),
            "database": os.getenv("MONGO_DB", "apscheduler_db"),
            "collection": os.getenv("MONGO_COLLECTION", "jobs")
        }
        if os.getenv("MONGO_USER") and os.getenv("MONGO_PASS"):
            mongo_config["username"] = os.getenv("MONGO_USER")
            mongo_config["password"] = os.getenv("MONGO_PASS")

        # This job store connects to the DB but does not run a scheduler instance.
        self.jobstore = MongoDBJobStore(**mongo_config)

    def get_all_jobs(self):
        """Fetches and deserializes all jobs from the MongoDB job store."""
        jobs = self.jobstore.get_all_jobs()
        job_list = []
        for job in jobs:
            job_list.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else "Paused",
                "trigger": str(job.trigger),
            })
        return job_list

    def get_job(self, job_id: str):
        """Retrieves a single job by its ID."""
        return self.jobstore.lookup_job(job_id)

    def pause_job(self, job_id: str):
        """Pauses a job by setting its next_run_time to None."""
        job = self.get_job(job_id)
        if job:
            job.next_run_time = None
            self.jobstore.update_job(job)
            return True
        return False

    def resume_job(self, job_id: str):
        """Resumes a paused job by recalculating its next run time."""
        job = self.get_job(job_id)
        if job:
            # Recalculate next run time based on its trigger
            now = datetime.now(job.trigger.timezone)
            job.next_run_time = job.trigger.get_next_fire_time(None, now)
            self.jobstore.update_job(job)
            return True
        return False

    def delete_job(self, job_id: str):
        """Deletes a job from the store."""
        try:
            self.jobstore.remove_job(job_id)
            return True
        except Exception:
            return False

    def add_job(self, job_config: dict):
        """Adds a new job to the store. This is a simplified example."""
        try:
            # We need to create a Job object to add it to the store.
            # The 'scheduler' argument can be a dummy object here.
            class DummyScheduler:
                timezone = 'UTC'

            job = Job(DummyScheduler(), **job_config)
            self.jobstore.add_job(job)
            return True
        except Exception as e:
            print(f"Error adding job: {e}")
            return False