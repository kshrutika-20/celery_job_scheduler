import os
from dotenv import load_dotenv
from apscheduler.jobstores.mongodb import MongoDBJobStore
from datetime import datetime
import pendulum
from pymongo import MongoClient

load_dotenv(dotenv_path='.env')


class SchedulerClient:
    """Provides an interface to manage jobs and view statuses."""

    def __init__(self):
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", 27017))
        self.db_name = os.getenv("MONGO_DB", "apscheduler_db")
        self.collection_name = os.getenv("MONGO_COLLECTION", "jobs")

        if os.getenv("MONGO_USER") and os.getenv("MONGO_PASS"):
            uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASS')}@{host}:{port}/{self.db_name}?authSource=admin"
            self.mongo_client = MongoClient(uri)
        else:
            self.mongo_client = MongoClient(host, port)

        self.jobstore = MongoDBJobStore(database=self.db_name, collection=self.collection_name,
                                        client=self.mongo_client)

    def get_all_jobs(self):
        """Fetches all jobs and enriches them with status and human-readable times."""
        jobs = self.jobstore.get_all_jobs()

        status_collection = self.mongo_client[self.db_name].job_statuses
        statuses = {s['job_id']: s for s in status_collection.find()}

        job_list = []
        for job in jobs:
            status_info = statuses.get(job.id, {})
            last_run = status_info.get("history", [{}])[-1]  # Get the last element of the history
            last_status = last_run.get("status", "Unknown")

            # Determine current UI status
            if job.next_run_time is None:
                ui_status = "Paused"
            elif last_status == "Running":
                ui_status = "Running"
            elif last_status == "Failure":
                ui_status = "Failing"
            else:  # Success or Unknown
                ui_status = "Scheduled"

            job_list.append({
                "id": job.id,
                "name": job.name,
                "next_run_time_utc": job.next_run_time.isoformat() if job.next_run_time else "N/A",
                "next_run_time_human": pendulum.instance(
                    job.next_run_time).diff_for_humans() if job.next_run_time else "Paused",
                "trigger": str(job.trigger),
                "last_status": last_status,
                "last_run_utc": last_run.get("timestamp_utc", "N/A"),
                "ui_status": ui_status
            })
        return job_list

    def get_job(self, job_id: str):
        return self.jobstore.lookup_job(job_id)

    def pause_job(self, job_id: str):
        job = self.get_job(job_id)
        if job:
            job.next_run_time = None
            self.jobstore.update_job(job)
            return True
        return False

    def resume_job(self, job_id: str):
        job = self.get_job(job_id)
        if job:
            now = datetime.now(job.trigger.timezone)
            job.next_run_time = job.trigger.get_next_fire_time(None, now)
            self.jobstore.update_job(job)
            return True
        return False

    def delete_job(self, job_id: str):
        try:
            self.jobstore.remove_job(job_id)
            status_collection = self.mongo_client[self.db_name].job_statuses
            status_collection.delete_one({"job_id": job_id})
            return True
        except Exception:
            return False
