from datetime import datetime
import pendulum
from apscheduler.jobstores.mongodb import MongoDBJobStore
from database import get_mongo_client
from config import settings

class SchedulerClient:
    def __init__(self):
        self.mongo_client = get_mongo_client()
        self.jobstore = MongoDBJobStore(database=settings.MONGO_DB, collection=settings.MONGO_SCHEDULER_COLLECTION, client=self.mongo_client)
        self.status_collection = self.mongo_client[settings.MONGO_DB][settings.MONGO_STATUS_COLLECTION]
    def get_all_jobs(self):
        jobs = self.jobstore.get_all_jobs()
        statuses = {s['job_id']: s for s in self.status_collection.find()}
        job_defs = {job['id']: job for job in JOB_DEFINITIONS}
        job_list = []
        for job in jobs:
            status_info = statuses.get(job.id, {})
            history = status_info.get("history", [])
            last_run = history[-1] if history else {}
            last_status = last_run.get("status", "Unknown")

            # Find the most recent run that has progress data.
            last_completed_progress = {}
            for run in reversed(history):
                if "progress" in run:
                    last_completed_progress = run["progress"]
                    break

            if job.next_run_time is None: ui_status = "Paused"
            elif last_status == "Running": ui_status = "Running"
            elif last_status == "Failure": ui_status = "Failing"
            else: ui_status = "Scheduled"

            job_list.append({
                "id": job.id, "name": job.name,
                "next_run_time_utc": job.next_run_time.isoformat() if job.next_run_time else "N/A",
                "next_run_time_human": pendulum.instance(job.next_run_time).diff_for_humans() if job.next_run_time else "Paused",
                "last_status": last_status,
                "ui_status": ui_status,
                "last_workflow_id": last_run.get("trace_id"),
                "is_workflow_starter": job_defs.get(job.id, {}).get("is_workflow_starter", False),
                "last_progress": last_completed_progress
            })
        return job_list