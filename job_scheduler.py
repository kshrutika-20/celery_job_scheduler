from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from job_definitions import run_celery_task
from config import Config
import atexit
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler(jobstores={"default": MemoryJobStore()}, timezone=Config.TIMEZONE)

    def schedule_jobs(self):
        logger.info("Scheduling jobs...")

        for job in Config.JOB_DEFINITIONS:
            if job["trigger"] == "cron":
                trigger = CronTrigger(**job["trigger_args"])
            elif job["trigger"] == "interval":
                trigger = IntervalTrigger(**job["trigger_args"])
            else:
                logger.warning(f"Unsupported trigger type: {job['trigger']}")
                continue

            self.scheduler.add_job(
                func=run_celery_task,
                args=[job["func"], job["args"]],
                trigger=trigger,
                id=job["id"],
                replace_existing=True
            )
            logger.info(f"Scheduled job: {job['id']}")

    def start(self):
        self.schedule_jobs()
        self.scheduler.start()
        logger.info("APScheduler started")
        atexit.register(lambda: self.scheduler.shutdown(wait=True))

        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.scheduler.shutdown()
            logger.info("Scheduler shutdown gracefully.")


if __name__ == "__main__":
    scheduler = JobScheduler()
    scheduler.start()
