from celery_app import celery_app
from config import Config
import time
import random
import logging

logger = logging.getLogger(__name__)

@celery_app.task(name="job_definitions.my_scheduled_task", bind=True, autoretry_for=(Exception,),
                 max_retries=Config.MAX_RETRIES,
                 retry_backoff=Config.RETRY_BACKOFF,
                 retry_backoff_max=Config.RETRY_BACKOFF_MAX)
def my_scheduled_task(self, name):
    print(f"Running task for {name}")
    if random.choice([True, False]):
        raise Exception("Random failure, will retry")
    time.sleep(2)
    print(f"Completed task for {name}")

@celery_app.task(name="job_definitions.another_task", bind=True, autoretry_for=(Exception,),
                 max_retries=Config.MAX_RETRIES,
                 retry_backoff=Config.RETRY_BACKOFF,
                 retry_backoff_max=Config.RETRY_BACKOFF_MAX)
def another_task(self, name):
    print(f"Running another task for {name}")
    time.sleep(1)
    print(f"Completed another task for {name}")

class TaskRegistry:
    @classmethod
    def get_task_by_name(cls, name):
        full_name = f"job_definitions.{name}"
        task = celery_app.tasks.get(full_name)
        if not task:
            logger.warning(f"Task '{full_name}' not found in registry.")
        return task

def run_celery_task(task_name, args):
    task = TaskRegistry.get_task_by_name(task_name)
    if task:
        task.apply_async(args=args)
    else:
        logger.error(f"Failed to find task '{task_name}' for execution.")