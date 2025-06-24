from celery import Celery
from config import Config

celery_app = Celery("scheduler",
                    broker=Config.CELERY_BROKER_URL,
                    backend=Config.CELERY_BACKEND_URL,
                    include=["job_definitions"])

celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_max_tasks_per_child=100,
    timezone=Config.TIMEZONE,
    broker_connection_retry_on_startup=True,
)