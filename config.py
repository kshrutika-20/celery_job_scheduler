import os

class Config:
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_BACKEND_URL = os.getenv("CELERY_BACKEND_URL", "redis://localhost:6379/0")
    MAX_RETRIES = 5
    RETRY_BACKOFF = True
    RETRY_BACKOFF_MAX = 600  # in seconds
    TIMEZONE = "Asia/Kolkata"

    JOB_DEFINITIONS = [
        {
            "id": "daily_task",
            "func": "my_scheduled_task",
            "trigger": "cron",
            "args": ["daily_job"],
            "trigger_args": {"hour": 0, "minute": 0}
        },
        {
            "id": "interval_task",
            "func": "another_task",
            "trigger": "interval",
            "args": ["interval_job"],
            "trigger_args": {"minutes": 10}
        }
    ]