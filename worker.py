from celery_app import celery_app
import job_definitions  # ensure task registration

if __name__ == "__main__":
    celery_app.worker_main(["worker", "--loglevel=info", "--concurrency=4"])