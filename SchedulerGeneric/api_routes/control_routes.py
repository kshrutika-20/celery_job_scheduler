import uuid
from fastapi import FastAPI, HTTPException
from scheduler_core import SchedulerManager

scheduler_manager = SchedulerManager()

# --- Internal Control API ---
control_app = FastAPI()


def _job_or_404(job_id):
    job = scheduler_manager.scheduler.get_job(job_id)
    if not job:
        # Also check job definitions for manually triggered jobs
        if job_id not in scheduler_manager.job_definitions:
            raise HTTPException(status_code=404,
                                detail="Job not found in the running scheduler instance or definitions")
    return job


@control_app.post("/control/pause/{job_id}")
def pause(job_id: str):
    _job_or_404(job_id)
    scheduler_manager.scheduler.pause_job(job_id)
    logging.info(f"Paused job '{job_id}' via control API.")
    return {"status": "paused", "job_id": job_id}


@control_app.post("/control/resume/{job_id}")
def resume(job_id: str):
    _job_or_404(job_id)
    scheduler_manager.scheduler.resume_job(job_id)
    logging.info(f"Resumed job '{job_id}' via control API.")
    return {"status": "resumed", "job_id": job_id}


@control_app.post("/control/trigger/{job_id}")
def trigger(job_id: str):
    """Triggers a job to run immediately, one time."""
    _job_or_404(job_id)
    # Generate a unique ID for this immediate run to avoid conflicts
    run_id = f"{job_id}_manual_{uuid.uuid4()}"
    scheduler_manager.scheduler.add_job(
        'scheduler_core:task_wrapper',
        trigger='date',
        args=[job_id],
        id=run_id,
        name=f"{job_id} (Manual Run)",
        replace_existing=False
    )
    logging.info(f"Manually triggered job '{job_id}' with run ID '{run_id}' via control API.")
    return {"status": "triggered", "job_id": job_id, "run_id": run_id}


@control_app.delete("/control/remove/{job_id}")
def remove(job_id: str):
    _job_or_404(job_id)
    scheduler_manager.scheduler.remove_job(job_id)
    logging.info(f"Removed job '{job_id}' via control API.")
    return {"status": "removed", "job_id": job_id}


def run_control_api():
    """Runs the internal control API in a separate thread."""
    uvicorn.run(control_app, host="0.0.0.0", port=9001)