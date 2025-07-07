import time
import sys
import logging
import threading
import uvicorn
from fastapi import FastAPI, HTTPException
from scheduler_core import SchedulerManager
from config import settings

# --- Main Scheduler Application ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
scheduler_manager = SchedulerManager()

# --- Internal Control API ---
control_app = FastAPI()


def _job_or_404(job_id):
    job = scheduler_manager.scheduler.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found in the running scheduler instance")
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


@control_app.delete("/control/remove/{job_id}")
def remove(job_id: str):
    _job_or_404(job_id)
    scheduler_manager.scheduler.remove_job(job_id)
    logging.info(f"Removed job '{job_id}' via control API.")
    return {"status": "removed", "job_id": job_id}


def run_control_api():
    """Runs the internal control API in a separate thread."""
    uvicorn.run(control_app, host="0.0.0.0", port=9001)


# --- Main Entry Point ---
def main():
    """Main function to run the scheduler and its control API."""
    try:
        # Start the control API in a background thread
        control_api_thread = threading.Thread(target=run_control_api, daemon=True)
        control_api_thread.start()
        logging.info("Internal control API started on port 9001.")

        # Schedule initial jobs and start the main scheduler loop
        scheduler_manager.schedule_all_jobs()
        scheduler_manager.start()
        print("Scheduler is running. Press Ctrl+C to exit.")

        # Keep the main thread alive
        control_api_thread.join()

    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutdown signal received.")
    finally:
        if scheduler_manager:
            scheduler_manager.shutdown()
        logging.info("Scheduler shut down successfully.")


if __name__ == "__main__":
    main()
