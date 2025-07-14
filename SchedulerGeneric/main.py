import time
import sys
import logging
import threading
import uvicorn
import uuid
from fastapi import FastAPI, HTTPException
from scheduler_core import SchedulerManager
from control_routes import router as control_router
from api_server import app as external_app
from pubsub_listener import pubsub_listener
from config import settings

# --- Main Scheduler Application ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
scheduler_manager = SchedulerManager()

internal_control_app = FastAPI()
internal_control_app.include_router(control_router)

def run_internal_control_api():
    uvicorn.run(internal_control_app, host="0.0.0.0", port=9001)

def run_external_api():
    uvicorn.run(external_app, host="0.0.0.0", port=8000)


# --- Main Entry Point ---
def main():
    """Main function to run the scheduler and its control API."""
    try:
        # Start the control API in a background thread
        control_api_thread = threading.Thread(target=run_internal_control_api, daemon=True)
        control_api_thread.start()
        logging.info("Internal control API started on port 9001.")

        # Start Pub/Sub listener
        listener_thread = threading.Thread(target=pubsub_listener, daemon=True)
        listener_thread.start()

        threading.Thread(target=run_external_api, daemon=True).start()
        logging.info("External FastAPI (UI) API started at :8000")

        # Schedule initial jobs and start the main scheduler loop
        scheduler_manager.schedule_all_jobs()
        scheduler_manager.start()
        print("Scheduler is running. Press Ctrl+C to exit.")

    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutdown signal received.")
    finally:
        if scheduler_manager:
            scheduler_manager.shutdown()
        logging.info("Scheduler shut down successfully.")


if __name__ == "__main__":
    main()