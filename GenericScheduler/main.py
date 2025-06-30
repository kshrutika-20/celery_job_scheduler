# main.py
import time
import sys
import logging
from scheduler_core import SchedulerManager

def main():
    """Main function to run the scheduler application."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        scheduler_manager = SchedulerManager()
        scheduler_manager.schedule_all_jobs()
        scheduler_manager.start()

        print("Scheduler is running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down.")
    except ImportError:
        logging.error("Could not import JOBS from job_definitions.py.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        try:
            scheduler_manager.shutdown()
        except Exception:
            pass

if __name__ == "__main__":
    main()
