import time, sys, logging
from scheduler_core import SchedulerManager

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    scheduler_manager = None
    try:
        scheduler_manager = SchedulerManager()
        scheduler_manager.schedule_all_jobs()
        scheduler_manager.start()
        print("Scheduler is running. Press Ctrl+C to exit.")
        while True: time.sleep(1)
    except KeyboardInterrupt: logging.info("Keyboard interrupt received. Shutting down.")
    except Exception as e: logging.error(f"Unexpected error in scheduler main: {e}"); sys.exit(1)
    finally:
        if scheduler_manager: scheduler_manager.shutdown()

if __name__ == "__main__": main()