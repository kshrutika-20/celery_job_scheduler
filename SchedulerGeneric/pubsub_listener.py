import threading
import redis
import json
import logging
import requests
from config import settings

def pubsub_listener():
    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("repo_tasks")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue

        data = json.loads(message["data"])
        if data.get("event") == "complete":
            job_id = data["job_id"]
            logging.info(f"[PUBSUB] All repos fetched for job: {job_id}, triggering label fetch")

            try:
                response = requests.post(
                    url=f"{settings.SCHEDULER_CONTROL_API_URL}/control/trigger/fetch_labels",
                    timeout=10
                )
                response.raise_for_status()
                logging.info(f"[PUBSUB] Successfully triggered fetch_labels task.")
            except Exception as e:
                logging.error(f"[PUBSUB] Failed to trigger fetch_labels task: {e}")
