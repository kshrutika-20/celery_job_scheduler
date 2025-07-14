import redis
import json

class RedisCounterManager:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)

    def init_counter(self, job_id: str, total_repos: int):
        self.redis.set(f"repo_counter:{job_id}", 0)
        self.redis.set(f"repo_total:{job_id}", total_repos)

    def increment_counter(self, job_id: str):
        count = self.redis.incr(f"repo_counter:{job_id}")
        total = int(self.redis.get(f"repo_total:{job_id}") or 0)
        if count == total:
            self.redis.publish("repo_tasks", json.dumps({"event": "complete", "job_id": job_id, "count": count}))
        return count

    def get_status(self, job_id: str):
        processed = int(self.redis.get(f"repo_counter:{job_id}") or 0)
        total = int(self.redis.get(f"repo_total:{job_id}") or 0)
        return {"job_id": job_id, "processed": processed, "total": total}
