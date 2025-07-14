from fastapi import APIRouter
from redis_counter import RedisCounterManager

router = APIRouter()
counter = RedisCounterManager()

@router.get("/api/job-status/{job_id}")
async def get_job_status(job_id: str):
    """
    Returns the current progress of a job.
    """
    return counter.get_status(job_id)
