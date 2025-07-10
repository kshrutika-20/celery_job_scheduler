from fastapi import APIRouter, HTTPException
import httpx
import os

router = APIRouter()

REDIS_ENTERPRISE_API_URL = os.getenv("REDIS_ENTERPRISE_API_URL", "https://your-redis-url:9443")
REDIS_ENTERPRISE_API_USERNAME = os.getenv("REDIS_ENTERPRISE_API_USERNAME", "your-username")
REDIS_ENTERPRISE_API_PASSWORD = os.getenv("REDIS_ENTERPRISE_API_PASSWORD", "your-password")


@router.get("/redis/stats", summary="Fetch latest Redis DB stats")
async def get_redis_stats():
    """
    Fetches real-time memory and performance stats for all Redis databases.
    """
    url = f"{REDIS_ENTERPRISE_API_URL}/v1/bdbs/stats/last"

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD))
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch stats: {response.text}")

            raw_stats = response.json().get("bdbs", [])

            formatted_stats = []
            for db in raw_stats:
                formatted_stats.append({
                    "name": db.get("name"),
                    "used_memory_mb": round(db.get("used_memory", 0) / (1024 ** 2), 2),
                    "used_memory_peak_mb": round(db.get("used_memory_peak", 0) / (1024 ** 2), 2),
                    "used_memory_rss_mb": round(db.get("used_memory_rss", 0) / (1024 ** 2), 2),
                    "ops_per_sec": db.get("ops"),
                    "avg_latency_ms": round(db.get("avg_latency", 0), 2),
                    "throughput_bytes_sec": db.get("throughput"),
                    "connected_clients": db.get("clients")
                })

            return formatted_stats

    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")


@router.get("/memory/usage")
async def get_memory_usage():
    """
    Returns current memory usage for all Redis databases.
    """
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(
                f"{REDIS_ENTERPRISE_API_URL}/v1/bdbs",
                auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD),
            )
            if response.status_code == 200:
                dbs = response.json()
                return [
                    {
                        "name": db.get("name"),
                        "memory_size_mb": db.get("memory_size") / (1024 * 1024),
                        "used_memory_mb": db.get("memory_usage") / (1024 * 1024),
                        "port": db.get("port"),
                    }
                    for db in dbs
                ]
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/memory/update-size/{db_id}")
async def update_memory_size(db_id: int, new_size_mb: int):
    """
    Updates the memory limit of a specific Redis database.
    """
    payload = {"memory_size": new_size_mb * 1024 * 1024}
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.put(
                f"{REDIS_ENTERPRISE_API_URL}/v1/bdbs/{db_id}",
                json=payload,
                auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD),
            )
            if response.status_code in [200, 202]:
                return {"status": "success", "message": f"Memory size updated for DB {db_id}"}
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/memory/flush/{db_id}")
async def flush_db_keys(db_id: int):
    """
    Manually flushes all keys in a Redis database (DANGEROUS!).
    """
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(
                f"{REDIS_ENTERPRISE_API_URL}/v1/bdbs/{db_id}/flush",
                auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD),
            )
            if response.status_code in [200, 202]:
                return {"status": "flushed", "db_id": db_id}
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/memory/config/{db_id}")
async def get_memory_config(db_id: int):
    """
    Returns memory configuration of a Redis DB (e.g., eviction policy).
    """
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(
                f"{REDIS_ENTERPRISE_API_URL}/v1/bdbs/{db_id}",
                auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD),
            )
            if response.status_code == 200:
                db_config = response.json()
                return {
                    "name": db_config.get("name"),
                    "eviction_policy": db_config.get("eviction_policy"),
                    "memory_size_mb": db_config.get("memory_size") / (1024 * 1024),
                    "dataset_ram_size_mb": db_config.get("dataset_ram_size") / (1024 * 1024),
                }
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cluster/memory")
async def get_cluster_memory():
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(
            f"{REDIS_ENTERPRISE_API_URL}/v1/cluster",
            auth=(REDIS_ENTERPRISE_API_USERNAME, REDIS_ENTERPRISE_API_PASSWORD),
        )
        if response.status_code == 200:
            cluster = response.json()
            return {
                "total_memory_gb": round(cluster["memory_size"] / (1024 ** 3), 2),
                "free_memory_gb": round(cluster["free_memory"] / (1024 ** 3), 2),
                "used_memory_gb": round(cluster["used_memory"] / (1024 ** 3), 2)
            }
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)
