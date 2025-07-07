import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from scheduler_client import SchedulerClient
from config import settings

app = FastAPI(title="Scheduler Management API")
# The client is now only used for reading job data for the UI
client = SchedulerClient()
templates = Jinja2Templates(directory="templates")

def forward_request_to_scheduler(method: str, path: str):
    """Forwards a command to the internal control API on the scheduler pod."""
    try:
        url = f"{settings.SCHEDULER_CONTROL_API_URL}{path}"
        response = requests.request(method, url, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Scheduler service unavailable: {e}")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main UI page which displays jobs."""
    try:
        jobs = client.get_all_jobs()
        return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})
    except Exception as e:
        logging.error(f"Failed to load jobs for UI: {e}")
        return templates.TemplateResponse("index.html", {"request": request, "jobs": [], "error": str(e)})


@app.get("/api/jobs")
async def get_jobs():
    """API endpoint to get all jobs (read-only from DB)."""
    return client.get_all_jobs()

@app.post("/api/jobs/{job_id}/pause")
async def pause_job_endpoint(job_id: str):
    """Forwards a pause command to the scheduler pod."""
    return forward_request_to_scheduler("post", f"/control/pause/{job_id}")

@app.post("/api/jobs/{job_id}/resume")
async def resume_job_endpoint(job_id: str):
    """Forwards a resume command to the scheduler pod."""
    return forward_request_to_scheduler("post", f"/control/resume/{job_id}")

@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    """Forwards a delete command to the scheduler pod."""
    return forward_request_to_scheduler("delete", f"/control/remove/{job_id}")