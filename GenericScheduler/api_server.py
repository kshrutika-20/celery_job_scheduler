from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from scheduler_client import SchedulerClient

app = FastAPI(title="Scheduler Management API")
client = SchedulerClient()
templates = Jinja2Templates(directory="templates")


class JobConfig(BaseModel):
    id: str
    func: str = Field(..., description="String path to the function, e.g., 'custom_functions:daily_report_job'")
    trigger: str = Field(..., description="Trigger type, e.g., 'cron' or 'interval'")
    hour: int | None = None
    minute: int | None = None
    seconds: int | None = None


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main UI page which displays jobs."""
    jobs = client.get_all_jobs()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})


@app.get("/api/jobs")
async def get_jobs():
    """API endpoint to get all jobs."""
    return client.get_all_jobs()


@app.post("/api/jobs/{job_id}/pause")
async def pause_job_endpoint(job_id: str):
    """API endpoint to pause a job."""
    if client.pause_job(job_id):
        return {"status": "success", "message": f"Job '{job_id}' paused."}
    raise HTTPException(status_code=404, detail="Job not found")


@app.post("/api/jobs/{job_id}/resume")
async def resume_job_endpoint(job_id: str):
    """API endpoint to resume a job."""
    if client.resume_job(job_id):
        return {"status": "success", "message": f"Job '{job_id}' resumed."}
    raise HTTPException(status_code=404, detail="Job not found")


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    """API endpoint to delete a job."""
    if client.delete_job(job_id):
        return {"status": "success", "message": f"Job '{job_id}' deleted."}
    raise HTTPException(status_code=404, detail="Job not found")


@app.post("/api/jobs")
async def add_job_endpoint(job_config: JobConfig):
    """API endpoint to add a new job."""
    trigger_args = {}
    if job_config.trigger == 'cron':
        trigger_args = {'hour': job_config.hour, 'minute': job_config.minute}
    elif job_config.trigger == 'interval':
        trigger_args = {'seconds': job_config.seconds}

    config_dict = {
        'id': job_config.id,
        'func': 'scheduler_core:task_wrapper',
        'args': [job_config.id],
        'trigger': job_config.trigger,
        'replace_existing': True,
        **trigger_args
    }

    if client.add_job(config_dict):
        return {"status": "success", "message": f"Job '{job_config.id}' added."}
    raise HTTPException(status_code=400, detail="Failed to add job")
