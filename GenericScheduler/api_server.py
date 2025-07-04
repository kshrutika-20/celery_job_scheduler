from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from scheduler_client import SchedulerClient

app = FastAPI(title="Scheduler Management API")
client = SchedulerClient()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    jobs = client.get_all_jobs()
    return templates.TemplateResponse("index.html", {"request": request, "jobs": jobs})

@app.get("/api/jobs")
async def get_jobs(): return client.get_all_jobs()

@app.post("/api/jobs/{job_id}/pause")
async def pause_job_endpoint(job_id: str):
    if client.pause_job(job_id): return {"status": "success", "message": f"Job '{job_id}' paused."}
    raise HTTPException(status_code=404, detail="Job not found")

@app.post("/api/jobs/{job_id}/resume")
async def resume_job_endpoint(job_id: str):
    if client.resume_job(job_id): return {"status": "success", "message": f"Job '{job_id}' resumed."}
    raise HTTPException(status_code=404, detail="Job not found")

@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    if client.delete_job(job_id): return {"status": "success", "message": f"Job '{job_id}' deleted."}
    raise HTTPException(status_code=404, detail="Job not found")
