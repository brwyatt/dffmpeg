import importlib.resources

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from dffmpeg.coordinator.api.dependencies import (
    get_config,
    get_job_repo,
    get_worker_repo,
)
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.workers import WorkerRepository

router = APIRouter()

# Locate the templates directory relative to the package root
templates_dir = importlib.resources.files("dffmpeg.coordinator") / "templates"
templates = Jinja2Templates(directory=templates_dir)


@router.get("/status", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    config: CoordinatorConfig = Depends(get_config),
    job_repo: JobRepository = Depends(get_job_repo),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    if not config.web_dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    online_workers = await worker_repo.get_workers_by_status("online")
    offline_workers = await worker_repo.get_workers_by_status("offline", since_seconds=3600)
    workers = online_workers + offline_workers

    # Sort workers: Online first, then by last seen
    workers.sort(key=lambda w: (w.status != "online", w.last_seen), reverse=True)

    worker_load = await job_repo.get_worker_load()

    # Get recent jobs (limit 50, window 1 hour default)
    jobs = await job_repo.get_dashboard_jobs(limit=50)

    return templates.TemplateResponse(
        request=request,
        name="status.html",
        context={
            "workers": workers,
            "worker_load": worker_load,
            "jobs": jobs,
        },
    )
