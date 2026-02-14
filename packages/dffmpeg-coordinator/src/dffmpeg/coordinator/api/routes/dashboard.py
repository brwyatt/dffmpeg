import importlib.resources
from datetime import datetime, timezone
from typing import Optional

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
templates = Jinja2Templates(directory=str(templates_dir))


def format_utc(dt: Optional[datetime]) -> Optional[str]:
    """
    Format a datetime object as an ISO 8601 string with UTC timezone.
    If the datetime is naive, it is assumed to be UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


async def get_status_data(window: int, job_repo: JobRepository, worker_repo: WorkerRepository):
    # Use the same window for offline workers as for recent jobs
    online_workers = await worker_repo.get_workers_by_status("online")
    offline_workers = await worker_repo.get_workers_by_status("offline", since_seconds=window)
    workers = online_workers + offline_workers

    # Sort workers: Online first, then by last seen (desc), then by ID (asc)
    # Using python sort for simplicity in the route
    workers.sort(key=lambda w: (w.status != "online", -(w.last_seen.timestamp() if w.last_seen else 0), w.worker_id))

    worker_load = await job_repo.get_worker_load()

    # Get recent jobs (limit 50)
    jobs = await job_repo.get_dashboard_jobs(limit=50, recent_window_seconds=window)

    return {
        "workers": [
            {
                "worker_id": w.worker_id,
                "status": w.status,
                "last_seen": format_utc(w.last_seen),
                "binaries": w.binaries,
                "paths": w.paths,
            }
            for w in workers
        ],
        "worker_load": worker_load,
        "jobs": [
            {
                "job_id": str(j.job_id),
                "status": j.status,
                "exit_code": j.exit_code,
                "binary_name": j.binary_name,
                "requester_id": j.requester_id,
                "worker_id": j.worker_id,
                "created_at": format_utc(j.created_at),
                "last_update": format_utc(j.last_update),
            }
            for j in jobs
        ],
    }


@router.get("/status", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    config: CoordinatorConfig = Depends(get_config),
):
    if not config.web_dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    return templates.TemplateResponse(
        request=request,
        name="status.html",
        context={},
    )


@router.get("/status/data")
async def dashboard_data(
    window: int = 3600,
    config: CoordinatorConfig = Depends(get_config),
    job_repo: JobRepository = Depends(get_job_repo),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    if not config.web_dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")

    return await get_status_data(window, job_repo, worker_repo)
