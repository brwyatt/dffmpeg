from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from dffmpeg.common.models import JobMetricsResponse, MetricCounts
from dffmpeg.coordinator.api.dependencies import (
    get_auth_repo,
    get_config,
    get_job_repo,
    verify_metrics_ip,
)
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.jobs import JobRepository

router = APIRouter()


@router.get(
    "/metrics",
    response_model=JobMetricsResponse,
    dependencies=[Depends(verify_metrics_ip)],
)
async def get_metrics(
    job_repo: JobRepository = Depends(get_job_repo),
    auth_repo: AuthRepository = Depends(get_auth_repo),
    config: CoordinatorConfig = Depends(get_config),
):
    # 1. Seed empty categories
    binaries = list(config.allowed_binaries)
    all_clients = await auth_repo.list_identities()
    worker_ids = [w.client_id for w in all_clients if w.role == "worker"]

    metrics = JobMetricsResponse(
        total=MetricCounts(),
        per_binary={b: MetricCounts() for b in binaries},
        per_worker={w: MetricCounts() for w in worker_ids},
    )

    # 2. Get recent jobs (last 5 minutes = 300 seconds)
    jobs = await job_repo.get_recent_jobs(window_seconds=300)
    now = datetime.now(timezone.utc)
    cutoff_1m = now.timestamp() - 60
    cutoff_5m = now.timestamp() - 300

    terminal_statuses = {"completed", "failed", "canceled"}

    # 3. In-memory tally
    for job in jobs:
        is_terminal = job.status in terminal_statuses
        job_time = job.last_update.timestamp() if job.last_update else 0

        # Current
        is_current = not is_terminal

        # Last 1m
        is_1m = is_current or (is_terminal and job_time >= cutoff_1m)

        # Last 5m
        is_5m = is_current or (is_terminal and job_time >= cutoff_5m)

        # Tally totals
        if is_current:
            metrics.total.current += 1
        if is_1m:
            metrics.total.last_1m += 1
        if is_5m:
            metrics.total.last_5m += 1

        # Tally per binary
        b = job.binary_name
        if b not in metrics.per_binary:
            metrics.per_binary[b] = MetricCounts()
        if is_current:
            metrics.per_binary[b].current += 1
        if is_1m:
            metrics.per_binary[b].last_1m += 1
        if is_5m:
            metrics.per_binary[b].last_5m += 1

        # Tally per worker
        w = job.worker_id
        if w:
            if w not in metrics.per_worker:
                metrics.per_worker[w] = MetricCounts()
            if is_current:
                metrics.per_worker[w].current += 1
            if is_1m:
                metrics.per_worker[w].last_1m += 1
            if is_5m:
                metrics.per_worker[w].last_5m += 1

    return metrics
