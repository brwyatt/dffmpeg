from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dffmpeg.common.colors import Colors, colorize, colorize_status
from dffmpeg.common.models import Job, JobRecord, Worker, WorkerRecord


def format_timestamp(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def print_job_list(jobs: List[Job] | List[JobRecord], show_requester: bool = False):
    if not jobs:
        print("No jobs found.")
        return

    # Header
    requester_header = f"{'Requester':<20} " if show_requester else ""
    print(f"{'Job ID':<26} {requester_header}{'Status':<21} {'Binary':<10} {'Created'}")

    # Separator
    separator_len = 80 + (21 if show_requester else 0)
    print("-" * separator_len)

    # Rows
    for job in jobs:
        status_str = f"{job.status}{f' ({job.exit_code})' if job.exit_code not in (None, 0) else ''}"
        requester_col = f"{job.requester_id:<20} " if show_requester else ""

        print(
            f"{str(job.job_id):<26} {requester_col}{colorize_status(status_str):<21} {job.binary_name:<10} "
            f"{format_timestamp(job.created_at)}"
        )


def print_job_details(job: Job | JobRecord):
    print(f"Job ID:       {colorize(str(job.job_id), Colors.CYAN)}")
    print(f"Requester:    {job.requester_id}")
    status_str = f"{job.status}{f' ({job.exit_code})' if job.exit_code not in (None, 0) else ''}"
    print(f"Status:       {colorize_status(status_str)}")

    if job.exit_code is not None:
        color = Colors.GREEN if job.exit_code == 0 else Colors.RED
        print(f"Exit Code:    {colorize(str(job.exit_code), color)}")

    print(f"Worker:       {job.worker_id or '<Unassigned>'}")
    print(f"Binary:       {job.binary_name}")
    print(f"Args:         {' '.join(job.arguments)}")
    print(f"Paths:        {', '.join(job.paths)}")
    print(f"Created:      {format_timestamp(job.created_at)}")
    print(f"Last Update:  {format_timestamp(job.last_update)}")

    if job.worker_last_seen:
        print(f"Worker Seen:  {format_timestamp(job.worker_last_seen)}")
    if job.client_last_seen:
        print(f"Client Seen:  {format_timestamp(job.client_last_seen)}")


def print_worker_list(workers: List[Worker] | List[WorkerRecord]):
    if not workers:
        print("No workers found.")
        return

    # Sort: Online first, then by last seen (descending), then ID (ascending)
    # Note: We assume workers list is already sorted or we sort it here.
    # To be safe and consistent, let's sort here if it's not too expensive,
    # but usually the caller might want to control sorting.
    # The requirement was just formatting, but consistent sorting is part of "common output".

    sorted_workers = sorted(
        workers, key=lambda w: (w.status != "online", -(w.last_seen.timestamp() if w.last_seen else 0), w.worker_id)
    )

    print(f"{'Worker ID':<20} {'Status':<21} {'Last Seen':<20}")
    print("-" * 63)
    for w in sorted_workers:
        last_seen_str = format_timestamp(w.last_seen)
        print(f"{w.worker_id:<20} {colorize_status(w.status):<21} {last_seen_str}")


def print_worker_details(worker: Worker | WorkerRecord):
    last_seen_str = format_timestamp(worker.last_seen)

    # Check for staleness if registration_interval is available
    # We don't have easy access to "now" in a way that matches the DB check exactly,
    # but we can do a rough check if needed.
    # The Admin CLI did: if last_seen < now - interval.
    # We'll just print it as is for now, or we could pass in a "stale" flag?
    # For now, let's stick to simple formatting. The caller can colorize if they want,
    # but moving the color logic here requires `datetime.now()`.

    is_stale = False
    if worker.last_seen:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=worker.registration_interval * 1.5)  # 1.5x grace
        if worker.last_seen < cutoff:
            is_stale = True

    if is_stale and worker.status == "online":
        last_seen_str = colorize(last_seen_str, Colors.YELLOW)
    elif worker.status == "online":
        last_seen_str = colorize(last_seen_str, Colors.GREEN)

    print(f"Worker ID:    {colorize(worker.worker_id, Colors.CYAN)}")
    print(f"Status:       {colorize_status(worker.status)}")
    print(f"Last Seen:    {last_seen_str}")
    print(f"Binaries:     {', '.join(sorted(worker.binaries))}")
    print(f"Capabilities: {', '.join(worker.capabilities)}")
    print(f"Paths:        {', '.join(sorted(worker.paths))}")
    print(f"Interval:     {worker.registration_interval}s")
    # Transport info might not be on the base Worker model if strictly following schemas,
    # but WorkerRecord has it. Let's check if we can access it safely.
    if isinstance(worker, WorkerRecord):
        print(f"Transport:    {worker.transport}")
