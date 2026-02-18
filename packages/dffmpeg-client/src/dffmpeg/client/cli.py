import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from dffmpeg.client.api import DFFmpegClient
from dffmpeg.client.config import load_config
from dffmpeg.common.colors import Colors, colorize
from dffmpeg.common.formatting import (
    print_job_details,
    print_job_list,
    print_worker_details,
    print_worker_list,
)
from dffmpeg.common.models import JobLogsMessage, JobStatusMessage

# Configure logging
# For CLI usage, we generally want INFO or WARNING by default.
# Proxy mode should be quieter to avoid corrupting output streams if possible,
# though stderr is usually safe for logs.
logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger(__name__)


async def stream_and_wait(client: DFFmpegClient, job_id: str, transport: str, metadata: dict) -> int:
    """
    Streams logs and status for a job, waiting for completion.
    Returns exit code (0 for success, 1 for failure/cancellation).
    """
    exit_code = 1

    try:
        async for message in client.stream_job(job_id, transport, metadata):
            if isinstance(message, JobLogsMessage):
                for log in message.payload.logs:
                    # Write to appropriate stream
                    # We assume 'stdout' and 'stderr' streams from worker
                    stream = sys.stdout if log.stream == "stdout" else sys.stderr
                    print(log.content, file=stream)
                    stream.flush()

            elif isinstance(message, JobStatusMessage):
                status = message.payload.status
                if status == "completed":
                    exit_code = message.payload.exit_code if message.payload.exit_code is not None else 0
                    break
                elif status == "failed":
                    exit_code = message.payload.exit_code if message.payload.exit_code is not None else 1
                    break
                elif status == "canceled":
                    exit_code = 130  # Standard SIGINT exit code
                    break

    except asyncio.CancelledError:
        # If we are cancelled locally (Ctrl+C), try to cancel remote job
        print(colorize("\nCanceling job...", Colors.YELLOW), file=sys.stderr)
        try:
            await client.cancel_job(job_id)
        except Exception as e:
            print(colorize(f"Failed to cancel job: {e}", Colors.RED), file=sys.stderr)
        exit_code = 130
        raise

    return exit_code


def process_arguments(raw_args: List[str], path_map: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """
    Processes arguments to identify and replace local paths with path variables.
    Returns (processed_args, used_path_variables)
    """
    processed_args = []
    used_paths = set()

    # Sort path map by length descending (longest match wins)
    sorted_paths = sorted(path_map.items(), key=lambda x: len(x[1]), reverse=True)

    for arg in raw_args:
        # Check for "file:" prefix
        prefix = ""
        path_to_process = arg
        if arg.startswith("file:"):
            prefix = "file:"
            path_to_process = arg[5:]

        # Only process absolute paths (start with /).
        # Skip flags and relative paths.
        if not path_to_process.startswith("/"):
            processed_args.append(arg)
            continue

        # Try to resolve to absolute path for matching
        try:
            # We use abspath to avoid resolving symlinks unless necessary?
            # User config likely uses physical paths.
            # resolve() handles symlinks and '..'
            abs_path = str(Path(path_to_process).resolve())
        except Exception:
            abs_path = str(Path(path_to_process).absolute())

        replaced = False
        for var_name, local_path in sorted_paths:
            # Check if abs_path starts with local_path
            if abs_path.startswith(local_path):
                # Boundary check: ensure match is on directory boundary
                # either exact match, or next char is separator
                remainder = abs_path[len(local_path) :]
                if not remainder or remainder.startswith(os.sep):
                    # Match!
                    # Replace prefix with $Var
                    # Note: We must return the remainder with the variable
                    new_arg = f"{prefix}${var_name}{remainder}"
                    processed_args.append(new_arg)
                    used_paths.add(var_name)
                    replaced = True
                    break

        if not replaced:
            processed_args.append(arg)

    return processed_args, list(used_paths)


async def run_submit(
    binary_name: str,
    raw_args: List[str],
    monitor: bool,
    config_file: str | None = None,
    heartbeat_interval: int | None = None,
) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    if heartbeat_interval is None:
        heartbeat_interval = config.job_heartbeat_interval

    # Process arguments to handle path mapping
    job_args, paths = process_arguments(raw_args, config.paths)

    async with DFFmpegClient(config) as client:
        try:
            job = await client.submit_job(
                binary_name, job_args, paths, monitor=monitor, heartbeat_interval=heartbeat_interval
            )

            if not monitor:
                print(colorize("Job submitted successfully.", Colors.GREEN))
                print(f"Job ID: {colorize(str(job.job_id), Colors.CYAN)}")
                return 0

            # Wait/Monitor mode
            await client._start_heartbeat_loop(str(job.job_id), job.heartbeat_interval)

            return await stream_and_wait(client, str(job.job_id), job.transport, job.transport_metadata)

        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return 1


async def run_worker_list(window: int = 3600 * 24, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            workers = await client.list_workers(window=window)
            print_worker_list(workers)
            return 0
        except Exception as e:
            logger.error(f"Error getting worker list: {e}")
            return 1


async def run_worker_show(worker_id: str, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            worker = await client.get_worker(worker_id)
            print_worker_details(worker)
            return 0
        except Exception as e:
            logger.error(f"Error getting worker details: {e}")
            return 1


async def run_job_list(window: int = 3600, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            jobs = await client.list_jobs(window=window)
            print_job_list(jobs)
            return 0
        except Exception as e:
            logger.error(f"Error getting job list: {e}")
            return 1


async def run_job_show(job_id: str, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            status = await client.get_job_status(job_id)
            print_job_details(status)
            return 0
        except Exception as e:
            logger.error(f"Error getting job details: {e}")
            return 1


async def run_status_cmd(window: int = 3600, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            print(colorize("=== Workers ===", Colors.MAGENTA))
            # Status command usually implies "current state", so maybe window applies to jobs?
            # For workers, we probably want to see recently offline ones too.
            # Let's use the window for both.
            workers = await client.list_workers(window=window)
            print_worker_list(workers)
            print()
            print(colorize("=== Recent Jobs ===", Colors.MAGENTA))
            jobs = await client.list_jobs(window=window)
            print_job_list(jobs)
            return 0
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return 1


async def run_attach(job_id: str, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            # Enable monitoring and start heartbeats
            await client.start_monitoring(job_id, monitor=True)

            # Get transport info to start streaming
            job = await client.get_job_status(job_id)
            if job.status in ["completed", "failed", "canceled"]:
                print(f"Job {job_id} already finished.")
                return 0

            return await stream_and_wait(client, job_id, job.transport, job.transport_metadata)
        except Exception as e:
            logger.error(f"Error attaching to job: {e}")
            return 1


async def run_cancel(job_id: str, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            await client.cancel_job(job_id)
            print(f"Job {colorize(job_id, Colors.CYAN)} cancellation requested.")
            return 0
        except Exception as e:
            logger.error(f"Error canceling job: {e}")
            return 1


async def run_logs(job_id: str, tail: int | None, follow: bool, config_file: str | None = None) -> int:
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            last_msg_id = None
            is_first_fetch = True

            while True:
                # Fetch logs
                # If first fetch and tail is set, use limit=tail
                limit = tail if (is_first_fetch and tail) else None
                resp = await client.get_job_logs(
                    job_id, since_message_id=str(last_msg_id) if last_msg_id else None, limit=limit
                )

                # Sort logs by timestamp just in case
                # LogEntry has timestamp field
                logs = sorted(resp.logs, key=lambda log: (log.timestamp if log.timestamp else 0))

                for log in logs:
                    stream = sys.stdout if log.stream == "stdout" else sys.stderr
                    print(log.content, file=stream)
                    stream.flush()

                if resp.last_message_id:
                    last_msg_id = resp.last_message_id

                is_first_fetch = False

                if not follow:
                    # If not following, we exit if we got no logs this time
                    # OR if we specified tail, we only do one fetch (historical tail)
                    if not resp.logs or tail:
                        break
                    continue

                # Following mode
                job = await client.get_job_status(job_id)
                if job.status in ["completed", "failed", "canceled"]:
                    break

                # If we got no logs this poll, wait a bit
                if not resp.logs:
                    await asyncio.sleep(2)

            # Final check for logs after loop exit (works for follow and non-follow)
            resp = await client.get_job_logs(job_id, since_message_id=str(last_msg_id) if last_msg_id else None)
            logs = sorted(resp.logs, key=lambda log: (log.timestamp if log.timestamp else 0))
            for log in logs:
                stream = sys.stdout if log.stream == "stdout" else sys.stderr
                print(log.content, file=stream)
                stream.flush()

            return 0
        except Exception as e:
            logger.error(f"Error fetching logs: {e}")
            return 1


def main():
    parser = argparse.ArgumentParser(description="dffmpeg client CLI")
    parser.add_argument("--config", "-c", help="Path to config file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit
    submit_parser = subparsers.add_parser("submit", help="Submit a job")
    submit_parser.add_argument("--binary", "-b", default="ffmpeg", help="Binary name (default: ffmpeg)")
    submit_parser.add_argument(
        "--detach", "-D", action="store_true", help="Submit job in background and exit immediately"
    )
    submit_parser.add_argument(
        "--heartbeat-interval", type=int, help="Override heartbeat interval (seconds) for this job"
    )
    submit_parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the binary")

    # Attach
    attach_parser = subparsers.add_parser("attach", help="Attach to an existing job to monitor it")
    attach_parser.add_argument("job_id", help="Job ID")

    # Status
    status_parser = subparsers.add_parser("status", help="Get cluster status")
    status_parser.add_argument("--window", "-w", type=int, default=3600, help="Time window in seconds (default: 3600)")

    # Worker
    worker_parser = subparsers.add_parser("worker", help="Worker management")
    worker_subparsers = worker_parser.add_subparsers(dest="subcommand", required=True)

    # Worker list
    w_list_parser = worker_subparsers.add_parser("list", help="List workers")
    w_list_parser.add_argument(
        "--window", "-w", type=int, default=3600 * 24, help="Time window for offline workers (default: 86400)"
    )

    # Worker show
    w_show_parser = worker_subparsers.add_parser("show", help="Show worker details")
    w_show_parser.add_argument("worker_id", help="Worker ID")

    # Job
    job_parser = subparsers.add_parser("job", help="Job management")
    job_subparsers = job_parser.add_subparsers(dest="subcommand", required=True)

    # Job list
    j_list_parser = job_subparsers.add_parser("list", help="List jobs")
    j_list_parser.add_argument("--window", "-w", type=int, default=3600, help="Time window in seconds (default: 3600)")

    # Job show
    j_show_parser = job_subparsers.add_parser("show", help="Show job details")
    j_show_parser.add_argument("job_id", help="Job ID")

    # Cancel
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a job")
    cancel_parser.add_argument("job_id", help="Job ID")

    # Logs
    logs_parser = subparsers.add_parser("logs", help="Fetch job logs")
    logs_parser.add_argument("job_id", help="Job ID")
    logs_parser.add_argument("--tail", "-n", type=int, help="Number of lines to show (from end)")
    logs_parser.add_argument("--follow", "-f", action="store_true", help="Follow log output")

    args = parser.parse_args()

    try:
        if args.command == "submit":
            # Strip '--' if present in arguments
            job_args = args.arguments
            if job_args and job_args[0] == "--":
                job_args = job_args[1:]

            # Default: active (monitor=True)
            # --detach: inactive (monitor=False)
            monitor = not args.detach

            sys.exit(
                asyncio.run(
                    run_submit(args.binary, job_args, monitor, args.config, heartbeat_interval=args.heartbeat_interval)
                )
            )

        elif args.command == "attach":
            sys.exit(asyncio.run(run_attach(args.job_id, args.config)))

        elif args.command == "status":
            sys.exit(asyncio.run(run_status_cmd(args.window, args.config)))

        elif args.command == "worker":
            if args.subcommand == "list":
                sys.exit(asyncio.run(run_worker_list(args.window, args.config)))
            elif args.subcommand == "show":
                sys.exit(asyncio.run(run_worker_show(args.worker_id, args.config)))

        elif args.command == "job":
            if args.subcommand == "list":
                sys.exit(asyncio.run(run_job_list(args.window, args.config)))
            elif args.subcommand == "show":
                sys.exit(asyncio.run(run_job_show(args.job_id, args.config)))

        elif args.command == "cancel":
            sys.exit(asyncio.run(run_cancel(args.job_id, args.config)))

        elif args.command == "logs":
            sys.exit(asyncio.run(run_logs(args.job_id, args.tail, args.follow, args.config)))

    except KeyboardInterrupt:
        sys.exit(130)


def proxy_main():
    """
    Entry point for proxy scripts (e.g. 'ffmpeg').
    Behaves as if 'submit' was called with the script name as binary.
    """
    binary_name: str = os.path.basename(sys.argv[0])
    job_args = sys.argv[1:]

    try:
        sys.exit(asyncio.run(run_submit(binary_name, job_args, monitor=True)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
