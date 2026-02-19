import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from dffmpeg.client.api import DFFmpegClient
from dffmpeg.client.config import load_config
from dffmpeg.common.cli_utils import (
    add_config_arg,
    add_job_id_arg,
    add_job_subcommand,
    add_window_arg,
    add_worker_subcommand,
    setup_subcommand,
)
from dffmpeg.common.colors import Colors, colorize
from dffmpeg.common.formatting import (
    print_job_details,
    print_job_list,
    print_worker_details,
    print_worker_list,
)
from dffmpeg.common.models import JobLogsMessage, JobStatusMessage

# Configure logging
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

    sorted_paths = sorted(path_map.items(), key=lambda x: len(x[1]), reverse=True)

    for arg in raw_args:
        prefix = ""
        path_to_process = arg
        if arg.startswith("file:"):
            prefix = "file:"
            path_to_process = arg[5:]

        if not path_to_process.startswith("/"):
            processed_args.append(arg)
            continue

        try:
            abs_path = str(Path(path_to_process).resolve())
        except Exception:
            abs_path = str(Path(path_to_process).absolute())

        replaced = False
        for var_name, local_path in sorted_paths:
            if abs_path.startswith(local_path):
                remainder = abs_path[len(local_path) :]
                if not remainder or remainder.startswith(os.sep):
                    new_arg = f"{prefix}${var_name}{remainder}"
                    processed_args.append(new_arg)
                    used_paths.add(var_name)
                    replaced = True
                    break

        if not replaced:
            processed_args.append(arg)

    return processed_args, list(used_paths)


async def job_submit(client: DFFmpegClient, args: argparse.Namespace) -> int:
    # Strip '--' if present in arguments
    job_args = args.arguments
    if job_args and job_args[0] == "--":
        job_args = job_args[1:]

    monitor = not args.detach
    heartbeat_interval = args.heartbeat_interval or client.config.job_heartbeat_interval

    # Process arguments to handle path mapping
    # Note: client.config is accessible
    processed_job_args, paths = process_arguments(job_args, client.config.paths)

    try:
        job = await client.submit_job(
            args.binary, processed_job_args, paths, monitor=monitor, heartbeat_interval=heartbeat_interval
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


async def worker_list(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        workers = await client.list_workers(window=args.window)
        print_worker_list(workers)
        return 0
    except Exception as e:
        logger.error(f"Error getting worker list: {e}")
        return 1


async def worker_show(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        worker = await client.get_worker(args.worker_id)
        print_worker_details(worker)
        return 0
    except Exception as e:
        logger.error(f"Error getting worker details: {e}")
        return 1


async def job_list(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        jobs = await client.list_jobs(window=args.window)
        print_job_list(jobs)
        return 0
    except Exception as e:
        logger.error(f"Error getting job list: {e}")
        return 1


async def job_show(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        status = await client.get_job_status(args.job_id)
        print_job_details(status)
        return 0
    except Exception as e:
        logger.error(f"Error getting job details: {e}")
        return 1


async def status_cmd(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        print(colorize("=== Workers ===", Colors.MAGENTA))
        await worker_list(client, args)
        print()
        print(colorize("=== Recent Jobs ===", Colors.MAGENTA))
        await job_list(client, args)
        return 0
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return 1


async def job_attach(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        await client.start_monitoring(args.job_id, monitor=True)
        job = await client.get_job_status(args.job_id)
        if job.status in ["completed", "failed", "canceled"]:
            print(f"Job {args.job_id} already finished.")
            return 0
        return await stream_and_wait(client, args.job_id, job.transport, job.transport_metadata)
    except Exception as e:
        logger.error(f"Error attaching to job: {e}")
        return 1


async def job_cancel(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        await client.cancel_job(args.job_id)
        print(f"Job {colorize(args.job_id, Colors.CYAN)} cancellation requested.")
        return 0
    except Exception as e:
        logger.error(f"Error canceling job: {e}")
        return 1


async def job_logs(client: DFFmpegClient, args: argparse.Namespace) -> int:
    try:
        last_msg_id = None
        while True:
            resp = await client.get_job_logs(args.job_id, since_message_id=str(last_msg_id) if last_msg_id else None)
            logs = sorted(resp.logs, key=lambda log: (log.timestamp if log.timestamp else 0))

            for log in logs:
                stream = sys.stdout if log.stream == "stdout" else sys.stderr
                print(log.content, file=stream)
                stream.flush()

            if resp.last_message_id:
                last_msg_id = resp.last_message_id

            if not args.follow:
                if not resp.logs:
                    break
                continue

            job = await client.get_job_status(args.job_id)
            if job.status in ["completed", "failed", "canceled"]:
                break

            if not resp.logs:
                await asyncio.sleep(2)

        # Final check
        resp = await client.get_job_logs(args.job_id, since_message_id=str(last_msg_id) if last_msg_id else None)
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
    add_config_arg(parser)

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit
    submit_parser = setup_subcommand(subparsers, "submit", "Submit a job", func=job_submit)
    submit_parser.add_argument("--binary", "-b", default="ffmpeg", help="Binary name (default: ffmpeg)")
    submit_parser.add_argument(
        "--detach", "-D", action="store_true", help="Submit job in background and exit immediately"
    )
    submit_parser.add_argument(
        "--heartbeat-interval", type=int, help="Override heartbeat interval (seconds) for this job"
    )
    submit_parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the binary")

    # Status
    status_parser = setup_subcommand(subparsers, "status", "Get cluster status", func=status_cmd)
    add_window_arg(status_parser)

    # Worker
    add_worker_subcommand(subparsers, list_func=worker_list, show_func=worker_show)

    # Job
    job_subparsers = add_job_subcommand(
        subparsers,
        list_func=job_list,
        show_func=job_show,
        logs_func=job_logs,
        include_logs_follow=True,
    )

    j_cancel = setup_subcommand(job_subparsers, "cancel", "Cancel a job", func=job_cancel)
    add_job_id_arg(j_cancel)

    j_attach = setup_subcommand(job_subparsers, "attach", "Attach to an existing job", func=job_attach)
    add_job_id_arg(j_attach)

    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    async def run():
        async with DFFmpegClient(config) as client:
            if hasattr(args, "func"):
                return await args.func(client, args)
            return 0

    try:
        sys.exit(asyncio.run(run()))
    except KeyboardInterrupt:
        sys.exit(130)


def proxy_main():
    """
    Entry point for proxy scripts (e.g. 'ffmpeg').
    """
    binary_name: str = os.path.basename(sys.argv[0])
    job_args = sys.argv[1:]

    # Construct args to mimic 'submit' command
    # We can reuse job_submit logic but we need to mock the args namespace
    # Or just call run_submit logic directly if we kept it separate.
    # But now job_submit expects (client, args).

    # Let's reconstruct a namespace
    args = argparse.Namespace(
        binary=binary_name,
        arguments=job_args,
        detach=False,
        heartbeat_interval=None,
        config=None,
    )

    try:
        config = load_config(None)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    async def run():
        async with DFFmpegClient(config) as client:
            return await job_submit(client, args)

    try:
        sys.exit(asyncio.run(run()))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
