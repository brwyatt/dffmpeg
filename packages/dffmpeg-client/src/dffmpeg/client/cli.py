import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, cast

from dffmpeg.client.api import DFFmpegClient
from dffmpeg.client.config import load_config
from dffmpeg.common.models import JobLogsMessage, JobStatusMessage, SupportedBinaries

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
                    exit_code = 0
                    break
                elif status == "failed":
                    exit_code = 1
                    break
                elif status == "canceled":
                    exit_code = 130  # Standard SIGINT exit code
                    break

    except asyncio.CancelledError:
        # If we are cancelled locally (Ctrl+C), try to cancel remote job
        print("\nCanceling job...", file=sys.stderr)
        try:
            await client.cancel_job(job_id)
        except Exception as e:
            print(f"Failed to cancel job: {e}", file=sys.stderr)
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
        # Only process absolute paths (start with /).
        # Skip flags and relative paths.
        if not arg.startswith("/"):
            processed_args.append(arg)
            continue

        # Try to resolve to absolute path for matching
        try:
            # We use abspath to avoid resolving symlinks unless necessary?
            # User config likely uses physical paths.
            # resolve() handles symlinks and '..'
            abs_arg = str(Path(arg).resolve())
        except Exception:
            abs_arg = str(Path(arg).absolute())

        replaced = False
        for var_name, local_path in sorted_paths:
            # Check if abs_arg starts with local_path
            if abs_arg.startswith(local_path):
                # Boundary check: ensure match is on directory boundary
                # either exact match, or next char is separator
                remainder = abs_arg[len(local_path) :]
                if not remainder or remainder.startswith(os.sep):
                    # Match!
                    # Replace prefix with $Var
                    # Note: We must return the remainder with the variable
                    new_arg = f"${var_name}{remainder}"
                    processed_args.append(new_arg)
                    used_paths.add(var_name)
                    replaced = True
                    break

        if not replaced:
            processed_args.append(arg)

    return processed_args, list(used_paths)


async def run_submit(binary_name: SupportedBinaries, raw_args: List[str], wait: bool, config_file: str | None = None):
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    # Process arguments to handle path mapping
    job_args, paths = process_arguments(raw_args, config.paths)

    async with DFFmpegClient(config) as client:
        try:
            job_id, transport, metadata = await client.submit_job(binary_name, job_args, paths)

            if not wait:
                print("Job submitted successfully.")
                print(f"Job ID: {job_id}")
                return 0

            # Wait mode
            return await stream_and_wait(client, job_id, transport, metadata)

        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return 1


async def run_status(job_id: str | None, config_file: str | None = None):
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            if not job_id:
                # TODO: Implement list jobs when API supports it
                print("Listing jobs is not yet implemented.")
                return 1

            status = await client.get_job_status(job_id)
            # Pretty print status
            print(f"Job ID: {status['job_id']}")
            print(f"Status: {status['status']}")
            print(f"Worker: {status.get('worker_id', '<Unassigned>')}")
            # print(status)
            return 0
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return 1


async def run_cancel(job_id: str, config_file: str | None = None):
    try:
        config = load_config(config_file)
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return 1

    async with DFFmpegClient(config) as client:
        try:
            await client.cancel_job(job_id)
            print(f"Job {job_id} cancellation requested.")
            return 0
        except Exception as e:
            logger.error(f"Error canceling job: {e}")
            return 1


def main():
    parser = argparse.ArgumentParser(description="dffmpeg client CLI")
    parser.add_argument("--config", "-c", help="Path to config file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit
    submit_parser = subparsers.add_parser("submit", help="Submit a job")
    submit_parser.add_argument("--binary", "-b", default="ffmpeg", help="Binary name (default: ffmpeg)")
    submit_parser.add_argument("--wait", "-w", action="store_true", help="Wait for job completion and stream logs")
    submit_parser.add_argument("arguments", nargs=argparse.REMAINDER, help="Arguments for the binary")

    # Status
    status_parser = subparsers.add_parser("status", help="Get job status")
    status_parser.add_argument("job_id", nargs="?", help="Job ID (optional, lists jobs if omitted)")

    # Cancel
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a job")
    cancel_parser.add_argument("job_id", help="Job ID")

    args = parser.parse_args()

    try:
        if args.command == "submit":
            # Strip '--' if present in arguments
            job_args = args.arguments
            if job_args and job_args[0] == "--":
                job_args = job_args[1:]

            sys.exit(asyncio.run(run_submit(args.binary, job_args, args.wait, args.config)))

        elif args.command == "status":
            sys.exit(asyncio.run(run_status(args.job_id, args.config)))

        elif args.command == "cancel":
            sys.exit(asyncio.run(run_cancel(args.job_id, args.config)))

    except KeyboardInterrupt:
        sys.exit(130)


def proxy_main():
    """
    Entry point for proxy scripts (e.g. 'ffmpeg').
    Behaves as if 'submit --wait' was called with the script name as binary.
    """
    binary_name: SupportedBinaries = cast(SupportedBinaries, os.path.basename(sys.argv[0]))
    job_args = sys.argv[1:]

    try:
        sys.exit(asyncio.run(run_submit(binary_name, job_args, wait=True)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
