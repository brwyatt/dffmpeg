import argparse
import asyncio
import logging
import signal
import sys

from dffmpeg.common.cli_utils import add_config_arg
from dffmpeg.common.version import get_package_version
from dffmpeg.worker.config import load_config
from dffmpeg.worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker(config):
    worker = Worker(config)

    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    force_stop_event = asyncio.Event()

    def signal_handler():
        if not stop_event.is_set():
            logger.info("Received shutdown signal. Initiating graceful drain. Press Ctrl+C again to force exit.")
            stop_event.set()
        else:
            logger.warning("Second shutdown signal received. Forcing fast shutdown.")
            force_stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        worker_task = asyncio.create_task(worker.start())

        # Wait for stop signal
        await stop_event.wait()

        # Start drain task
        drain_task = asyncio.create_task(worker.drain())
        force_wait_task = asyncio.create_task(force_stop_event.wait())

        done, pending = await asyncio.wait([drain_task, force_wait_task], return_when=asyncio.FIRST_COMPLETED)

        if force_wait_task in done:
            # Force stop requested
            drain_task.cancel()
            await worker.stop()
        else:
            # Drain finished normally
            force_wait_task.cancel()
            # If drain finished because there were no jobs and min drain elapsed,
            # or jobs completed naturally, we still need to stop() to cleanup transport/client.
            await worker.stop()

        # Wait for worker task to complete (it checks _running flag)
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Worker shutdown timed out, cancelling...")
            worker_task.cancel()
        except asyncio.CancelledError:
            pass

    except Exception as e:
        logger.error(f"Worker runtime error: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="dffmpeg Worker")
    add_config_arg(parser)
    parser.add_argument("--version", action="store_true", help="Print version and exit")
    args = parser.parse_args()

    if args.version:
        print(f"dffmpeg-worker {get_package_version('dffmpeg-worker')}")
        sys.exit(0)

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        logger.fatal("Configuration file not found.")
        return
    except Exception as e:
        logger.fatal(f"Failed to load configuration: {e}")
        return

    try:
        asyncio.run(run_worker(config))
    except KeyboardInterrupt:
        pass  # Handled by signal handler, but just in case


if __name__ == "__main__":
    main()
