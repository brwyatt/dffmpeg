import argparse
import asyncio
import logging
import signal

from dffmpeg.common.cli_utils import add_config_arg
from dffmpeg.worker.config import load_config
from dffmpeg.worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_worker(config):
    worker = Worker(config)

    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        worker_task = asyncio.create_task(worker.start())

        # Wait for stop signal
        await stop_event.wait()

        # Shutdown
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
    args = parser.parse_args()

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
