import asyncio
import logging
from typing import Awaitable, Callable, Protocol

from dffmpeg.common.models import LogEntry

logger = logging.getLogger(__name__)


class JobExecutor(Protocol):
    """
    Protocol for job executors.
    """

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
    ) -> None:
        """
        Executes a job.

        Args:
            log_callback (Callable): A callback to handle log entries.
        """
        ...


class SimulatedJobExecutor:
    """
    Executor that simulates a job execution with sleep statements.
    """

    def __init__(self, job_id: str):
        self.job_id = job_id

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
    ) -> None:
        """
        Executes the simulated work.

        Args:
            log_callback (Callable): A callback to handle log entries.
        """
        logger.info(f"Simulating work for job {self.job_id}")

        for i in range(10):
            await asyncio.sleep(1)
            log = LogEntry(stream="stdout", content=f"Processing frame {i * 100}...")
            await log_callback(log)
