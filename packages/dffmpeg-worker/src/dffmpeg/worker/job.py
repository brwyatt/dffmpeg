import asyncio
import logging
import random
from typing import Callable, Dict, Optional

from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import (
    JobLogsPayload,
    JobStatusUpdate,
    JobStatusUpdateStatus,
    LogEntry,
)
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.executor import JobExecutor

logger = logging.getLogger(__name__)


class JobRunner:
    """
    Manages the execution of a single assigned job.
    Includes heartbeats, log streaming, and status reporting.
    """

    def __init__(
        self,
        config: WorkerConfig,
        client: AuthenticatedAsyncClient,
        job_id: ULID,
        job_payload: Dict,
        cleanup_callback: Callable,
        executor: JobExecutor,
    ):
        self.config = config
        self.client = client
        self.job_id = job_id
        self.payload = job_payload
        self.cleanup_callback = cleanup_callback
        self.executor = executor
        self.client_id = config.client_id

        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        self.coordinator_paths = {
            "heartbeat": f"/jobs/{self.job_id}/heartbeat",
            "accept": f"/jobs/{self.job_id}/accept",
            "logs": f"/jobs/{self.job_id}/logs",
            "status": f"/jobs/{self.job_id}/status",
        }

        self._log_buffer: list[LogEntry] = []
        self._last_status: Optional[str] = None
        self._silent_cancellation: bool = False

    async def start(self):
        """Starts the job execution."""
        logger.info(f"[{self.client_id}] Starting job {self.job_id}")
        self._main_task = asyncio.create_task(self._run())

    async def cancel(self):
        """Cancels the job execution."""
        logger.info(f"[{self.client_id}] Canceling job {self.job_id}")
        self._silent_cancellation = False
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

        # Don't call _report_status("canceled") here, let the _run loop handle cleanup
        # actually, if main task is cancelled, _run's finally block or exception handler should catch it.

    async def abort(self):
        """Aborts the job execution without reporting status."""
        logger.info(f"[{self.client_id}] Aborting job {self.job_id}")
        self._silent_cancellation = True
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self):
        """Sends periodic heartbeats to the coordinator."""
        path = self.coordinator_paths["heartbeat"]
        interval = self.payload.get("heartbeat_interval", 5)
        jitter_bound = min(0.5 * interval, self.config.jitter)
        while True:
            try:
                jitter = random.uniform(-jitter_bound, jitter_bound)
                await asyncio.sleep(max(1, interval + jitter))

                await self.client.post(path)

                logger.debug(f"[{self.client_id}] Sent heartbeat for {self.job_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[{self.client_id}] Heartbeat failed for {self.job_id}: {e}")

    async def _send_log(self, entry: LogEntry):
        """
        Sends a log entry to the coordinator.

        Args:
            entry (LogEntry): The log entry to send.
        """
        self._log_buffer.append(entry)

        # Try to flush buffer
        logs_payload = JobLogsPayload(logs=self._log_buffer)
        path = self.coordinator_paths["logs"]
        body = logs_payload.model_dump(mode="json", exclude_none=True)

        try:
            await self.client.post(path, json=body)
            # If successful, clear buffer
            self._log_buffer.clear()
        except Exception as e:
            logger.warning(f"Failed to send logs: {e}. Buffered {len(self._log_buffer)} entries.")

    async def _do_work(self):
        """
        Executes the actual job work.
        """
        await self.executor.execute(self._send_log)

    async def _run(self):
        """Main execution flow for the job."""
        try:
            # 1. Accept the job
            logger.info(f"[{self.client_id}] Accepting job {self.job_id}")
            path = self.coordinator_paths["accept"]
            await self.client.post(path)

            # 2. Start heartbeat
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            # 3. Execute
            await self._do_work()

            # 4. Report Success
            logger.info(f"[{self.client_id}] Job {self.job_id} completed successfully")
            await self._report_status("completed")

        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Job {self.job_id} execution canceled")
            if not self._silent_cancellation:
                await self._report_status("canceled")
            raise

        except Exception as e:
            logger.error(f"[{self.client_id}] Job {self.job_id} failed: {e}", exc_info=True)
            await self._report_status("failed")

        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            # client is owned by Worker, do not close here
            self.cleanup_callback(self.job_id)

    async def _report_status(self, status: JobStatusUpdateStatus):
        """
        Reports final status to coordinator.

        Args:
            status (str): The final status of the job (e.g., "completed", "failed", "canceled").
        """
        self._last_status = status
        payload_model = JobStatusUpdate(status=status)
        path = self.coordinator_paths["status"]
        body = payload_model.model_dump(mode="json")

        for i in range(5):
            try:
                await self.client.post(path, json=body)
                return
            except Exception as e:
                wait_time = min(30, 2**i)
                logger.error(
                    f"[{self.client_id}] Failed to report status {status} for {self.job_id}: {e}. "
                    f"Retrying in {wait_time}s ({i + 1}/5)..."
                )
                await asyncio.sleep(wait_time)

        logger.critical(f"[{self.client_id}] Could not report status {status} for {self.job_id} after retries.")
