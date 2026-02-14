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
        self._log_flusher_task: Optional[asyncio.Task] = None
        self._log_queue: asyncio.Queue[LogEntry] = asyncio.Queue()

        self.coordinator_paths = {
            "heartbeat": f"/jobs/{self.job_id}/worker_heartbeat",
            "accept": f"/jobs/{self.job_id}/accept",
            "logs": f"/jobs/{self.job_id}/logs",
            "status": f"/jobs/{self.job_id}/status",
        }

        self._log_buffer: list[LogEntry] = []
        self._last_status: Optional[str] = None
        self._silent_cancellation: bool = False
        self._fast_shutdown: bool = False

    async def start(self):
        """Starts the job execution."""
        logger.info(f"[{self.client_id}] Starting job {self.job_id}")
        self._log_flusher_task = asyncio.create_task(self._log_flusher())
        self._main_task = asyncio.create_task(self._run())

    async def cancel(self, fast_shutdown: bool = False):
        """Cancels the job execution."""
        logger.info(f"[{self.client_id}] Canceling job {self.job_id} (fast_shutdown={fast_shutdown})")
        self._silent_cancellation = False
        self._fast_shutdown = fast_shutdown
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
        Sends a log entry to the coordinator by putting it in the batching queue.

        Args:
            entry (LogEntry): The log entry to send.
        """
        self._log_queue.put_nowait(entry)

    async def _flush_logs(self):
        """
        Drains the log queue into the buffer and sends the accumulated batch.
        """
        # Drains the queue into the buffer
        while not self._log_queue.empty():
            self._log_buffer.append(self._log_queue.get_nowait())

        if not self._log_buffer:
            return

        # Try to flush buffer
        logs_payload = JobLogsPayload(logs=self._log_buffer)
        path = self.coordinator_paths["logs"]
        body = logs_payload.model_dump(mode="json", exclude_none=True)

        try:
            await self.client.post(path, json=body)
            # If successful, clear the buffer
            self._log_buffer.clear()
        except Exception as e:
            logger.warning(f"Failed to send {len(self._log_buffer)} logs: {e}")
            # Keep logs in buffer for next attempt (up to batch size)
            if len(self._log_buffer) > self.config.log_batch_size:
                logger.error(f"Log buffer overflow for job {self.job_id}, dropping oldest logs.")
                self._log_buffer = self._log_buffer[-self.config.log_batch_size :]

    async def _log_flusher(self):
        """
        Background task to periodically flush logs using a time-based window.
        """
        while True:
            try:
                # Wait indefinitely for the first log to trigger the window
                entry = await self._log_queue.get()
                self._log_buffer.append(entry)

                # Start the collection window
                start_time = asyncio.get_event_loop().time()
                while len(self._log_buffer) < self.config.log_batch_size:
                    now = asyncio.get_event_loop().time()
                    remaining = self.config.log_batch_delay - (now - start_time)

                    if remaining <= 0:
                        break

                    try:
                        entry = await asyncio.wait_for(self._log_queue.get(), timeout=remaining)
                        self._log_buffer.append(entry)
                    except asyncio.TimeoutError:
                        # Window expired
                        break

                # Flush the accumulated buffer (and anything else that arrived in the meantime)
                await self._flush_logs()

            except asyncio.CancelledError:
                await self._flush_logs()
                break
            except Exception as e:
                logger.error(f"Log flusher error for job {self.job_id}: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _do_work(self) -> int:
        """
        Executes the actual job work.
        """
        return await self.executor.execute(self._send_log)

    async def _run(self):
        """Main execution flow for the job."""
        exit_code = None
        try:
            # 1. Accept the job
            logger.info(f"[{self.client_id}] Accepting job {self.job_id}")
            path = self.coordinator_paths["accept"]
            await self.client.post(path)

            # 2. Start heartbeat
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            # 3. Execute
            exit_code = await self._do_work()

            # 4. Report Success/Failure based on exit code
            await self._flush_logs()
            if exit_code == 0:
                logger.info(f"[{self.client_id}] Job {self.job_id} completed successfully")
                await self._report_status("completed", exit_code=exit_code)
            else:
                logger.error(f"[{self.client_id}] Job {self.job_id} failed with exit code {exit_code}")
                await self._report_status("failed", exit_code=exit_code)

        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Job {self.job_id} execution canceled")
            await self._flush_logs()
            if not self._silent_cancellation:
                retries = 0 if self._fast_shutdown else 5
                await self._report_status("canceled", retries=retries)
            raise

        except Exception as e:
            logger.error(f"[{self.client_id}] Job {self.job_id} failed: {e}", exc_info=True)
            await self._report_status("failed", exit_code=exit_code)

        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

            # Final log flush
            if self._log_flusher_task:
                self._log_flusher_task.cancel()
                try:
                    await self._log_flusher_task
                except asyncio.CancelledError:
                    pass

            # Ensure any remaining logs in the queue or buffer are sent
            await self._flush_logs()

            # client is owned by Worker, do not close here
            self.cleanup_callback(self.job_id)

    async def _report_status(
        self,
        status: JobStatusUpdateStatus,
        exit_code: Optional[int] = None,
        retries: int = 5,
    ):
        """
        Reports final status to coordinator.

        Args:
            status (str): The final status of the job (e.g., "completed", "failed", "canceled").
            exit_code (Optional[int]): The process exit code, if applicable.
            retries (int): Number of retry attempts.
        """
        self._last_status = status
        payload_model = JobStatusUpdate(status=status, exit_code=exit_code)
        path = self.coordinator_paths["status"]
        body = payload_model.model_dump(mode="json")

        # Try at least once, plus retries
        attempts = max(1, retries + 1)

        for i in range(attempts):
            try:
                await self.client.post(path, json=body)
                return
            except Exception as e:
                # If this was the last attempt, log and break
                if i == attempts - 1:
                    break

                wait_time = min(30, 2**i)
                logger.error(
                    f"[{self.client_id}] Failed to report status {status} for {self.job_id}: {e}. "
                    f"Retrying in {wait_time}s ({i + 1}/{attempts})..."
                )
                await asyncio.sleep(wait_time)

        logger.critical(f"[{self.client_id}] Could not report status {status} for {self.job_id} after retries.")
