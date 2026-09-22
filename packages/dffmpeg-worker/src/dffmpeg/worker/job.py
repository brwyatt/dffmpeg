import asyncio
import logging
from typing import Any, Callable, Dict, Optional

from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.loop_utils import heartbeat_loop
from dffmpeg.common.models import (
    JobLogsPayload,
    JobStatusUpdate,
    JobStatusUpdateStatus,
    LogEntry,
)
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.executor import JobExecutor

logger = logging.getLogger(__name__)

# Custom sleep helper to allow surgical unit test mocking without global side-effects
_sleep = asyncio.sleep


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
        job_payload: Dict[str, Any],
        cleanup_callback: Callable[[ULID], Any],
        executor: JobExecutor,
    ):
        self.config = config
        self.client = client
        self.job_id = job_id
        self.payload = job_payload
        self.cleanup_callback = cleanup_callback
        self.executor = executor
        self.client_id = config.client_id

        self._main_task: Optional[asyncio.Task[None]] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._log_flusher_task: Optional[asyncio.Task[None]] = None
        self._log_queue: asyncio.Queue[LogEntry] = asyncio.Queue()
        self._flush_lock = asyncio.Lock()
        self._new_log_event = asyncio.Event()
        self._binary_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=16)
        self._binary_uploader_task: Optional[asyncio.Task[None]] = None
        self._binary_mode_active: bool = False

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
        self._running: bool = False
        self._executor_done: bool = False

    async def start(self):
        """Starts the job execution."""
        self._running = True
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

        async def _action():
            resp = await self.client.post(path)
            logger.debug(f"[{self.client_id}] Sent heartbeat for {self.job_id}")
            if resp.status_code != 200:
                logger.warning(f"Job heartbeat failed for {self.job_id}: {resp.status_code} - {resp.text}")
                resp.raise_for_status()

        await heartbeat_loop(
            name=f"job heartbeat ({self.job_id})",
            action=_action,
            is_running=lambda: getattr(self, "_running", False),
            interval=float(interval),
            jitter_bound=jitter_bound,
            first_immediate=False,
        )

    async def _send_log(self, entry: LogEntry):
        """
        Sends a log entry to the coordinator by putting it in the batching queue.

        Args:
            entry (LogEntry): The log entry to send.
        """
        self._log_queue.put_nowait(entry)
        self._new_log_event.set()

    async def _flush_logs(self):
        """
        Drains the log queue into the buffer and sends the accumulated batch.
        """
        async with self._flush_lock:
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
            except asyncio.CancelledError as ce:
                raise ce
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
        while self._running:
            try:
                # Wait indefinitely for the first log to trigger the window
                await self._new_log_event.wait()
                self._new_log_event.clear()

                # Start the collection window
                now = asyncio.get_event_loop().time()
                end_time = now + self.config.log_batch_delay
                while self._log_queue.qsize() < self.config.log_batch_size and now < end_time:
                    try:
                        await asyncio.wait_for(self._new_log_event.wait(), timeout=end_time - now)
                        self._new_log_event.clear()
                    except asyncio.TimeoutError:
                        pass
                    now = asyncio.get_event_loop().time()

                # Flush the accumulated buffer (and anything else that arrived in the meantime)
                await self._flush_logs()

            except asyncio.CancelledError as ce:
                await self._flush_logs()
                raise ce
            except Exception as e:
                logger.error(f"Log flusher error for job {self.job_id}: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _binary_uploader(self):
        """
        Drains raw bytes from the queue, clusters them into larger blocks for network efficiency,
        and posts them sequentially to the Coordinator's chunk streaming endpoint.
        """
        seq = 0
        total_bytes = 0

        try:
            while self._running and (not self._executor_done or not self._binary_queue.empty()):
                try:
                    # Non-blocking check or short wait for the next chunk
                    chunk = await asyncio.wait_for(self._binary_queue.get(), timeout=0.2)
                except asyncio.TimeoutError:
                    continue

                # Automatically cluster small micro-chunks into larger ones (up to 256KB) for I/O efficiency
                chunk_data = bytearray(chunk)
                while not self._binary_queue.empty() and len(chunk_data) < 256 * 1024:
                    chunk_data.extend(self._binary_queue.get_nowait())
                    self._binary_queue.task_done()

                data_to_send = bytes(chunk_data)
                self._binary_queue.task_done()

                path = f"/jobs/{self.job_id}/streams/stdout/chunks?seq={seq}"

                attempts = 5
                success = False
                for i in range(attempts):
                    try:
                        await self.client.post_binary(path, content=data_to_send)
                        success = True
                        break
                    except Exception as e:
                        if i == attempts - 1:
                            logger.critical(f"Failed to upload binary chunk {seq} after {attempts} attempts: {e}")
                            break
                        wait_time = min(15, 2**i)
                        await _sleep(wait_time)

                if not success:
                    raise RuntimeError("Persistent network failure while uploading stream chunks")

                total_bytes += len(data_to_send)
                seq += 1

            # Send EOF when queue is fully drained and subprocess terminates
            if seq > 0:
                logger.info(f"Drained binary stream queue. Sending EOF with {seq} chunks, {total_bytes} bytes.")
                path = f"/jobs/{self.job_id}/streams/stdout/eof"
                payload = {"final_sequence": seq - 1, "total_bytes": total_bytes}
                await self.client.post(path, json=payload)
            elif self._binary_mode_active:
                logger.info("Binary stream was active but produced 0 bytes. Sending empty EOF.")
                path = f"/jobs/{self.job_id}/streams/stdout/eof"
                payload = {"final_sequence": None, "total_bytes": 0}
                await self.client.post(path, json=payload)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error in binary uploader for job {self.job_id}: {e}", exc_info=True)
            # Re-raise so that the parent task can detect uploader crash and terminate executor/subprocess
            raise e

    async def _do_work(self) -> int:
        """
        Executes the actual job work.
        """
        # Launch binary chunk uploader background task
        self._binary_uploader_task = asyncio.create_task(self._binary_uploader())

        async def _on_binary_data(data: bytes):
            self._binary_mode_active = True
            await self._binary_queue.put(data)

        async def run_executor():
            try:
                return await self.executor.execute(self._send_log, _on_binary_data)
            except TypeError as te:
                if "positional argument" in str(te) or "unexpected keyword" in str(te):
                    return await self.executor.execute(self._send_log)
                raise

        executor_task = asyncio.create_task(run_executor())

        try:
            done, _pending = await asyncio.wait(
                [executor_task, self._binary_uploader_task], return_when=asyncio.FIRST_COMPLETED
            )

            # If the binary uploader crashed first, fail fast and raise its exception
            if self._binary_uploader_task in done:
                exc = self._binary_uploader_task.exception()
                if exc:
                    logger.error(f"Uploader task failed with exception: {exc}")
                    executor_task.cancel()
                    raise exc

            # Otherwise, wait for the executor task to complete
            ret = await executor_task
            self._executor_done = True

            # Wait for uploader to cleanly finish draining any leftover chunks and send EOF
            if self._binary_uploader_task and not self._binary_uploader_task.done():
                await self._binary_uploader_task

            return ret
        except Exception as e:
            logger.error(f"Job execution failed or uploader crashed: {e}")
            self._executor_done = True
            # Cancel uploader if still running
            if self._binary_uploader_task and not self._binary_uploader_task.done():
                self._binary_uploader_task.cancel()
            # Cancel executor if still running to cleanly terminate child subprocess
            if not executor_task.done():
                executor_task.cancel()
            raise

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
            self._running = False

            # Cancel background tasks first to stop them from adding/sending more
            if self._heartbeat_task:
                self._heartbeat_task.cancel()

            if self._log_flusher_task:
                self._log_flusher_task.cancel()

            try:
                # Ensure any remaining logs in the queue or buffer are sent immediately
                await self._flush_logs()
            except (Exception, asyncio.CancelledError) as e:
                logger.error(f"[{self.client_id}] Error during job {self.job_id} final log flush: {e}", exc_info=True)

            # Now await the cancelled tasks to ensure they terminate cleanly
            if self._heartbeat_task:
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass

            if self._log_flusher_task:
                try:
                    await self._log_flusher_task
                except asyncio.CancelledError:
                    pass

            if self._binary_uploader_task:
                try:
                    await self._binary_uploader_task
                except asyncio.CancelledError:
                    pass

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
