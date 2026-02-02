import asyncio
import logging
import random
from typing import Dict, Optional

import httpx
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import (
    JobLogsPayload,
    JobStatusUpdate,
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
        signer: RequestSigner,
        job_id: ULID,
        job_payload: Dict,
        cleanup_callback: callable,
        executor: JobExecutor,
    ):
        self.config = config
        self.signer = signer
        self.job_id = job_id
        self.payload = job_payload
        self.cleanup_callback = cleanup_callback
        self.executor = executor
        self.client_id = config.client_id
        self.base_url = (
            f"{config.coordinator.scheme}://{config.coordinator.host}:{config.coordinator.port}"
            f"{'' if config.coordinator.path_base.startswith('/') else '/'}{config.coordinator.path_base}"
        )

        self._main_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._http_client = httpx.AsyncClient(base_url=self.base_url)

        self.coordinator_paths = {
            "heartbeat": f"/jobs/{self.job_id}/heartbeat",
            "accept": f"/jobs/{self.job_id}/accept",
            "logs": f"/jobs/{self.job_id}/logs",
            "status": f"/jobs/{self.job_id}/status",
        }

    async def start(self):
        """Starts the job execution."""
        logger.info(f"[{self.client_id}] Starting job {self.job_id}")
        self._main_task = asyncio.create_task(self._run())

    async def cancel(self):
        """Cancels the job execution."""
        logger.info(f"[{self.client_id}] Canceling job {self.job_id}")
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

        # Don't call _report_status("canceled") here, let the _run loop handle cleanup
        # actually, if main task is cancelled, _run's finally block or exception handler should catch it.

    async def _heartbeat_loop(self):
        """Sends periodic heartbeats to the coordinator."""
        path = self.coordinator_paths["heartbeat"]
        interval = self.payload.get("heartbeat_interval", 5)
        jitter_bound = min(0.5 * interval, self.config.jitter)
        while True:
            try:
                jitter = random.uniform(-jitter_bound, jitter_bound)
                await asyncio.sleep(max(1, interval + jitter))
                
                headers, payload = self.signer.sign_request(self.client_id, "POST", path)
                await self._http_client.request("POST", path, headers=headers, content=payload)
                
                logger.debug(f"[{self.client_id}] Sent heartbeat for {self.job_id}")
            except asyncio.CancelledError:
                break
            except httpx.RequestError as e:
                logger.warning(f"[{self.client_id}] Heartbeat failed for {self.job_id} (network error): {e}")
            except Exception as e:
                logger.warning(f"[{self.client_id}] Heartbeat failed for {self.job_id}: {e}")

    async def _send_log(self, entry: LogEntry):
        """
        Sends a log entry to the coordinator.

        Args:
            entry (LogEntry): The log entry to send.
        """
        logs_payload = JobLogsPayload(logs=[entry])
        path = self.coordinator_paths["logs"]
        body = logs_payload.model_dump(mode="json", exclude_none=True)

        try:
            headers, payload = self.signer.sign_request(self.client_id, "POST", path, body)
            await self._http_client.request("POST", path, headers=headers, content=payload)
        except Exception as e:
            logger.warning(f"Failed to send logs: {e}")

    async def _do_work(self):
        """
        Executes the actual job work.
        """
        await self.executor.execute(str(self.job_id), self._send_log)

    async def _run(self):
        """Main execution flow for the job."""
        try:
            # 1. Accept the job
            logger.info(f"[{self.client_id}] Accepting job {self.job_id}")
            path = self.coordinator_paths["accept"]
            headers, payload = self.signer.sign_request(self.client_id, "POST", path)
            await self._http_client.request("POST", path, headers=headers, content=payload)

            # 2. Start heartbeat
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            # 3. Execute
            await self._do_work()

            # 4. Report Success
            logger.info(f"[{self.client_id}] Job {self.job_id} completed successfully")
            await self._report_status("completed")

        except asyncio.CancelledError:
            logger.info(f"[{self.client_id}] Job {self.job_id} execution canceled")
            await self._report_status("canceled")
            raise

        except Exception as e:
            logger.error(f"[{self.client_id}] Job {self.job_id} failed: {e}", exc_info=True)
            await self._report_status("failed")

        finally:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            await self._http_client.aclose()
            self.cleanup_callback(self.job_id)

    async def _report_status(self, status: str):
        """
        Reports final status to coordinator.

        Args:
            status (str): The final status of the job (e.g., "completed", "failed", "canceled").
        """
        try:
            payload_model = JobStatusUpdate(status=status)
            path = self.coordinator_paths["status"]
            body = payload_model.model_dump(mode="json")
            
            headers, payload = self.signer.sign_request(self.client_id, "POST", path, body)
            await self._http_client.request("POST", path, headers=headers, content=payload)
        except Exception as e:
            logger.error(f"[{self.client_id}] Failed to report status {status} for {self.job_id}: {e}")
