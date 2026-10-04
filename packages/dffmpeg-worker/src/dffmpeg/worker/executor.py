import asyncio
import logging
from typing import Awaitable, Callable, Dict, List, Optional, Protocol

from dffmpeg.common.models import LogEntry
from dffmpeg.common.paths import resolve_arguments, resolve_path
from dffmpeg.common.stdio_stream_handler import StdioHandler

logger = logging.getLogger(__name__)


class JobExecutor(Protocol):
    """
    Protocol for job executors.
    """

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
        binary_callback: Optional[Callable[[bytes], Awaitable[None]]] = None,
    ) -> int:
        """
        Executes a job.

        Args:
            log_callback (Callable): A callback to handle log entries.
            binary_callback (Optional[Callable]): A callback to handle raw binary data.
        """
        ...


class SubprocessJobExecutor:
    """
    Executor that runs a subprocess.
    """

    def __init__(
        self,
        job_id: str,
        binary_path: str,
        arguments: List[str],
        path_map: Dict[str, str],
        working_directory: str | None = None,
    ):
        self.job_id = job_id
        self.binary_path = binary_path
        self.raw_arguments = arguments
        self.path_map = path_map

        self.resolved_arguments = resolve_arguments(arguments, path_map)

        self.working_directory = working_directory
        self.resolved_working_directory = resolve_path(working_directory, path_map) if working_directory else None

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
        binary_callback: Optional[Callable[[bytes], Awaitable[None]]] = None,
    ) -> int:
        """
        Executes the subprocess, using an adaptive classifier on stdout.
        """
        logger.info(f"Executing command: {self.binary_path} {' '.join(self.resolved_arguments)}")

        process = await asyncio.create_subprocess_exec(
            self.binary_path,
            *self.resolved_arguments,
            cwd=self.resolved_working_directory,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        assert process.stdout is not None, "process.stdout must be piped"
        assert process.stderr is not None, "process.stderr must be piped"

        stdout_reader = StdioHandler(
            stream_name="stdout",
            stream=process.stdout,
            log_callback=log_callback,
            binary_callback=binary_callback,
        )
        stderr_reader = StdioHandler(
            stream_name="stderr",
            stream=process.stderr,
            log_callback=log_callback,
            binary_callback=None,
        )

        try:
            await asyncio.gather(
                stdout_reader.read_loop(),
                stderr_reader.read_loop(),
            )

            return_code = await process.wait()

            return return_code

        except asyncio.CancelledError:
            logger.warning(f"Job {self.job_id} canceled, terminating subprocess...")
            raise
        finally:
            if process.returncode is None:
                try:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning(f"Process {process.pid} did not terminate, killing...")
                        process.kill()
                        await process.wait()
                except Exception as e:
                    logger.error(f"Failed to ensure process termination: {e}")
