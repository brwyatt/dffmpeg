import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Protocol

from dffmpeg.common.models import LogEntry

logger = logging.getLogger(__name__)


class JobExecutor(Protocol):
    """
    Protocol for job executors.
    """

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
    ) -> int:
        """
        Executes a job.

        Args:
            log_callback (Callable): A callback to handle log entries.
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
    ):
        self.job_id = job_id
        self.binary_path = binary_path
        self.raw_arguments = arguments
        self.path_map = path_map
        self.resolved_arguments = self._resolve_arguments()

    def _resolve_arguments(self) -> List[str]:
        """
        Resolves arguments by substituting path variables.
        """
        resolved = []
        for arg in self.raw_arguments:
            if arg.startswith("$"):
                # Extract variable name (up to first / or end of string)
                # Example: $Movies/file.mkv -> variable=Movies, suffix=/file.mkv
                parts = arg.split("/", 1)
                variable_with_prefix = parts[0]
                variable = variable_with_prefix[1:]  # Strip $

                if variable in self.path_map:
                    base_path = self.path_map[variable]
                    suffix = ("/" + parts[1]) if len(parts) > 1 else ""
                    # Ensure we don't end up with double slashes if base_path ends with /
                    if base_path.endswith("/") and suffix.startswith("/"):
                        resolved_arg = base_path + suffix[1:]
                    else:
                        resolved_arg = base_path + suffix
                    resolved.append(resolved_arg)
                    continue
                else:
                    # If variable not found, leave as is
                    # Given the pre-validation in worker, this shouldn't happen for known paths.
                    # For now, we assume pre-validation caught missing required paths.
                    logger.warning(f"Variable {variable} not found in path map for argument {arg}. Leaving as is.")
                    resolved.append(arg)
            else:
                resolved.append(arg)
        return resolved

    async def execute(
        self,
        log_callback: Callable[[LogEntry], Awaitable[None]],
    ) -> int:
        """
        Executes the subprocess.
        """
        logger.info(f"Executing command: {self.binary_path} {' '.join(self.resolved_arguments)}")

        process = await asyncio.create_subprocess_exec(
            self.binary_path,
            *self.resolved_arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def read_stream(stream, stream_name):
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded_line = line.decode()
                if decoded_line:
                    await log_callback(
                        LogEntry(
                            stream=stream_name,
                            content=decoded_line.rstrip("\r\n"),
                            timestamp=datetime.now(timezone.utc),
                        )
                    )

        try:
            await asyncio.gather(
                read_stream(process.stdout, "stdout"),
                read_stream(process.stderr, "stderr"),
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
