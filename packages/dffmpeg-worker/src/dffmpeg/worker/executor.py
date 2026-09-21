import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional, Protocol

from dffmpeg.common.models import LogEntry
from dffmpeg.common.paths import resolve_arguments, resolve_path

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

        async def read_stderr(stream):
            try:
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    decoded_line = line.decode("utf-8", errors="replace")
                    await log_callback(
                        LogEntry(
                            stream="stderr",
                            content=decoded_line.rstrip("\r\n"),
                            timestamp=datetime.now(timezone.utc),
                        )
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error reading from stderr stream, but process continues: {e}")

        async def read_stdout(stream):
            binary_mode = False
            chunk_limit = 64 * 1024  # 64KB line length limit before latching

            try:
                while True:
                    if binary_mode:
                        # Binary Phase: high-throughput direct raw read
                        data = await stream.read(256 * 1024)
                        if not data:
                            break
                        if binary_callback:
                            await binary_callback(data)
                    else:
                        # Text Phase: readline-based validation (perfect backward compatibility)
                        line = await stream.readline()
                        if not line:
                            break

                        # Trigger binary mode if:
                        # 1. Null byte in line
                        # 2. Line exceeds 64KB
                        # 3. Invalid UTF-8 sequence
                        if b"\x00" in line or len(line) > chunk_limit:
                            binary_mode = True
                        else:
                            try:
                                decoded_line = line.decode("utf-8")
                                await log_callback(
                                    LogEntry(
                                        stream="stdout",
                                        content=decoded_line.rstrip("\r\n"),
                                        timestamp=datetime.now(timezone.utc),
                                    )
                                )
                            except UnicodeDecodeError:
                                binary_mode = True

                        if binary_mode:
                            if binary_callback:
                                await binary_callback(line)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error reading from stdout stream, but process continues: {e}")

        try:
            await asyncio.gather(
                read_stdout(process.stdout),
                read_stderr(process.stderr),
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
