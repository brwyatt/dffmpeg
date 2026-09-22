import asyncio
import codecs
import logging
import re
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Literal, Optional, Protocol

from dffmpeg.common.models import LogEnding, LogEntry
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

        DELIMITER_RE = re.compile(rb"\r\n|\r|\n")

        async def parse_buffer_and_emit(buffer: bytearray, is_eof: bool, stream_name: Literal["stdout", "stderr"]):
            chunk_limit = 64 * 1024
            while True:
                match = DELIMITER_RE.search(buffer)
                if match:
                    # Check Split-Packet \r Boundary: Standalone \r at the very end of the packet,
                    # wait for more packets in case it is \r\n (CRLF).
                    if not is_eof and match.group() == b"\r" and match.end() == len(buffer):
                        break

                    line_bytes = buffer[: match.start()]
                    delim = match.group()

                    if delim == b"\r\n":
                        ending = LogEnding.CRLF
                    elif delim == b"\n":
                        ending = LogEnding.LF
                    else:
                        ending = LogEnding.CR

                    content_str = line_bytes.decode("utf-8", errors="replace")
                    await log_callback(
                        LogEntry(
                            stream=stream_name,
                            content=content_str,
                            ending=ending,
                            timestamp=datetime.now(timezone.utc),
                        )
                    )
                    del buffer[: match.end()]
                else:
                    # No delimiter matches found. Handle lines exceeding 64KB limit
                    if len(buffer) >= chunk_limit:
                        chunk_bytes = buffer[:chunk_limit]
                        chunk_str = chunk_bytes.decode("utf-8", errors="replace")
                        await log_callback(
                            LogEntry(
                                stream=stream_name,
                                content=chunk_str,
                                ending=LogEnding.NONE,
                                timestamp=datetime.now(timezone.utc),
                            )
                        )
                        del buffer[:chunk_limit]
                        continue

                    # Handle EOF residual buffer
                    if is_eof and buffer:
                        chunk_str = buffer.decode("utf-8", errors="replace")
                        await log_callback(
                            LogEntry(
                                stream=stream_name,
                                content=chunk_str,
                                ending=LogEnding.NONE,
                                timestamp=datetime.now(timezone.utc),
                            )
                        )
                        buffer.clear()
                    break

        async def read_stderr(stream):
            buffer = bytearray()
            try:
                while True:
                    data = await stream.read(4096)
                    if not data:
                        await parse_buffer_and_emit(buffer, is_eof=True, stream_name="stderr")
                        break
                    buffer.extend(data)
                    await parse_buffer_and_emit(buffer, is_eof=False, stream_name="stderr")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Error reading from stderr stream, but process continues: {e}")

        async def read_stdout(stream):
            binary_mode = False
            buffer = bytearray()
            chunk_limit = 64 * 1024  # 64KB line length limit before latching
            utf8_decoder = codecs.getincrementaldecoder("utf-8")()

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
                        # Text Phase: 4KB chunk-based buffer (eliminates LimitOverrunError deadlocks)
                        data = await stream.read(4096)
                        if not data:
                            # Process residual buffer
                            if buffer:
                                if b"\x00" in buffer:
                                    if binary_callback:
                                        await binary_callback(bytes(buffer))
                                else:
                                    await parse_buffer_and_emit(buffer, is_eof=True, stream_name="stdout")
                            break

                        if b"\x00" in data:
                            binary_mode = True
                        else:
                            try:
                                utf8_decoder.decode(data, final=False)
                            except UnicodeDecodeError:
                                binary_mode = True

                        buffer.extend(data)

                        if not binary_mode:
                            # Process complete lines via universal regex parser
                            await parse_buffer_and_emit(buffer, is_eof=False, stream_name="stdout")

                        # If binary mode was triggered or 64KB buffer limit exceeded
                        if binary_mode or len(buffer) > chunk_limit:
                            binary_mode = True
                            if binary_callback and buffer:
                                await binary_callback(bytes(buffer))
                            buffer.clear()
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
