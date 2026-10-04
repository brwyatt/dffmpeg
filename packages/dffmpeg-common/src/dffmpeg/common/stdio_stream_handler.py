import asyncio
import codecs
import logging
import re
from datetime import datetime, timezone
from typing import Awaitable, Callable, Literal, Optional

from dffmpeg.common.models import LogEnding, LogEntry

logger = logging.getLogger(__name__)


class StdioHandler:
    """
    Unified, high-throughput reader for a subprocess standard stream.
    Implements adaptive text line-buffering (with universal line endings),
    automatic incremental UTF-8 boundary validation, and optional binary latching.
    """

    stream_name: Literal["stdout", "stderr"]
    stream: asyncio.StreamReader

    def __init__(
        self,
        stream_name: Literal["stdout", "stderr"],
        stream: asyncio.StreamReader,
        log_callback: Callable[[LogEntry], Awaitable[None]],
        binary_callback: Optional[Callable[[bytes], Awaitable[None]]] = None,
        chunk_limit: int = 64 * 1024,
    ):
        self.stream_name = stream_name
        self.stream = stream
        self.log_callback = log_callback
        self.binary_callback = binary_callback
        self.chunk_limit = chunk_limit
        self.binary_mode = False
        self.buffer = bytearray()
        self.utf8_decoder = codecs.getincrementaldecoder("utf-8")()
        self._delimiter_re = re.compile(rb"\r\n|\r|\n")

    async def read_loop(self):
        try:
            while True:
                if self.binary_mode:
                    # Binary Phase: high-throughput direct raw read
                    data = await self.stream.read(256 * 1024)
                    if not data:
                        break
                    if self.binary_callback:
                        await self.binary_callback(data)
                else:
                    # Text Phase: 4KB chunk-based buffer
                    data = await self.stream.read(4096)
                    if not data:
                        # Process residual buffer
                        if self.buffer:
                            await self.parse_buffer_and_emit(is_eof=True)
                        break

                    # Binary latch checks: trigger on null bytes or invalid UTF-8 (only if binary callback is present)
                    if self.binary_callback and b"\x00" in data:
                        self.binary_mode = True
                    elif self.binary_callback:
                        try:
                            self.utf8_decoder.decode(data, final=False)
                        except UnicodeDecodeError:
                            self.binary_mode = True

                    self.buffer.extend(data)

                    if not self.binary_mode:
                        # Process complete lines
                        await self.parse_buffer_and_emit(is_eof=False)

                    # Handle 64KB buffer limit exceeded in text mode (only trigger stdout binary switch if callback is
                    # present)
                    if self.binary_callback and (self.binary_mode or len(self.buffer) > self.chunk_limit):
                        self.binary_mode = True
                        if self.buffer:
                            await self.binary_callback(bytes(self.buffer))
                        self.buffer.clear()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Error reading from {self.stream_name} stream, but process continues: {e}")

    async def parse_buffer_and_emit(self, is_eof: bool):
        while True:
            match = self._delimiter_re.search(self.buffer)
            if match:
                # Check Split-Packet \r Boundary: Standalone \r at the very end of the packet,
                # wait for more packets in case it is \r\n (CRLF).
                if not is_eof and match.group() == b"\r" and match.end() == len(self.buffer):
                    break

                line_bytes = self.buffer[: match.start()]
                delim = match.group()

                if delim == b"\r\n":
                    ending = LogEnding.CRLF
                elif delim == b"\n":
                    ending = LogEnding.LF
                else:
                    ending = LogEnding.CR

                content_str = line_bytes.decode("utf-8", errors="replace")
                await self.log_callback(
                    LogEntry(
                        stream=self.stream_name,
                        content=content_str,
                        ending=ending,
                        timestamp=datetime.now(timezone.utc),
                    )
                )
                del self.buffer[: match.end()]
            else:
                # No delimiter matches found. Handle lines exceeding limit
                if len(self.buffer) >= self.chunk_limit:
                    chunk_bytes = self.buffer[: self.chunk_limit]
                    chunk_str = chunk_bytes.decode("utf-8", errors="replace")
                    await self.log_callback(
                        LogEntry(
                            stream=self.stream_name,
                            content=chunk_str,
                            ending=LogEnding.NONE,
                            timestamp=datetime.now(timezone.utc),
                        )
                    )
                    del self.buffer[: self.chunk_limit]
                    continue

                # Handle EOF residual buffer
                if is_eof and self.buffer:
                    chunk_str = self.buffer.decode("utf-8", errors="replace")
                    await self.log_callback(
                        LogEntry(
                            stream=self.stream_name,
                            content=chunk_str,
                            ending=LogEnding.NONE,
                            timestamp=datetime.now(timezone.utc),
                        )
                    )
                    self.buffer.clear()
                break
