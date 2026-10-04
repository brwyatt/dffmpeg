import asyncio

import pytest

from dffmpeg.common.models import LogEnding
from dffmpeg.common.stdio_stream_handler import StdioHandler


@pytest.mark.asyncio
async def test_stream_reader_lf():
    stream = asyncio.StreamReader()
    stream.feed_data(b"line 1\nline 2\n")
    stream.feed_eof()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)
    await reader.read_loop()

    assert len(logs) == 2
    assert logs[0].content == "line 1"
    assert logs[0].ending == LogEnding.LF
    assert logs[1].content == "line 2"
    assert logs[1].ending == LogEnding.LF


@pytest.mark.asyncio
async def test_stream_reader_crlf():
    stream = asyncio.StreamReader()
    stream.feed_data(b"line 1\r\nline 2\r\n")
    stream.feed_eof()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)
    await reader.read_loop()

    assert len(logs) == 2
    assert logs[0].content == "line 1"
    assert logs[0].ending == LogEnding.CRLF
    assert logs[1].content == "line 2"
    assert logs[1].ending == LogEnding.CRLF


@pytest.mark.asyncio
async def test_stream_reader_cr():
    stream = asyncio.StreamReader()
    stream.feed_data(b"frame= 100\rframe= 200\r")
    stream.feed_eof()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)
    await reader.read_loop()

    assert len(logs) == 2
    assert logs[0].content == "frame= 100"
    assert logs[0].ending == LogEnding.CR
    assert logs[1].content == "frame= 200"
    assert logs[1].ending == LogEnding.CR


@pytest.mark.asyncio
async def test_stream_reader_eof_residual():
    stream = asyncio.StreamReader()
    stream.feed_data(b"residual non-newline text")
    stream.feed_eof()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)
    await reader.read_loop()

    assert len(logs) == 1
    assert logs[0].content == "residual non-newline text"
    assert logs[0].ending == LogEnding.NONE


@pytest.mark.asyncio
async def test_stream_reader_split_packet_crlf():
    stream = asyncio.StreamReader()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)

    # Feed packet ending with trailing \r
    stream.feed_data(b"line 1\r")
    # Run the loop on the event loop for a moment
    loop_task = asyncio.create_task(reader.read_loop())
    await asyncio.sleep(0.01)

    # Standard trailing \r check should delay emission to confirm if CRLF is coming
    assert len(logs) == 0

    # Feed next packet starting with \n
    stream.feed_data(b"\nline 2\n")
    stream.feed_eof()
    await loop_task

    assert len(logs) == 2
    assert logs[0].content == "line 1"
    assert logs[0].ending == LogEnding.CRLF
    assert logs[1].content == "line 2"
    assert logs[1].ending == LogEnding.LF


@pytest.mark.asyncio
async def test_stream_reader_64kb_chunking():
    stream = asyncio.StreamReader()
    large_data = b"A" * (70 * 1024)  # 70KB unbroken line
    stream.feed_data(large_data)
    stream.feed_eof()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    reader = StdioHandler(stream_name="stdout", stream=stream, log_callback=log_callback)
    await reader.read_loop()

    # Expected: split into 64KB and 6KB chunks, both LogEnding.NONE
    assert len(logs) == 2
    assert len(logs[0].content) == 64 * 1024
    assert logs[0].ending == LogEnding.NONE
    assert len(logs[1].content) == 6 * 1024
    assert logs[1].ending == LogEnding.NONE


@pytest.mark.asyncio
async def test_stream_reader_split_multibyte_utf8():
    stream = asyncio.StreamReader()

    logs = []

    async def log_callback(entry):
        logs.append(entry)

    binary = []

    async def binary_callback(data):
        binary.append(data)

    reader = StdioHandler(
        stream_name="stdout", stream=stream, log_callback=log_callback, binary_callback=binary_callback
    )

    # '日' in UTF-8 is b'\xe6\x97\xa5'.
    # Feed "Hello \xe6" first (split multibyte character)
    stream.feed_data(b"Hello \xe6")
    loop_task = asyncio.create_task(reader.read_loop())
    await asyncio.sleep(0.01)

    # It should not trigger binary mode yet (no binary chunks recorded)
    assert len(binary) == 0

    # Feed the rest b"\x97\xa5\n"
    stream.feed_data(b"\x97\xa5\n")
    stream.feed_eof()
    await loop_task

    assert len(binary) == 0
    assert len(logs) == 1
    assert logs[0].content == "Hello 日"
    assert logs[0].ending == LogEnding.LF
