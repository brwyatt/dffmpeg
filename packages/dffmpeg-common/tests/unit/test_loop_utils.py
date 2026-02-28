import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dffmpeg.common.loop_utils import async_retry, heartbeat_loop


@pytest.mark.asyncio
async def test_async_retry_success_first_try():
    mock_func = AsyncMock(return_value="success")

    result = await async_retry(mock_func, max_sleep=10.0)

    assert result == "success"
    mock_func.assert_called_once()


@pytest.mark.asyncio
async def test_async_retry_success_after_retries():
    # Fail twice, succeed on third
    mock_func = AsyncMock(side_effect=[ValueError("fail 1"), ValueError("fail 2"), "success"])

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await async_retry(mock_func, max_sleep=10.0, initial_delay=1.0, multiplier=2.0)

        assert result == "success"
        assert mock_func.call_count == 3
        # Should have slept for 1.0, then 2.0
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)


@pytest.mark.asyncio
async def test_async_retry_exceeds_max_sleep():
    mock_func = AsyncMock(side_effect=ValueError("persistent fail"))

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # Initial delay 2.0, max_sleep 3.0
        # Attempt 1: fails, delay 2.0 < 3.0 -> sleep(2.0), new delay = 4.0
        # Attempt 2: fails, delay 4.0 >= 3.0 -> raises Error
        with pytest.raises(ValueError, match="persistent fail"):
            await async_retry(mock_func, max_sleep=3.0, initial_delay=2.0, multiplier=2.0)

        assert mock_func.call_count == 2
        mock_sleep.assert_called_once_with(2.0)


@pytest.mark.asyncio
async def test_async_retry_cancelled():
    mock_func = AsyncMock(side_effect=asyncio.CancelledError("cancelled"))

    with pytest.raises(asyncio.CancelledError):
        await async_retry(mock_func, max_sleep=10.0)

    assert mock_func.call_count == 1


@pytest.mark.asyncio
async def test_heartbeat_loop_basic():
    action = AsyncMock()

    # We want the loop to run twice and then stop
    run_state = [True, True, False]

    def is_running():
        return run_state.pop(0)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # We patch random.uniform to control the jitter
        with patch("random.uniform", return_value=0.1):
            await heartbeat_loop(
                name="test",
                action=action,
                is_running=is_running,
                interval=5.0,
                jitter_bound=0.5,
                first_immediate=False,
            )

    # first iteration: sleep(5.1), action()
    # second iteration: sleep(5.1), action()
    assert action.call_count == 2
    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(5.1)


@pytest.mark.asyncio
async def test_heartbeat_loop_first_immediate():
    action = AsyncMock()

    # We want the loop to run twice and then stop
    run_state = [True, True, False]

    def is_running():
        return run_state.pop(0)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with patch("random.uniform", return_value=0.1):
            await heartbeat_loop(
                name="test",
                action=action,
                is_running=is_running,
                interval=5.0,
                jitter_bound=0.5,
                first_immediate=True,
            )

    assert action.call_count == 2
    # Sleep should only be called once because first_immediate skips the sleep on the first loop
    assert mock_sleep.call_count == 1
    mock_sleep.assert_called_with(5.1)


@pytest.mark.asyncio
async def test_heartbeat_loop_action_raises():
    # If the action raises an exception that async_retry lets bubble up
    # (e.g. because max_sleep is exceeded immediately), heartbeat_loop catches it and logs it.
    action = AsyncMock(side_effect=ValueError("action failed"))

    run_state = [True, False]

    def is_running():
        return run_state.pop(0)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with patch("dffmpeg.common.loop_utils.logger") as mock_logger:
            await heartbeat_loop(
                name="test",
                action=action,
                is_running=is_running,
                interval=1.0,  # This acts as max_sleep in async_retry
                jitter_bound=0.5,
                first_immediate=True,
                retry_initial_delay=2.0,  # Forces async_retry to bubble up on first error since 2.0 >= 1.0
            )

    # action is called once, it fails, async_retry sees delay=2.0 >= max_sleep=1.0, raises.
    # heartbeat_loop catches it, logs it. It no longer sleeps in the except block!
    assert action.call_count == 1
    mock_sleep.assert_not_called()  # We mocked the extra sleep out
    assert mock_logger.error.called


@pytest.mark.asyncio
async def test_heartbeat_loop_cancelled():
    action = AsyncMock(side_effect=asyncio.CancelledError("cancelled"))

    run_state = [True]

    def is_running():
        return run_state.pop(0)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_loop(
                name="test",
                action=action,
                is_running=is_running,
                interval=1.0,
                jitter_bound=0.5,
                first_immediate=True,
            )
