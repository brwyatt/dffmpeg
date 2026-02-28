import asyncio
import logging
import random
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


async def async_retry(
    func: Callable[[], Awaitable[Any]],
    max_sleep: float,
    initial_delay: float = 1.0,
    multiplier: float = 2.0,
) -> Any:
    """
    Retries an async function with exponential backoff until the next delay
    is >= the max_sleep. If that happens, raises the last exception.
    """
    delay = initial_delay
    while True:
        try:
            return await func()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if delay >= max_sleep:
                raise e
            logger.debug(f"Retrying after {delay}s due to error: {e}")
            await asyncio.sleep(delay)
            delay *= multiplier


async def heartbeat_loop(
    name: str,
    action: Callable[[], Awaitable[Any]],
    is_running: Callable[[], bool],
    interval: float,
    jitter_bound: float,
    first_immediate: bool = False,
    retry_initial_delay: float = 1.0,
) -> None:
    """
    A generic loop for periodic background actions (like heartbeats).
    """
    first_loop = True
    while is_running():
        try:
            if not first_loop or not first_immediate:
                jitter = random.uniform(-jitter_bound, jitter_bound)
                await asyncio.sleep(max(1.0, interval + jitter))

            first_loop = False

            # Use retry logic for the action. max_sleep is the interval so retries don't exceed the loop cycle length.
            await async_retry(func=action, max_sleep=interval, initial_delay=retry_initial_delay)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error in {name} loop: {e}")
            # Ensure we don't rapid-fire on exception
            if first_loop:
                # Means we errored before we set this to False, somehow
                first_loop = False
