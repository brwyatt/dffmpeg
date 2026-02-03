import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional

import httpx
from pydantic import TypeAdapter
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.common.transports.base import BaseClientTransport

logger = logging.getLogger(__name__)


class HTTPPollingClientTransport(BaseClientTransport):
    def __init__(self, client_id: str, hmac_key: str, coordinator_url: str, poll_wait: int = 5, **kwargs):
        self.client_id = client_id
        self.signer = RequestSigner(hmac_key)
        self.base_url = coordinator_url
        self.poll_wait = poll_wait
        self.poll_path: Optional[str] = None
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None
        self._message_adapter = TypeAdapter(Message)

    async def connect(self, metadata: Dict[str, Any]):
        self.poll_path = metadata.get("path")
        if not self.poll_path:
            raise ValueError("Transport metadata missing 'path' for HTTP polling")

        self._client = httpx.AsyncClient(base_url=self.base_url)
        self._running = True
        logger.info(f"Connected to HTTP Polling transport at {self.base_url}{self.poll_path}")

    async def disconnect(self):
        self._running = False
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("Disconnected from HTTP Polling transport")

    async def listen(self) -> AsyncIterator[BaseMessage]:  # type: ignore
        if not self._client or not self.poll_path:
            raise RuntimeError("Transport not connected")

        last_message_id: Optional[ULID] = None

        while self._running:
            try:
                # Sign the request (path only, params are handled separately)
                headers, _ = self.signer.sign_request(self.client_id, "GET", self.poll_path)

                params: Dict[str, Any] = {"wait": self.poll_wait}
                if last_message_id:
                    params["last_message_id"] = str(last_message_id)

                response = await self._client.get(self.poll_path, headers=headers, params=params, timeout=10.0)

                if response.status_code == 200:
                    data = response.json()
                    messages = data.get("messages", [])
                    for msg_data in messages:
                        try:
                            msg = self._message_adapter.validate_python(msg_data)
                            last_message_id = msg.message_id
                            yield msg
                        except Exception as e:
                            logger.error(f"Failed to parse message: {e}")
                else:
                    logger.warning(f"Poll failed with status {response.status_code}: {response.text}")
                    await asyncio.sleep(1)

            except httpx.ReadTimeout:
                continue
            except Exception as e:
                if not self._running:
                    break
                logger.error(f"Error in HTTP polling loop: {e}")
                await asyncio.sleep(1)
