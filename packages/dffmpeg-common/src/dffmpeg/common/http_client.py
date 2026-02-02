import logging
from typing import Any, Callable, Dict, Optional

import httpx

from dffmpeg.common.auth.request_signer import RequestSigner

logger = logging.getLogger(__name__)


class AuthenticatedAsyncClient:
    """
    Async HTTP client that handles HMAC request signing.
    """

    def __init__(
        self,
        base_url: str,
        client_id: str,
        hmac_key: str,
        http_client_cls: Callable[..., httpx.AsyncClient] = httpx.AsyncClient,
    ):
        self.client_id = client_id
        self.signer = RequestSigner(hmac_key)
        self._client = http_client_cls(base_url=base_url)

    async def request(self, method: str, url: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        """
        Sends a signed HTTP request.

        Args:
            method (str): HTTP method.
            url (str): Request URL/path.
            json (Optional[Dict]): JSON body.
            **kwargs: Additional arguments for httpx.request.

        Returns:
            httpx.Response: The response.
        """
        headers, content = self.signer.sign_request(self.client_id, method, url, json)

        # Merge headers if provided in kwargs
        request_headers = kwargs.pop("headers", {})
        request_headers.update(headers)

        return await self._client.request(method, url, headers=request_headers, content=content, **kwargs)

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Sends a GET request."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        """Sends a POST request."""
        return await self.request("POST", url, json=json, **kwargs)

    async def aclose(self):
        """Closes the underlying client."""
        await self._client.aclose()
