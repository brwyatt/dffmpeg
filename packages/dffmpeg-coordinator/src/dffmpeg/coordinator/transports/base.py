from typing import Any, Dict, Optional

from fastapi import FastAPI

from dffmpeg.common.models import Message, TransportMetadata


class BaseServerTransport():
    async def setup(self, app: FastAPI):
        raise NotImplementedError()

    async def send_message(self, message: Message, transport_metadata: Optional[TransportMetadata] = None) -> bool:
        raise NotImplementedError()

    def get_metadata(self, client_id: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError()
