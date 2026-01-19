from typing import Optional

from fastapi import FastAPI

from dffmpeg.common.models import Message


class BaseServerTransport():
    async def setup(self, app: Optional[FastAPI] = None):
        raise NotImplemented()

    async def send_message(self, message: Message) -> bool:
        raise NotImplemented()
