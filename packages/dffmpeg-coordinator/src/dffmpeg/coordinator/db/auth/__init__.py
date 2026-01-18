from typing import Optional

from dffmpeg.common.models import AuthenticatedIdentity

from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class AuthRepository(BaseDB):
    def __new__(self, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.auth", engine))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def get_identity(self, client_id: str, include_hmac_key: bool = False) -> Optional[AuthenticatedIdentity]:
        raise NotImplementedError()
