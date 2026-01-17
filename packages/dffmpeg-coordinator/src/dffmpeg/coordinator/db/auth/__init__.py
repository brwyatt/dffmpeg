from dffmpeg.coordinator.db.db_loader import load

from dffmpeg.common.models import AuthenticatedIdentity


class AuthRepository():
    def __new__(self, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.auth", engine))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    def get_identity(self, client_id: str, include_hmac_key: bool = False) -> AuthenticatedIdentity:
        raise NotImplementedError()
