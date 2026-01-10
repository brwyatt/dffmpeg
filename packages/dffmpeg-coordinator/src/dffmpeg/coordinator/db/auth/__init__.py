from importlib.metadata import entry_points

from dffmpeg.common.models import AuthenticatedIdentity


class AuthRepository():
    def __new__(self, *args, engine: str, **kwargs):
        available_entrypoints = entry_points(group="dffmpeg.db.auth")
        matching = [x for x in available_entrypoints if x.name == engine]
        if len(matching) != 1:
            available_names = ", ".join([x.name for x in available_entrypoints])
            raise ValueError(f"Invalid database engine \"{engine}\" for AuthRepository! Expected one of: {available_names}")
        loaded = matching[0].load()
        return object.__new__(loaded)

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    def get_identity(self, client_id: str, include_hmac_key: bool = False) -> AuthenticatedIdentity:
        raise NotImplementedError()
