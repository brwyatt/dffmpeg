from typing import Any, Dict, Optional

from pydantic import BaseModel
from dffmpeg.coordinator.db.auth import AuthRepository


class DBConfig(BaseModel):
    defaults: Dict[str, Any] = {}
    auth: Dict[str, Any] = {}


# should load this from somewhere, but for now...
config = DBConfig(
    defaults = {},
    auth = {
        "engine": "sqlite",
        "path": "./authdb.sqlite"
    },
)

class DB():
    def __init__(self, config: DBConfig):
        self.config = config
        self._auth: Optional[AuthRepository] = None

    @property
    def auth(self):
        if not self._auth:
            auth_config = {**self.config.defaults, **self.config.auth}
            self._auth = AuthRepository(**auth_config)
        return self._auth
