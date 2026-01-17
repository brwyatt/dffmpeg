from logging import getLogger
from typing import Any, Dict, Optional

from pydantic import BaseModel
from dffmpeg.coordinator.db.auth import AuthRepository


logger = getLogger(__name__)


type ConfigOptions = Dict[str, Any]
class DBConfig(BaseModel):
    defaults: ConfigOptions = {}
    engine_defaults: Dict[str, ConfigOptions] = {}
    auth: ConfigOptions = {}

class DB():
    def __init__(self, config: DBConfig):
        self.config = config
        self._auth: Optional[AuthRepository] = None

    def get_db_config(self, db_name: str):
        logger.warning(f"Fetching DB config for {db_name}")
        engine = getattr(self.config, db_name).get("engine", self.config.defaults.get("engine", "sqlite"))
        return {
            **self.config.defaults,
            **self.config.engine_defaults.get(engine, {}),
            **getattr(self.config, db_name),
        }

    @property
    def auth(self):
        if not self._auth:
            self._auth = AuthRepository(**self.get_db_config("auth"))
        return self._auth
