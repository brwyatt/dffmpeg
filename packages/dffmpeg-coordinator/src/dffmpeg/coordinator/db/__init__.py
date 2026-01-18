from logging import getLogger
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository


logger = getLogger(__name__)


type ConfigOptions = Dict[str, Any]
class DBConfig(BaseModel):
    defaults: ConfigOptions = Field(default_factory=dict)
    engine_defaults: Dict[str, ConfigOptions] = Field(default_factory=dict)
    repositories: Dict[str, ConfigOptions] = Field(default_factory=dict)

    def get_repo_config(self, repo_name: str) -> ConfigOptions:
        repo_config = self.repositories.get(repo_name, {})
        engine = repo_config.get("engine") or self.defaults.get("engine", "sqlite")

        config = self.defaults.copy()
        config.update(self.engine_defaults.get(engine, {}))
        config.update(repo_config)

        config["engine"] = engine

        return config

class DB():
    def __init__(self, config: DBConfig):
        self.config = config
        self._auth: Optional[AuthRepository] = None
        self._jobs: Optional[JobRepository] = None
        self._messages: Optional[MessageRepository] = None

    async def setup_all(self):
        await self.auth.setup()
        await self.jobs.setup()
        await self.messages.setup()

    @property
    def auth(self):
        if not self._auth:
            self._auth = AuthRepository(**self.config.get_repo_config("auth"))
        return self._auth

    @property
    def jobs(self):
        if not self._jobs:
            self._jobs = JobRepository(**self.config.get_repo_config("jobs"))
        return self._jobs

    @property
    def messages(self):
        if not self._messages:
            self._messages = MessageRepository(**self.config.get_repo_config("messages"))
        return self._messages
