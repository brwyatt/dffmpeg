from logging import getLogger
from typing import Dict, Optional

from pydantic import Field

from dffmpeg.common.models.config import ConfigOptions, DefaultConfig
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.db.workers import WorkerRepository


logger = getLogger(__name__)


class DBConfig(DefaultConfig):
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
        self._workers: Optional[WorkerRepository] = None

    async def setup_all(self):
        await self.auth.setup()
        await self.jobs.setup()
        await self.messages.setup()
        await self.workers.setup()

    @property
    def auth(self) -> AuthRepository:
        if not self._auth:
            logger.info("Setting up Auth Repository")
            self._auth = AuthRepository(**self.config.get_repo_config("auth"))
        return self._auth

    @property
    def jobs(self) -> JobRepository:
        if not self._jobs:
            logger.info("Setting up Job Repository")
            self._jobs = JobRepository(**self.config.get_repo_config("jobs"))
        return self._jobs

    @property
    def messages(self) -> MessageRepository:
        if not self._messages:
            logger.info("Setting up Message Repository")
            self._messages = MessageRepository(**self.config.get_repo_config("messages"))
        return self._messages

    @property
    def workers(self) -> WorkerRepository:
        if not self._workers:
            logger.info("Setting up Worker Repository")
            self._workers = WorkerRepository(**self.config.get_repo_config("workers"))
        return self._workers
