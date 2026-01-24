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
    """
    Configuration for the Database Layer.

    Attributes:
        engine_defaults (Dict[str, ConfigOptions]): Default configuration for each engine type.
        repositories (Dict[str, ConfigOptions]): Specific configuration for each repository (auth, jobs, etc.).
    """

    engine_defaults: Dict[str, ConfigOptions] = Field(default_factory=dict)
    repositories: Dict[str, ConfigOptions] = Field(default_factory=dict)

    def get_repo_config(self, repo_name: str) -> ConfigOptions:
        """
        Retrieves the combined configuration for a specific repository.
        Merges defaults, engine defaults, and repository-specific config.

        Args:
            repo_name (str): The name of the repository (e.g., "auth", "jobs").

        Returns:
            ConfigOptions: The merged configuration dictionary.
        """
        repo_config = self.repositories.get(repo_name, {})
        engine = repo_config.get("engine") or self.defaults.get("engine", "sqlite")

        config = self.defaults.copy()
        config.update(self.engine_defaults.get(engine, {}))
        config.update(repo_config)

        config["engine"] = engine

        return config


class DB:
    """
    Database Manager class responsible for initializing and providing access to repositories.
    """

    def __init__(self, config: DBConfig):
        self.config = config
        self._auth: Optional[AuthRepository] = None
        self._jobs: Optional[JobRepository] = None
        self._messages: Optional[MessageRepository] = None
        self._workers: Optional[WorkerRepository] = None

    async def setup_all(self):
        """
        Initializes all repositories (e.g., creating tables).
        """
        await self.auth.setup()
        await self.jobs.setup()
        await self.messages.setup()
        await self.workers.setup()

    @property
    def auth(self) -> AuthRepository:
        """Lazy-loaded AuthRepository."""
        if not self._auth:
            logger.info("Setting up Auth Repository")
            self._auth = AuthRepository(**self.config.get_repo_config("auth"))
        return self._auth

    @property
    def jobs(self) -> JobRepository:
        """Lazy-loaded JobRepository."""
        if not self._jobs:
            logger.info("Setting up Job Repository")
            self._jobs = JobRepository(**self.config.get_repo_config("jobs"))
        return self._jobs

    @property
    def messages(self) -> MessageRepository:
        """Lazy-loaded MessageRepository."""
        if not self._messages:
            logger.info("Setting up Message Repository")
            self._messages = MessageRepository(**self.config.get_repo_config("messages"))
        return self._messages

    @property
    def workers(self) -> WorkerRepository:
        """Lazy-loaded WorkerRepository."""
        if not self._workers:
            logger.info("Setting up Worker Repository")
            self._workers = WorkerRepository(**self.config.get_repo_config("workers"))
        return self._workers
