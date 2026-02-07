import asyncio
from importlib.metadata import entry_points
from logging import getLogger
from typing import Dict, List, Type

from fastapi import FastAPI
from pydantic import Field

from dffmpeg.common.models import BaseMessage, ComponentHealth
from dffmpeg.common.models.config import ConfigOptions, DefaultConfig
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.transports.base import BaseServerTransport

logger = getLogger(__name__)


class TransportConfig(DefaultConfig):
    enabled_transports: List[str] = Field(default_factory=list)
    transport_settings: Dict[str, ConfigOptions] = Field(default_factory=dict)

    def get_transport_config(self, transport: str) -> ConfigOptions:
        transport_config = self.transport_settings.get(transport, {})

        config = self.defaults.copy()
        config.update(transport_config)

        return config


class TransportManager:
    def __init__(self, config: TransportConfig, app: FastAPI):
        self.config = config
        self.loaded_transports = self.load_transports()
        self._transports: Dict[str, BaseServerTransport] = {}
        self.app = app

    async def setup_all(self):
        for key in self.loaded_transports.keys():
            await self[key].setup()

    async def health_check(self) -> Dict[str, ComponentHealth]:
        """
        Check the health of all loaded transports.

        Returns:
            Dict[str, ComponentHealth]: A dictionary mapping transport names to their health status.
        """
        names = list(self.loaded_transports.keys())
        results = await asyncio.gather(
            *(self[name].health_check() for name in names),
            return_exceptions=True,
        )

        health = {}
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                health[name] = ComponentHealth(status="unhealthy", detail=str(result))
            else:
                health[name] = result

        return health

    async def send_message(self, message: BaseMessage) -> bool:
        db: DB = self.app.state.db

        await db.messages.add_message(message)

        if message.job_id:
            transport = await db.jobs.get_transport(message.job_id)
        else:
            transport = await db.workers.get_transport(message.recipient_id)

        if transport is None:
            return False

        return await self[transport.transport].send_message(message, transport_metadata=transport.transport_metadata)

    @property
    def transport_names(self) -> List[str]:
        return list(self.loaded_transports.keys())

    def load_transports(self) -> Dict[str, Type[BaseServerTransport]]:
        available_entrypoints = entry_points(group="dffmpeg.transports.server")
        enabled_transports = self.config.enabled_transports
        available_names = [x.name for x in available_entrypoints]
        logger.info(f"Requested transports: {', '.join(enabled_transports)}")
        logger.info(f"Available transports: {', '.join(available_names)}")

        matching = [x for x in available_entrypoints if len(enabled_transports) == 0 or x.name in enabled_transports]

        if len(matching) < 1:
            ValueError(f"No transports matched requested transports: {', '.join(enabled_transports)}")

        not_found = list(set(enabled_transports) - set(available_names))
        if len(not_found) >= 1:
            logger.warning(
                f"Could not find some requested transports, they will not be enabled: {', '.join(not_found)}"
            )

        loaded = {}

        for x in matching:
            cls = x.load()
            if not isinstance(cls, type) or not issubclass(cls, BaseServerTransport):
                raise TypeError(
                    f"Loaded entrypoint {x.name} for dffmpeg.transports.server is not a valid BaseServerTransport!"
                )
            loaded[x.name] = cls

        return loaded

    def __getitem__(self, key) -> BaseServerTransport:
        if key not in self.loaded_transports:
            raise KeyError(f"`{key}` is not a valid loaded transport!")
        if not self._transports.get(key):
            self._transports[key] = self.loaded_transports[key](app=self.app, **self.config.get_transport_config(key))
        return self._transports[key]
