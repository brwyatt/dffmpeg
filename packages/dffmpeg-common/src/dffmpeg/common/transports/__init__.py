from importlib.metadata import entry_points
from logging import getLogger
from typing import Dict, List, Type

from pydantic import Field

from dffmpeg.common.models.config import ConfigOptions, DefaultConfig
from dffmpeg.common.transports.base import BaseClientTransport

logger = getLogger(__name__)


class ClientTransportConfig(DefaultConfig):
    enabled_transports: List[str] = Field(default_factory=list)
    transport_settings: Dict[str, ConfigOptions] = Field(default_factory=dict)

    def get_transport_config(self, transport: str) -> ConfigOptions:
        transport_config = self.transport_settings.get(transport, {})

        config = self.defaults.copy()
        config.update(transport_config)

        return config


class TransportManager:
    def __init__(self, config: ClientTransportConfig):
        self.config = config
        self.loaded_transports = self.load_transports()
        self._transports: Dict[str, BaseClientTransport] = {}

    @property
    def transport_names(self) -> List[str]:
        return list(self.loaded_transports.keys())

    def load_transports(self) -> Dict[str, Type[BaseClientTransport]]:
        available_entrypoints = {x.name: x for x in entry_points(group="dffmpeg.transports.client")}
        enabled_transports = self.config.enabled_transports

        # If no transports are explicitly enabled, default to http_polling
        if not enabled_transports:
            enabled_transports = ["http_polling"]

        logger.info(f"Requested transports: {', '.join(enabled_transports)}")
        logger.info(f"Available transports: {', '.join(available_entrypoints.keys())}")

        matching = []
        for name in enabled_transports:
            if name in available_entrypoints:
                matching.append(available_entrypoints[name])
            else:
                logger.warning(f"Requested transport '{name}' not found in available transports.")

        not_found = list(set(enabled_transports) - set(available_entrypoints.keys()))
        if len(not_found) >= 1:
            logger.warning(
                f"Could not find some requested transports, they will not be enabled: {', '.join(not_found)}"
            )

        loaded = {}

        for x in matching:
            try:
                cls = x.load()
                if not isinstance(cls, type) or not issubclass(cls, BaseClientTransport):
                    logger.warning(
                        f"Loaded entrypoint {x.name} for dffmpeg.transports.client is not a valid BaseClientTransport!"
                    )
                    continue
                loaded[x.name] = cls
            except Exception as e:
                logger.error(f"Failed to load transport {x.name}: {e}")

        return loaded

    def __getitem__(self, key: str) -> BaseClientTransport:
        if key not in self.loaded_transports:
            raise KeyError(f"`{key}` is not a valid loaded transport!")

        if key not in self._transports:
            transport_config = self.config.get_transport_config(key)
            self._transports[key] = self.loaded_transports[key](**transport_config)

        return self._transports[key]
