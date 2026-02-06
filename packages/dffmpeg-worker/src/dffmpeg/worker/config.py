from logging import getLogger
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from dffmpeg.common.transports import ClientTransportConfig
from dffmpeg.common.models.config import CoordinatorConnectionConfig
from dffmpeg.common.config_utils import load_hmac_key, inject_transport_defaults

logger = getLogger(__name__)


class WorkerConfig(BaseModel):
    client_id: str
    hmac_key: str | None = None
    hmac_key_file: str | None = None
    registration_interval: int = 15
    jitter: float = 0.5
    coordinator: CoordinatorConnectionConfig = Field(default_factory=CoordinatorConnectionConfig)
    transports: ClientTransportConfig = Field(default_factory=ClientTransportConfig)
    binaries: dict[str, str] = Field(default_factory=dict)
    paths: dict[str, str] = Field(default_factory=dict)


def load_config(path: Path | str = "./config.yaml") -> WorkerConfig:
    path = Path(path)
    if not path.exists():
        logger.warning(f"Could not find config file at {str(path)}")

    data = {}
    if path.exists():
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

    # Handle HMAC key file
    load_hmac_key(data, path)

    config = WorkerConfig.model_validate(data)

    # Inject transport defaults
    inject_transport_defaults(
        config.transports,
        config.coordinator,
        config.client_id,
        str(config.hmac_key),
    )

    return config
