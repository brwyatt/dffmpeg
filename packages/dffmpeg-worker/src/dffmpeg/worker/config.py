from logging import getLogger
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from dffmpeg.common.config_utils import (
    find_config_file,
    inject_transport_defaults,
    load_hmac_key,
)
from dffmpeg.common.models.config import CoordinatorConnectionConfig
from dffmpeg.common.transports import ClientTransportConfig

logger = getLogger(__name__)


class MountConfig(BaseModel):
    path: str
    dependencies: list[str] = Field(default_factory=list)


class MountManagementConfig(BaseModel):
    recovery: bool = True
    sudo: bool = False
    mounts: list[str | MountConfig] = Field(default_factory=list)


class WorkerConfig(BaseModel):
    client_id: str
    hmac_key: str | None = None
    hmac_key_file: str | None = None
    registration_interval: int = 15
    jitter: float = 0.5
    log_batch_size: int = 100
    log_batch_delay: float = 0.25
    coordinator: CoordinatorConnectionConfig = Field(default_factory=CoordinatorConnectionConfig)
    transports: ClientTransportConfig = Field(default_factory=ClientTransportConfig)
    binaries: dict[str, str] = Field(default_factory=dict)
    paths: dict[str, str] = Field(default_factory=dict)
    mount_management: MountManagementConfig = Field(default_factory=MountManagementConfig)


def load_config(path: Path | str | None = None) -> WorkerConfig:
    config_path = None
    try:
        config_path = find_config_file(app_name="worker", env_var="DFFMPEG_WORKER_CONFIG", explicit_path=path)
    except FileNotFoundError as e:
        logger.error(str(e))
        raise

    data = {}
    if config_path:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    else:
        logger.warning("Could not find worker config file.")

    # Handle HMAC key file
    load_hmac_key(data, config_path or Path.cwd())

    config = WorkerConfig.model_validate(data)

    # Inject transport defaults
    inject_transport_defaults(
        config.transports,
        config.coordinator,
        config.client_id,
        str(config.hmac_key),
    )

    return config
