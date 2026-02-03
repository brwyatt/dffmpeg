from logging import getLogger
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from dffmpeg.common.models import default_job_heartbeat_interval
from dffmpeg.coordinator.db import DBConfig
from dffmpeg.coordinator.transports import TransportConfig

logger = getLogger(__name__)


class JanitorConfig(BaseModel):
    interval: int = 10
    jitter: float = 0.5
    worker_threshold_factor: float = 1.5
    job_heartbeat_threshold_factor: float = 1.5
    job_assignment_timeout: int = 30
    job_pending_retry_delay: int = 5
    job_pending_timeout: int = 30


class CoordinatorConfig(BaseModel):
    database: DBConfig = Field(default_factory=DBConfig)
    transports: TransportConfig = Field(default_factory=TransportConfig)
    janitor: JanitorConfig = Field(default_factory=JanitorConfig)
    job_heartbeat_interval: int = default_job_heartbeat_interval


def load_config(path: Path | str = "./config.yml") -> CoordinatorConfig:
    path = Path(path)
    if not path.exists():
        logger.warning(f"Could not find config file at {str(path)}")
        return CoordinatorConfig()

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    # Handle external encryption keys file if referenced in the config
    auth_config = data.get("database", {}).get("repositories", {}).get("auth", {})
    keys_file = auth_config.get("encryption_keys_file")
    if keys_file:
        keys_path = Path(keys_file)
        if not keys_path.is_absolute():
            keys_path = path.parent / keys_path

        if keys_path.exists():
            with open(keys_path, "r") as f:
                keys_data = yaml.safe_load(f)
                if isinstance(keys_data, dict):
                    # Merge or set the encryption_keys
                    if "encryption_keys" not in auth_config:
                        auth_config["encryption_keys"] = {}
                    auth_config["encryption_keys"].update(keys_data)
        else:
            logger.warning(f"Encryption keys file not found at {keys_path}")

    return CoordinatorConfig.model_validate(data)
