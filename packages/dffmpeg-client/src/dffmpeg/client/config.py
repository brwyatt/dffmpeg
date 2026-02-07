import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, ValidationError

from dffmpeg.common.config_utils import (
    find_config_file,
    inject_transport_defaults,
    load_hmac_key,
)
from dffmpeg.common.models.config import CoordinatorConnectionConfig
from dffmpeg.common.transports import ClientTransportConfig

logger = logging.getLogger(__name__)


class ClientConfig(BaseModel):
    client_id: str
    hmac_key: str | None = None
    hmac_key_file: str | None = None
    coordinator: CoordinatorConnectionConfig = Field(default_factory=CoordinatorConnectionConfig)
    transports: ClientTransportConfig = Field(default_factory=ClientTransportConfig)
    paths: Dict[str, str] = Field(default_factory=dict)


def load_config(config_file: Optional[str] = None) -> ClientConfig:
    """
    Loads configuration from file and environment variables.
    """
    config_path = None
    try:
        config_path = find_config_file(
            app_name="client",
            env_var="DFFMPEG_CLIENT_CONFIG",
            explicit_path=config_file,
        )
    except FileNotFoundError as e:
        logger.error(str(e))
        raise

    file_data: Dict[str, Any] = {}

    if config_path:
        logger.debug(f"Loading config from {config_path}")
        try:
            with open(config_path, "r") as f:
                file_data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}")
    else:
        logger.debug("No config file found, relying on environment variables.")

    # Override with Environment Variables
    if val := os.environ.get("DFFMPEG_CLIENT_ID"):
        file_data["client_id"] = val

    if val := os.environ.get("DFFMPEG_HMAC_KEY"):
        file_data["hmac_key"] = val

    if val := os.environ.get("DFFMPEG_COORDINATOR_URL"):
        try:
            u = urlparse(val)
            file_data.setdefault("coordinator", {})
            file_data["coordinator"]["scheme"] = u.scheme
            file_data["coordinator"]["host"] = u.hostname
            file_data["coordinator"]["port"] = u.port or (443 if u.scheme == "https" else 80)
            file_data["coordinator"]["path_base"] = u.path
        except Exception as e:
            logger.warning(f"Failed to parse DFFMPEG_COORDINATOR_URL: {e}")

    # Load HMAC key (handles file reference)
    load_hmac_key(file_data, config_path or Path.cwd())

    try:
        config = ClientConfig.model_validate(file_data)
    except ValidationError as e:
        logger.error(
            "Configuration validation failed. Please ensure client_id and hmac_key are provided via config file or"
            " environment variables."
        )
        raise e

    # Inject transport defaults
    inject_transport_defaults(
        config.transports,
        config.coordinator,
        config.client_id,
        str(config.hmac_key),
    )

    return config
