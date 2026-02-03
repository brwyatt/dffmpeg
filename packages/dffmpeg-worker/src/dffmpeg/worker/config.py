from logging import getLogger
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

from dffmpeg.common.transports import ClientTransportConfig

logger = getLogger(__name__)


class CoordinatorConnectionConfig(BaseModel):
    scheme: Literal["http", "https"] = "http"
    host: str = "localhost"
    port: int = 8000
    path_base: str = ""


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


def load_config(path: Path | str = "./config.yml") -> WorkerConfig:
    path = Path(path)
    if not path.exists():
        logger.warning(f"Could not find config file at {str(path)}")

    data = {}
    if path.exists():
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

    # Handle HMAC key file
    hmac_key_file = data.get("hmac_key_file")
    if hmac_key_file:
        if data.get("hmac_key"):
            logger.warning("Both 'hmac_key' and 'hmac_key_file' are defined. 'hmac_key_file' will take precedence.")

        key_path = Path(hmac_key_file)
        if not key_path.is_absolute():
            key_path = path.parent / key_path

        if key_path.exists():
            with open(key_path, "r") as f:
                data["hmac_key"] = f.read().strip()
        else:
            logger.warning(f"HMAC key file not found at {key_path}")

    config = WorkerConfig.model_validate(data)

    if not config.hmac_key:
        raise ValueError("hmac_key must be provided either directly or via hmac_key_file")

    # Inject default config for http_polling if not present
    http_polling_config = config.transports.transport_settings.get("http_polling", {})

    # Construct coordinator URL
    coord = config.coordinator
    base_url = (
        f"{coord.scheme}://{coord.host}:{coord.port}{'' if coord.path_base.startswith('/') else '/'}{coord.path_base}"
    )

    defaults = {
        "client_id": config.client_id,
        "hmac_key": config.hmac_key,
        "coordinator_url": base_url,
    }

    # Merge defaults into the transport config (existing values take precedence?
    # Actually, these are system values, they probably should take precedence or be defaults.
    # Let's treat them as defaults that overwrite if missing, but we're populating the dict.)
    for k, v in defaults.items():
        if k not in http_polling_config:
            http_polling_config[k] = v

    config.transports.transport_settings["http_polling"] = http_polling_config

    return config
