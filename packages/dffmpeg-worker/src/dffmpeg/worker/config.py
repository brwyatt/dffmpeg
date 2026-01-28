from logging import getLogger
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

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
    coordinator: CoordinatorConnectionConfig = Field(default_factory=CoordinatorConnectionConfig)


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

    return config
