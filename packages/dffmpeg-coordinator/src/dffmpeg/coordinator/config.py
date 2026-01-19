import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from logging import getLogger

from dffmpeg.coordinator.db import DBConfig
from dffmpeg.coordinator.transports import TransportConfig


logger = getLogger(__name__)


class CoordinatorConfig(BaseModel):
    database: DBConfig = Field(default_factory=DBConfig)
    transports: TransportConfig = Field(default_factory=TransportConfig)


def load_config(path: str = "./config.yml") -> CoordinatorConfig:
    path = Path(path)
    if not path.exists():
        logger.warning(f"Could not find config file at {str(path)}")
        return CoordinatorConfig()

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return CoordinatorConfig.model_validate(data)
