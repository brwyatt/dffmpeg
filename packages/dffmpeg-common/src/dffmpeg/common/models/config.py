from typing import Any, Dict, Literal

from pydantic import BaseModel, Field

type ConfigOptions = Dict[str, Any]


class DefaultConfig(BaseModel):
    defaults: ConfigOptions = Field(default_factory=dict)


class CoordinatorConnectionConfig(BaseModel):
    scheme: Literal["http", "https"] = "http"
    host: str = "localhost"
    port: int = 8000
    path_base: str = ""
