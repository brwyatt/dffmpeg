from typing import Any, Dict

from pydantic import BaseModel, Field


type ConfigOptions = Dict[str, Any]


class DefaultConfig(BaseModel):
    defaults: ConfigOptions = Field(default_factory=dict)
