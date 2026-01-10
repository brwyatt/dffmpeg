import time

from typing import Literal, Optional
from pydantic import BaseModel, Field


class AuthenticatedIdentity(BaseModel):
    authenticated: bool = False
    client_id: str = Field(min_length=1)
    role: str = Literal["client", "worker"]
    timestamp: float = Field(default_factory=time.time)
    hmac_key: Optional[str] = Field(min_length=44, max_length=44)
