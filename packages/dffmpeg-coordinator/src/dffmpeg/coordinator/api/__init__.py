from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from logging import getLogger
from pydantic import BaseModel

from dffmpeg.common.models import AuthenticatedIdentity

from dffmpeg.coordinator.api.auth import optional_hmac_auth
from dffmpeg.coordinator.db import DB, DBConfig


logger = getLogger(__name__)


# should load this from somewhere, but for now...
config = DBConfig(
    defaults = {},
    engine_defaults = {},
    auth = {
        "engine": "sqlite",
        "path": "./authdb.sqlite"
    },
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = DB(config)
    app.state.db.setup_all()
    yield

app = FastAPI(title="dffmpeg Coordinator", lifespan=lifespan)


class PingRequest(BaseModel):
    client_id: str
    message: str

@app.get("/health")
async def health():
    """Unauthenticated health check"""
    return {"status": "online"}

@app.post("/ping")
async def ping(payload: PingRequest, identity: AuthenticatedIdentity = Depends(optional_hmac_auth)):
    """Authenticated test endpoint"""
    return {
        "status": "received",
        "echo": payload.message,
        "identity": identity,
    }
