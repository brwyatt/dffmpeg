from fastapi import FastAPI, Depends
from logging import getLogger
from pydantic import BaseModel

from dffmpeg.coordinator.api.auth import HMACIdentity, optional_hmac_auth


logger = getLogger(__name__)

app = FastAPI(title="dffmpeg Coordinator")


class PingRequest(BaseModel):
    client_id: str
    message: str

@app.get("/health")
async def health():
    """Unauthenticated health check"""
    return {"status": "online"}

@app.post("/ping")
async def ping(payload: PingRequest, identity: HMACIdentity = Depends(optional_hmac_auth)):
    """Authenticated test endpoint"""
    return {
        "status": "received",
        "echo": payload.message,
        "identity": identity,
    }
