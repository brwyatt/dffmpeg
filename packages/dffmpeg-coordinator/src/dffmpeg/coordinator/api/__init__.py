from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Depends, HTTPException, Request
from logging import getLogger
from pydantic import BaseModel

from dffmpeg.common.models import AuthenticatedIdentity, Message, TransportRecord, WorkerRegistration

from dffmpeg.coordinator.api.auth import optional_hmac_auth, required_hmac_auth
from dffmpeg.coordinator.config import load_config
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository
from dffmpeg.coordinator.transports import Transports


logger = getLogger(__name__)


config = load_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = DB(config=config.database)
    await app.state.db.setup_all()

    app.state.transports = Transports(config=config.transports, app=app)
    await app.state.transports.setup_all()

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


def get_worker_repo(request: Request) -> WorkerRepository:
    return request.app.state.db.workers


def get_transports(request: Request) -> Transports:
    return request.app.state.transports


def get_negotiated_transport(client_transports: List[str], server_transports: List[str]):
    for client_transport in client_transports:
        if client_transport in server_transports:
            return client_transport
    raise ValueError("Cannot find supported transport!")


@app.post("/worker/register")
async def worker_register(
    payload: WorkerRegistration,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: Transports = Depends(get_transports),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    if identity.client_id != payload.worker_id:
        raise HTTPException(status_code=403, detail="WorkerID does not match authenticated ClientID")

    try:
        negotiated_transport = get_negotiated_transport(payload.supported_transports, transports.transport_names)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"No supported transports in: {', '.join(payload.supported_transports)}",
        )

    record = WorkerRecord(
        **payload.model_dump(mode="python", exclude={"supported_transports"}),
        status="online",
        transport=negotiated_transport,
        transport_metadata=transports[negotiated_transport].get_metadata(payload.worker_id),
    )

    await worker_repo.add_or_update(record)

    return TransportRecord(transport=WorkerRecord.transport, transport_metadata=WorkerRecord.transport_metadata)


@app.post("/test/emit-message/{recipient_id}")
async def emit_test_message(recipient_id: str, payload: Optional[Dict[str, Any]] = None):
    if payload is None:
        payload = {}

    # This simulates an internal event (like a worker finishing a job)
    msg = Message(
        recipient_id=recipient_id,
        message_type="status_update",
        payload=payload,
    )
    # This calls your new Transport Dispatcher logic
    if await app.state.transports.send_message(msg):
        return {"message_id": str(msg.message_id)}
    else:
        HTTPException(status_code=404, detail="Could not find transport for message")
