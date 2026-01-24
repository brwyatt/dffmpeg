from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException

from dffmpeg.common.models import Message
from dffmpeg.coordinator.api.dependencies import get_transports
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()


@router.post("/test/emit-message/{recipient_id}")
async def emit_test_message(
    recipient_id: str,
    payload: Optional[Dict[str, Any]] = None,
    transports: TransportManager = Depends(get_transports),
):
    """
    Test endpoint to simulate emitting a message to a recipient.

    Args:
        recipient_id (str): The ID of the message recipient.
        payload (Optional[Dict[str, Any]]): The message payload.
        transports (Transports): Transport manager.

    Returns:
        dict: The message ID if sent successfully.

    Raises:
        HTTPException: If the transport for the message cannot be found.
    """
    if payload is None:
        payload = {}

    # This simulates an internal event (like a worker finishing a job)
    msg = Message(
        recipient_id=recipient_id,
        message_type="job_status",
        payload=payload,
    )
    # This calls your new Transport Dispatcher logic
    if await transports.send_message(msg):
        return {"message_id": str(msg.message_id)}
    else:
        raise HTTPException(status_code=404, detail="Could not find transport for message")
