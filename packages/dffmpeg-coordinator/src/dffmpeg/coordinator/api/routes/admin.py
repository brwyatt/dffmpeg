from fastapi import APIRouter, Depends, HTTPException

from dffmpeg.common.models import JanitorActionRequest
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.api.dependencies import get_janitor
from dffmpeg.coordinator.janitor import Janitor

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/janitor", status_code=202)
async def janitor_action(
    request: JanitorActionRequest,
    janitor: Janitor = Depends(get_janitor),
    auth=Depends(required_hmac_auth),
):
    """
    Trigger a background janitor action.
    Requires an 'admin' role.
    """
    if auth.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")

    task_name = request.action
    janitor.schedule_task(task_name, delay=0)

    return {"message": "Action accepted", "action": request.action}
