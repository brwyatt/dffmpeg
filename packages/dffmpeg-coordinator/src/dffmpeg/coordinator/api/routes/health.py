from fastapi import APIRouter, Depends, Response

from dffmpeg.common.models import HealthResponse
from dffmpeg.common.version import get_package_version
from dffmpeg.coordinator.api.dependencies import get_db, get_transports
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health(
    response: Response,
    deep: bool = False,
    db: DB = Depends(get_db),
    transports: TransportManager = Depends(get_transports),
):
    """
    Health check endpoint.

    Args:
        response (Response): The response object.
        deep (bool): Whether to perform a deep health check of all components.
        db (DB): The database manager.
        transports (TransportManager): The transport manager.

    Returns:
        HealthResponse: The health status of the service.
    """
    version = get_package_version("dffmpeg-coordinator")

    if not deep:
        return HealthResponse(status="online", version=version)

    db_health = await db.health_check()
    transport_health = await transports.health_check()

    # Determine overall status
    status = "online"
    if any(h.status == "unhealthy" for h in db_health.values()) or any(
        h.status == "unhealthy" for h in transport_health.values()
    ):
        status = "unhealthy"
        response.status_code = 500

    return HealthResponse(
        status=status,
        version=version,
        databases=db_health,
        transports=transport_health,
    )
