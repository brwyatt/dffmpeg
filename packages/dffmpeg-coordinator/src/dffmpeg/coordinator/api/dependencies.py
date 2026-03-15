import logging

from fastapi import Depends, HTTPException, Request

from dffmpeg.coordinator.api.utils import is_ip_allowed
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.janitor import Janitor
from dffmpeg.coordinator.transports import TransportManager

logger = logging.getLogger(__name__)


def get_auth_repo(request: Request) -> AuthRepository:
    """
    Dependency to retrieve the AuthRepository from the application state.
    """
    return request.app.state.db.auth


def get_job_repo(request: Request) -> JobRepository:
    """
    Dependency to retrieve the JobRepository from the application state.
    """
    return request.app.state.db.jobs


def get_message_repo(request: Request) -> MessageRepository:
    """
    Dependency to retrieve the MessageRepository from the application state.
    """
    return request.app.state.db.messages


def get_worker_repo(request: Request) -> WorkerRepository:
    """
    Dependency to retrieve the WorkerRepository from the application state.
    """
    return request.app.state.db.workers


def get_db(request: Request) -> DB:
    """
    Dependency to retrieve the DB manager from the application state.
    """
    return request.app.state.db


def get_transports(request: Request) -> TransportManager:
    """
    Dependency to retrieve the Transports manager from the application state.
    """
    return request.app.state.transports


def get_janitor(request: Request) -> Janitor:
    """
    Dependency to retrieve the Janitor manager from the application state.
    """
    return request.app.state.janitor


def get_config(request: Request) -> CoordinatorConfig:
    """
    Dependency to retrieve the CoordinatorConfig from the application state.
    """
    return request.app.state.config


def verify_dashboard_enabled(config: CoordinatorConfig = Depends(get_config)):
    """
    Dependency to verify if the web dashboard is enabled.
    """
    if not config.web_dashboard_enabled:
        raise HTTPException(status_code=404, detail="Dashboard disabled")


def verify_dashboard_ip(request: Request, config: CoordinatorConfig = Depends(get_config)):
    """
    Dependency to verify if the client's IP is allowed to access the dashboard.
    """
    if request.client and request.client.host:
        if is_ip_allowed(request.client.host, config.allowed_dashboard_ips):
            return
        logger.warning(
            f"Dashboard access blocked from IP {request.client.host} (Allowed: {config.allowed_dashboard_ips})"
        )
    else:
        logger.warning("Dashboard access blocked from unknown host")
    raise HTTPException(status_code=403, detail="Forbidden")


def verify_metrics_ip(request: Request, config: CoordinatorConfig = Depends(get_config)):
    """
    Dependency to verify if the client's IP is allowed to access the metrics endpoint.
    """
    if request.client and request.client.host:
        if is_ip_allowed(request.client.host, config.allowed_metrics_ips):
            return
        logger.warning(f"Metrics access blocked from IP {request.client.host} (Allowed: {config.allowed_metrics_ips})")
    else:
        logger.warning("Metrics access blocked from unknown host")
    raise HTTPException(status_code=403, detail="Forbidden")
