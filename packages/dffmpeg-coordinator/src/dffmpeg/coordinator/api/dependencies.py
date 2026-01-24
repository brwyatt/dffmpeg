from fastapi import Request

from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import Transports


def get_worker_repo(request: Request) -> WorkerRepository:
    """
    Dependency to retrieve the WorkerRepository from the application state.
    """
    return request.app.state.db.workers


def get_transports(request: Request) -> Transports:
    """
    Dependency to retrieve the Transports manager from the application state.
    """
    return request.app.state.transports
