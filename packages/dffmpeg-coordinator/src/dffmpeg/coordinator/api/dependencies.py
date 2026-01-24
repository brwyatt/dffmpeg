from fastapi import Request

from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import TransportManager


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


def get_transports(request: Request) -> TransportManager:
    """
    Dependency to retrieve the Transports manager from the application state.
    """
    return request.app.state.transports
