import time
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Discriminator, Field, Tag
from ulid import ULID

ClientId: str = Field(min_length=1)
OptionalClientId: str | None = Field(default=None, min_length=1)

type IdentityRole = Literal["client", "worker", "admin"]


class AuthenticatedIdentity(BaseModel):
    """
    Represents an identity that has been authenticated via HMAC.

    Attributes:
        authenticated (bool): Whether the identity is authenticated.
        client_id (str): The client ID.
        role (Literal["client", "worker", "admin"]): The role of the client.
        timestamp (float): The timestamp of the request/authentication.
        hmac_key (Optional[str]): The HMAC key used for signing (usually filtered out in responses).
    """

    authenticated: bool = False
    client_id: str = ClientId
    role: IdentityRole
    timestamp: float = Field(default_factory=time.time)
    hmac_key: Optional[str] = Field(min_length=44, max_length=44)


TransportMetadata = Dict[str, Any]


class TransportRecord(BaseModel):
    """
    Represents transport-specific configuration and metadata.

    Attributes:
        transport (str): The name of the transport (e.g., "http_polling").
        transport_metadata (Dict[str, Any]): Transport-specific configuration details (e.g., poll paths).
    """

    transport: str = Field(min_length=1)
    transport_metadata: TransportMetadata = Field(default_factory=dict)


type JobStatus = Literal["pending", "assigned", "running", "completed", "failed", "canceled", "canceling"]

default_job_heartbeat_interval = 5


class Job(BaseModel):
    """
    Represents a FFmpeg job.

    Attributes:
        job_id (ULID): Unique identifier for the job.
        requester_id (str): The client ID who requested the job.
        binary_name (str): The binary to execute.
        arguments (List[str]): List of arguments to pass to the binary.
        paths (List[str]): List of path variables required by the job.
        status (Literal): Current status of the job.
        worker_id (Optional[str]): The worker assigned to the job, if any.
        created_at (datetime): Timestamp of creation.
        last_update (datetime): Timestamp of last record modification.
        worker_last_seen (datetime): Timestamp of last worker heartbeat.
        client_last_seen (Optional[datetime]): Timestamp of last client heartbeat.
        heartbeat_interval (int): Number of seconds between heartbeats (worker and client).
        monitor (bool): Whether the job is actively monitored by the client.
    """

    job_id: ULID = Field(default_factory=ULID)
    requester_id: str = ClientId
    binary_name: str = Field(min_length=1)
    arguments: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    status: JobStatus
    exit_code: Optional[int] = None
    worker_id: str | None = OptionalClientId
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    worker_last_seen: Optional[datetime] = None
    client_last_seen: Optional[datetime] = None
    heartbeat_interval: int = default_job_heartbeat_interval
    monitor: bool = False


class JobRequest(BaseModel):
    """
    Payload for submitting a new job.

    Attributes:
        binary_name (str): The binary to execute.
        arguments (List[str]): List of arguments to pass to the binary.
        paths (List[str]): List of path variables required by the job.
        supported_transports (List[str]): List of transports supported by the client for updates.
        monitor (bool): Whether to enable active client monitoring.
        heartbeat_interval (Optional[int]): Requested heartbeat interval.
    """

    binary_name: str = Field(min_length=1)
    arguments: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    supported_transports: List[str] = Field(min_length=1)
    monitor: bool = False
    heartbeat_interval: Optional[int] = None


class JobRecord(Job, TransportRecord):
    """
    Complete Job record including transport details.
    """

    pass


type CommandStatus = Literal["ok"]


class CommandResponse(BaseModel):
    """
    Generic response for command acknowledgments.
    """

    status: CommandStatus
    detail: Optional[str] = None


class WorkerDeregistration(BaseModel):
    """
    Payload for worker deregistration.
    """

    worker_id: str = ClientId


class JobStatusPayload(BaseModel):
    """
    Payload for job status updates.
    """

    status: JobStatus
    exit_code: Optional[int] = None
    last_update: Optional[datetime] = None


class JobRequestPayload(BaseModel):
    """
    Payload for job requests sent to workers.
    """

    job_id: str
    binary_name: str = Field(min_length=1)
    arguments: List[str]
    paths: List[str]
    heartbeat_interval: int = default_job_heartbeat_interval


type JobStatusUpdateStatus = Literal["completed", "failed", "canceled"]


class JobStatusUpdate(BaseModel):
    """
    Payload for updating job status (completion/failure).
    """

    status: JobStatusUpdateStatus
    exit_code: Optional[int] = None


class LogEntry(BaseModel):
    """
    Represents a single log line from a job.
    """

    stream: Literal["stdout", "stderr"]
    content: str
    timestamp: datetime | None = None


class JobLogsPayload(BaseModel):
    """
    Payload for submitting a batch of job logs.
    """

    logs: List[LogEntry] = Field(min_length=1)


class JobLogsResponse(BaseModel):
    """
    Response containing a batch of job logs and a cursor for the next request.
    """

    logs: List[LogEntry]
    last_message_id: ULID | None = None


type MessageType = Literal["job_status", "job_request", "job_logs"]


class BaseMessage(BaseModel):
    """
    Base class for messages sent between coordinator and clients/workers.
    """

    message_id: ULID = Field(default_factory=ULID)
    sender_id: str | None = OptionalClientId
    recipient_id: str = ClientId
    job_id: ULID | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sent_at: datetime | None = None
    message_type: Any
    payload: Any


class JobStatusMessage(BaseMessage):
    message_type: Literal["job_status"] = "job_status"
    payload: JobStatusPayload


class JobRequestMessage(BaseMessage):
    message_type: Literal["job_request"] = "job_request"
    payload: JobRequestPayload


class JobLogsMessage(BaseMessage):
    message_type: Literal["job_logs"] = "job_logs"
    payload: JobLogsPayload


type Message = Annotated[
    Union[
        Annotated[JobStatusMessage, Tag("job_status")],
        Annotated[JobRequestMessage, Tag("job_request")],
        Annotated[JobLogsMessage, Tag("job_logs")],
    ],
    Discriminator("message_type"),
]


class WorkerBase(BaseModel):
    """
    Base model for Worker attributes.

    Attributes:
        worker_id (str): The unique ID of the worker.
        capabilities (List[str]): List of capabilities (e.g., "h264").
        binaries (List[str]): List of available binaries (e.g., "ffmpeg", "ffprobe").
        paths (List[str]): List of available paths/mounts.
        registration_interval (int): How often the worker is expected to check in (seconds).
        version (Optional[str]): The version of the worker package.
    """

    worker_id: str = ClientId
    capabilities: List[str] = Field(default_factory=list)
    binaries: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    registration_interval: int
    version: Optional[str] = None


type WorkerStatus = Literal["online", "offline", "error"]


class Worker(WorkerBase):
    """
    Represents a Worker's full state.

    Attributes:
        status (Literal): Connection status (online, offline, error).
        last_seen (datetime): Timestamp when the worker was last seen.
    """

    status: WorkerStatus
    last_seen: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WorkerRegistration(WorkerBase):
    """
    Payload for worker registration.

    Attributes:
        supported_transports (List[str]): List of transports supported by the worker.
    """

    supported_transports: List[str] = Field(min_length=1)


class ComponentHealth(BaseModel):
    """
    Represents the health status of a single component.

    Attributes:
        status (Literal["online", "unhealthy"]): The status of the component.
        detail (Optional[str]): Additional details about the status.
    """

    status: Literal["online", "unhealthy"]
    detail: Optional[str] = None


class HealthResponse(BaseModel):
    """
    Standardized response for health check endpoints.

    Attributes:
        status (Literal["online", "unhealthy"]): Overall status of the service.
        databases (Optional[Dict[str, ComponentHealth]]): Health status of database repositories.
        transports (Optional[Dict[str, ComponentHealth]]): Health status of transport implementations.
    """

    status: Literal["online", "unhealthy"]
    databases: Optional[Dict[str, ComponentHealth]] = None
    transports: Optional[Dict[str, ComponentHealth]] = None
