import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field
from ulid import ULID

ClientId: str = Field(min_length=1)
OptionalClientId: str | None = Field(default=None, min_length=1)


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
    role: Literal["client", "worker", "admin"]
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


JobStatus = Literal["pending", "assigned", "running", "completed", "failed", "canceled", "canceling"]


class Job(BaseModel):
    """
    Represents a FFmpeg job.

    Attributes:
        job_id (ULID): Unique identifier for the job.
        requester_id (str): The client ID who requested the job.
        binary_name (Literal["ffmpeg"]): The binary to execute.
        arguments (List[str]): List of arguments to pass to the binary.
        paths (List[str]): List of path variables required by the job.
        status (Literal): Current status of the job.
        worker_id (Optional[str]): The worker assigned to the job, if any.
        created_at (datetime): Timestamp of creation.
        last_update (datetime): Timestamp of last update.
    """

    job_id: ULID = Field(default_factory=ULID)
    requester_id: str = ClientId
    binary_name: Literal["ffmpeg"]
    arguments: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    status: JobStatus
    worker_id: str | None = OptionalClientId
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class JobRequest(BaseModel):
    """
    Payload for submitting a new job.

    Attributes:
        binary_name (Literal["ffmpeg"]): The binary to execute.
        arguments (List[str]): List of arguments to pass to the binary.
        paths (List[str]): List of path variables required by the job.
        supported_transports (List[str]): List of transports supported by the client for updates.
    """

    binary_name: Literal["ffmpeg"]
    arguments: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)
    supported_transports: List[str] = Field(min_length=1)


class JobStatusUpdate(BaseModel):
    """
    Payload for updating job status (completion/failure).
    """

    status: Literal["completed", "failed", "canceled"]


MessageType = Literal["job_status", "job_request"]


class Message(BaseModel):
    """
    Represents a message sent between coordinator and clients/workers.

    Attributes:
        message_id (ULID): Unique identifier for the message.
        recipient_id (str): The ID of the recipient.
        job_id (Optional[ULID]): Associated job ID, if any.
        timestamp (datetime): When the message was created.
        message_type (Literal): Type of message (job_status, job_request).
        payload (Union[Dict, List, str]): The message content.
        sent_at (Optional[datetime]): When the message was actually sent/delivered.
    """

    message_id: ULID = Field(default_factory=ULID)
    recipient_id: str = ClientId
    job_id: ULID | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message_type: MessageType
    payload: Dict | List | str
    sent_at: datetime | None = None


class WorkerBase(BaseModel):
    """
    Base model for Worker attributes.

    Attributes:
        worker_id (str): The unique ID of the worker.
        capabilities (List[str]): List of capabilities (e.g., "h264").
        binaries (List[str]): List of available binaries (e.g., "ffmpeg", "ffprobe").
        paths (List[str]): List of available paths/mounts.
    """

    worker_id: str = ClientId
    capabilities: List[str] = Field(default_factory=list)
    binaries: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)


WorkerStatus = Literal["online", "offline", "error"]


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
