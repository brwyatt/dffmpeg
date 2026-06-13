import asyncio
import contextlib
import json
from collections import defaultdict
from itertools import chain
from logging import getLogger
from typing import Any, AsyncIterator, Dict, Optional, Set

from fastapi import Depends, FastAPI, Header
from fastapi.responses import StreamingResponse
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    BaseMessage,
    ComponentHealth,
    TransportMetadata,
)
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.transports.base import BaseServerTransport

logger = getLogger(__name__)


class HTTPPollingTransport(BaseServerTransport):
    """
    Transport implementation using HTTP Polling.
    Clients poll specific endpoints to receive messages.
    """

    def __init__(
        self, *args, app: FastAPI, base_path: str = "/poll", backend_transport: Optional[str] = None, **kwargs
    ):
        self.app = app
        self.base_path = base_path
        self.job_path = f"{base_path}/jobs/{{job_id}}"
        self.worker_path = f"{base_path}/worker"
        self._draining = False
        self.backend_transport = backend_transport
        self._drain_event = asyncio.Event()

        # Registry for waiting poll connections
        self._job_waiters: Dict[str, Set[asyncio.Event]] = defaultdict(set)
        self._recipient_waiters: Dict[str, Set[asyncio.Event]] = defaultdict(set)

    async def setup(self):
        """
        Sets up the polling endpoints on the FastAPI application.
        """
        self.app.add_api_route(self.job_path, self.handle_job_poll, methods=["GET"])
        self.app.add_api_route(self.worker_path, self.handle_worker_poll, methods=["GET"])

    @contextlib.asynccontextmanager
    async def _wait_context(
        self, identity: AuthenticatedIdentity, job_id: Optional[ULID] = None
    ) -> AsyncIterator[asyncio.Event]:
        """
        Manages the registration of an asyncio.Event for a polling or streaming connection.
        Yields the event and ensures it is properly unregistered when the context closes.
        """
        event = asyncio.Event()

        if job_id:
            self._job_waiters[str(job_id)].add(event)
        else:
            self._recipient_waiters[identity.client_id].add(event)

        try:
            yield event
        except asyncio.CancelledError:
            logger.info(f"Connection closed by {identity.client_id}")
            raise
        finally:
            if job_id:
                jid = str(job_id)
                if jid in self._job_waiters:
                    self._job_waiters[jid].discard(event)
                    if not self._job_waiters[jid]:
                        del self._job_waiters[jid]
            else:
                cid = identity.client_id
                if cid in self._recipient_waiters:
                    self._recipient_waiters[cid].discard(event)
                    if not self._recipient_waiters[cid]:
                        del self._recipient_waiters[cid]

    @contextlib.asynccontextmanager
    async def _backend_client(self, identity: AuthenticatedIdentity, job_id: Optional[ULID] = None):
        """
        Creates, connects, and yields a backend client transport.
        Ensures clean disconnection on exit.
        """
        if not self.backend_transport:
            yield None
            return

        backend = self.app.state.transports[self.backend_transport]
        metadata = None

        # Try to resolve stashed backend metadata from the DB first (requires looking up worker or job)
        db = self.app.state.db
        transport_metadata = None
        if job_id:
            job_rec = await db.jobs.get_job(job_id)
            if job_rec:
                transport_metadata = job_rec.transport_metadata
        else:
            worker_rec = await db.workers.get_worker(identity.client_id)
            if worker_rec:
                transport_metadata = worker_rec.transport_metadata

        if transport_metadata:
            metadata = transport_metadata.get("_backend_metadata")

        # Fallback to computing on-the-fly
        if not metadata:
            metadata = backend.get_metadata(identity.client_id, job_id)

        config = self.app.state.config.transports.get_transport_config(self.backend_transport)
        client_cls = backend.get_client_transport_class()
        client = client_cls(**config)

        await client.connect(metadata)
        try:
            yield client
        finally:
            await client.disconnect()

    async def _poll_loop(
        self,
        identity: AuthenticatedIdentity,
        last_message_id: Optional[ULID] = None,
        wait: Optional[int] = None,
        job_id: Optional[ULID] = None,
    ):
        """
        The abstracted core logic used by both polling endpoints.
        Waits for new messages or a timeout.

        Args:
            identity (AuthenticatedIdentity): The authenticated user.
            last_message_id (Optional[ULID]): Filter for messages newer than this ID.
            wait (int): Timeout in seconds.
            job_id (Optional[ULID]): Filter for job-specific messages.

        Returns:
            dict: A dictionary containing the list of messages.
        """
        if wait is None:
            wait = 0

        if self.backend_transport:
            async with self._backend_client(identity, job_id) as client:
                receive_task = asyncio.create_task(client.receive())
                drain_task = asyncio.create_task(self._drain_event.wait())

                done, pending = await asyncio.wait(
                    [receive_task, drain_task], timeout=wait if wait > 0 else 0.001, return_when=asyncio.FIRST_COMPLETED
                )

                for t in pending:
                    t.cancel()

                if drain_task in done:
                    return {"messages": []}

                if receive_task in done:
                    msg = receive_task.result()
                    return {"messages": [msg]}
                else:
                    return {"messages": []}

        repo: MessageRepository = self.app.state.db.messages
        end_time = asyncio.get_event_loop().time() + wait

        async with self._wait_context(identity, job_id) as event:
            while True:
                messages = await repo.retrieve_messages(
                    recipient_id=identity.client_id,
                    last_message_id=last_message_id,
                    job_id=job_id,
                )

                if messages:
                    return {"messages": messages}

                if self._draining or asyncio.get_event_loop().time() >= end_time:
                    return {"messages": []}

                wait_timeout = min(5, max(0, end_time - asyncio.get_event_loop().time()))
                try:
                    await asyncio.wait_for(event.wait(), timeout=wait_timeout)
                    event.clear()  # Reset for next loop iteration
                except asyncio.TimeoutError:
                    continue  # Regular interval check

    async def _stream_loop(
        self,
        identity: AuthenticatedIdentity,
        last_message_id: Optional[ULID] = None,
        wait: Optional[int] = None,
        job_id: Optional[ULID] = None,
    ):
        if wait is None or wait <= 0:
            wait = 15  # Default keepalive interval

        if self.backend_transport:
            async with self._backend_client(identity, job_id) as client:
                while not self._draining:
                    receive_task = asyncio.create_task(client.receive())
                    drain_task = asyncio.create_task(self._drain_event.wait())

                    done, pending = await asyncio.wait(
                        [receive_task, drain_task], timeout=wait, return_when=asyncio.FIRST_COMPLETED
                    )

                    for t in pending:
                        t.cancel()

                    if drain_task in done:
                        return

                    if receive_task in done:
                        msg = receive_task.result()
                        msgs_dump = [msg.model_dump(mode="json")]
                        yield json.dumps({"messages": msgs_dump}) + "\n"
                    else:
                        logger.debug("Stream keep-alive timeout, sending keep-alive")
                        yield "\n"
            return

        repo: MessageRepository = self.app.state.db.messages

        async with self._wait_context(identity, job_id) as event:
            while True:
                messages = await repo.retrieve_messages(
                    recipient_id=identity.client_id,
                    last_message_id=last_message_id,
                    job_id=job_id,
                )

                if messages:
                    last_message_id = messages[-1].message_id
                    msgs_dump = [msg.model_dump(mode="json") for msg in messages]
                    yield json.dumps({"messages": msgs_dump}) + "\n"

                if self._draining:
                    return

                try:
                    await asyncio.wait_for(event.wait(), timeout=wait)
                    event.clear()
                except asyncio.TimeoutError:
                    logger.debug("Stream keep-alive timeout, sending keep-alive")
                    yield "\n"

    async def handle_job_poll(
        self,
        job_id: ULID,
        last_message_id: Optional[ULID] = None,
        wait: Optional[int] = None,
        accept: Optional[str] = Header(None),
        identity=Depends(required_hmac_auth),
    ):
        """
        Endpoint handler for job-specific polling.
        """
        if accept and "application/x-ndjson" in accept:
            return StreamingResponse(
                self._stream_loop(identity, last_message_id=last_message_id, wait=wait, job_id=job_id),
                media_type="application/x-ndjson",
            )
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait, job_id=job_id)

    async def handle_worker_poll(
        self,
        last_message_id: Optional[ULID] = None,
        wait: Optional[int] = None,
        accept: Optional[str] = Header(None),
        identity=Depends(required_hmac_auth),
    ):
        """
        Endpoint handler for worker polling (general messages).
        """
        if accept and "application/x-ndjson" in accept:
            return StreamingResponse(
                self._stream_loop(identity, last_message_id=last_message_id, wait=wait),
                media_type="application/x-ndjson",
            )
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait)

    async def send_message(self, message: BaseMessage, transport_metadata: Optional[TransportMetadata] = None) -> bool:
        """
        Notifies polling clients that a new message might be available.
        Does not actually 'send' the message payload directly, but triggers a poll check.

        Args:
            message (Message): The message object (already saved to DB).
            transport_metadata (Optional[TransportMetadata]): Metadata for the transport.

        Returns:
            bool: Always True (notification sent).
        """
        if self.backend_transport:
            backend = self.app.state.transports[self.backend_transport]
            metadata = transport_metadata.get("_backend_metadata") if transport_metadata else None
            # Fallback to computing on-the-fly if missing or empty
            if not metadata:
                is_worker_msg = message.message_type in ("job_request", "verify_registration")
                job_id = None if is_worker_msg else message.job_id
                metadata = backend.get_metadata(message.recipient_id, job_id)
            return await backend.send_message(message, transport_metadata=metadata)

        # Notify recipient-specific listeners (e.g. workers)
        if message.recipient_id in self._recipient_waiters:
            for event in self._recipient_waiters[message.recipient_id]:
                event.set()

        # Notify job-specific listeners (e.g. clients watching a job)
        if message.job_id:
            jid = str(message.job_id)
            if jid in self._job_waiters:
                for event in self._job_waiters[jid]:
                    event.set()

        return True

    def get_metadata(self, client_id: str, job_id: Optional[ULID] = None) -> Dict[str, Any]:
        """
        Returns metadata needed for a client to connect/poll (e.g. the path).

        Args:
            client_id (str): The client ID.
            job_id (Optional[str]): The job ID if applicable.

        Returns:
            Dict[str, Any]: Metadata containing the path.
        """
        metadata = {
            "path": self.job_path.format(job_id=job_id) if job_id else self.worker_path,
        }
        if self.backend_transport:
            backend = self.app.state.transports[self.backend_transport]
            metadata["_backend_metadata"] = backend.get_metadata(client_id, job_id)
        return metadata

    async def health_check(self) -> ComponentHealth:
        """
        Check the health of the HTTP polling transport.
        For now, this just means the transport is initialized.
        """
        if self.backend_transport:
            backend = self.app.state.transports[self.backend_transport]
            backend_health = await backend.health_check()
            return ComponentHealth(
                status=backend_health.status,
                detail=f"Backed by {self.backend_transport}: {backend_health.detail or backend_health.status}",
            )
        return ComponentHealth(status="online", detail="HTTP Polling mode (standalone)")

    async def drain(self):
        """
        Wakes up all waiting pollers so they can return and cleanly disconnect.
        """
        self._draining = True
        self._drain_event.set()
        waiter_events = set(chain.from_iterable(self._recipient_waiters.values())).union(
            set(chain.from_iterable(self._job_waiters.values()))
        )
        for event in waiter_events:
            event.set()
