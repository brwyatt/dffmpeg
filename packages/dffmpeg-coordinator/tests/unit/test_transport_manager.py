from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import BaseMessage, TransportRecord
from dffmpeg.coordinator.transports import TransportConfig, TransportManager
from dffmpeg.coordinator.transports.base import BaseServerTransport


class MockTransport(BaseServerTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_called = False

    async def setup(self):
        self.setup_called = True

    async def health_check(self):
        from dffmpeg.common.models import ComponentHealth

        return ComponentHealth(status="online")

    def get_metadata(self, client_id, job_id=None):
        return {"id": client_id, "job": str(job_id) if job_id else None}

    async def send_message(self, message, transport_metadata=None):
        return True


@pytest.mark.asyncio
async def test_transport_manager_recipient_aware_routing():
    app = MagicMock()
    config = TransportConfig(enabled_transports=["mock"])

    # Mock the loader to return our mock transport
    ep_mock = MagicMock()
    ep_mock.name = "mock"
    ep_mock.load.return_value = MockTransport

    with patch("dffmpeg.coordinator.transports.entry_points", return_value=[ep_mock]):
        with MagicMock() as mock_db:
            app.state.db = mock_db
            mock_db.messages.add_message = AsyncMock()

            manager = TransportManager(config, app)

            message = MagicMock(spec=BaseMessage)
            message.message_id = ULID()
            message.recipient_id = "worker1"
            message.job_id = ULID()

            # Scenario 1: Recipient is a Worker
            mock_db.workers.get_transport = AsyncMock(
                return_value=TransportRecord(transport="mock", transport_metadata={"topic": "worker_topic"})
            )
            mock_db.jobs.get_transport = AsyncMock(
                return_value=TransportRecord(transport="mock", transport_metadata={"topic": "job_topic"})
            )

            await manager.send_message(message)

            # Should check worker first
            mock_db.workers.get_transport.assert_called_once_with("worker1")
            # Should NOT have fallen back to job transport because worker was found
            mock_db.jobs.get_transport.assert_not_called()

            # Scenario 2: Recipient is NOT a worker (e.g. a Client)
            mock_db.workers.get_transport.reset_mock()
            mock_db.jobs.get_transport.reset_mock()
            mock_db.workers.get_transport = AsyncMock(return_value=None)
            mock_db.jobs.get_transport = AsyncMock(
                return_value=TransportRecord(transport="mock", transport_metadata={"topic": "job_topic"})
            )

            await manager.send_message(message)

            # Should check worker first
            mock_db.workers.get_transport.assert_called_with("worker1")
            # Should have fallen back to job transport
            mock_db.jobs.get_transport.assert_called_once_with(message.job_id)


@pytest.mark.asyncio
async def test_transport_manager_opt_in_loading():
    app = MagicMock()

    # Mock entry points
    ep_http = MagicMock()
    ep_http.name = "http_polling"
    ep_http.load.return_value = MockTransport
    ep_mqtt = MagicMock()
    ep_mqtt.name = "mqtt"
    ep_mqtt.load.return_value = MockTransport

    with patch("dffmpeg.coordinator.transports.entry_points", return_value=[ep_http, ep_mqtt]):
        # Scenario 1: No enabled_transports -> defaults to http_polling
        config1 = TransportConfig(enabled_transports=[])
        manager1 = TransportManager(config1, app)
        assert list(manager1.loaded_transports.keys()) == ["http_polling"]

        # Scenario 2: Explicit list -> priority and filtering
        config2 = TransportConfig(enabled_transports=["mqtt", "http_polling"])
        manager2 = TransportManager(config2, app)
        assert list(manager2.loaded_transports.keys()) == ["mqtt", "http_polling"]

        # Scenario 3: Filter unknown
        config3 = TransportConfig(enabled_transports=["mqtt", "nonexistent"])
        manager3 = TransportManager(config3, app)
        assert list(manager3.loaded_transports.keys()) == ["mqtt"]
