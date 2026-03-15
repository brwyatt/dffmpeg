from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from dffmpeg.common.models import AuthenticatedIdentity, JanitorActionRequest
from dffmpeg.coordinator.api.routes.admin import janitor_action


@pytest.mark.asyncio
async def test_janitor_action_admin_role():
    mock_janitor = MagicMock()
    request = JanitorActionRequest(action="clean_workers")
    auth = AuthenticatedIdentity(client_id="localadmin", role="admin", hmac_key="a" * 44)

    response = await janitor_action(request=request, janitor=mock_janitor, auth=auth)

    assert response == {"message": "Action accepted", "action": "clean_workers"}
    mock_janitor.schedule_task.assert_called_once_with("clean_workers", delay=0)


@pytest.mark.asyncio
async def test_janitor_action_forbidden_role():
    mock_janitor = MagicMock()
    request = JanitorActionRequest(action="clean_workers")
    auth = AuthenticatedIdentity(client_id="normaluser", role="client", hmac_key="a" * 44)

    with pytest.raises(HTTPException) as exc:
        await janitor_action(request=request, janitor=mock_janitor, auth=auth)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Admin role required"
    mock_janitor.schedule_task.assert_not_called()
