import ipaddress
import os
import secrets
import shutil
import tempfile

import pytest
import pytest_asyncio

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db import DB, DBConfig


@pytest_asyncio.fixture
async def db():
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.db")

    # Using a dictionary for config structure as per the previous file
    config = DBConfig(
        defaults={"engine": "sqlite", "path": db_path},
        repositories={"auth": {}, "jobs": {}, "messages": {}, "workers": {}},
    )

    db = DB(config)
    await db.setup_all()

    yield db

    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_authenticated_identity_default_scope():
    """
    Verify that a new AuthenticatedIdentity defaults allowed_cidrs to ["0.0.0.0/0", "::/0"].
    """
    key = secrets.token_urlsafe(33)
    identity = AuthenticatedIdentity(client_id="test_default", role="client", hmac_key=key)
    assert len(identity.allowed_cidrs) == 2
    assert ipaddress.IPv4Network("0.0.0.0/0") in identity.allowed_cidrs
    assert ipaddress.IPv6Network("::/0") in identity.allowed_cidrs


@pytest.mark.asyncio
async def test_bootstrap_local_admin(db: DB):
    """
    Verify that bootstrap_local_admin creates a user scoped to ["127.0.0.0/8", "::1/128"].
    """
    # setup_all already called bootstrap_local_admin

    admin = await db.auth.get_identity("localadmin", include_hmac_key=True)
    assert admin is not None
    assert admin.client_id == "localadmin"
    assert ipaddress.IPv4Network("127.0.0.0/8") in admin.allowed_cidrs
    assert ipaddress.IPv6Network("::1/128") in admin.allowed_cidrs
    assert len(admin.allowed_cidrs) == 2
    assert len(str(admin.hmac_key)) == 44


@pytest.mark.asyncio
async def test_custom_scope_storage(db: DB):
    """
    Verify that custom scopes are stored and retrieved correctly.
    """
    key = secrets.token_urlsafe(33)
    user = AuthenticatedIdentity(
        client_id="custom_user",
        role="client",
        hmac_key=key,
        allowed_cidrs=[ipaddress.ip_network("192.168.1.0/24"), ipaddress.ip_network("10.0.0.0/8")],
    )

    await db.auth.add_identity(user)

    fetched = await db.auth.get_identity("custom_user")
    assert fetched is not None
    assert len(fetched.allowed_cidrs) == 2
    assert ipaddress.IPv4Network("192.168.1.0/24") in fetched.allowed_cidrs
    assert ipaddress.IPv4Network("10.0.0.0/8") in fetched.allowed_cidrs


@pytest.mark.asyncio
async def test_list_identities_scopes(db: DB):
    """
    Verify that scopes are loaded correctly when listing identities.
    """
    key1 = secrets.token_urlsafe(33)
    user1 = AuthenticatedIdentity(
        client_id="user1",
        role="client",
        hmac_key=key1,
        # Default is global (0.0.0.0/0, ::/0)
    )
    await db.auth.add_identity(user1)

    key2 = secrets.token_urlsafe(33)
    user2 = AuthenticatedIdentity(
        client_id="user2",
        role="client",
        hmac_key=key2,
        allowed_cidrs=[ipaddress.ip_network("127.0.0.1/32"), ipaddress.ip_network("10.0.0.1/32")],
    )
    await db.auth.add_identity(user2)

    users = await db.auth.list_identities()
    # list_identities returns a list, not a dict. We need to convert or iterate.
    # The previous test used: user_map = {u.client_id: u for u in users}

    user_map = {u.client_id: u for u in users}

    assert "user1" in user_map
    assert len(user_map["user1"].allowed_cidrs) == 2
    assert ipaddress.IPv4Network("0.0.0.0/0") in user_map["user1"].allowed_cidrs
    assert ipaddress.IPv6Network("::/0") in user_map["user1"].allowed_cidrs

    assert "user2" in user_map
    assert len(user_map["user2"].allowed_cidrs) == 2
    assert ipaddress.IPv4Network("127.0.0.1/32") in user_map["user2"].allowed_cidrs
    assert ipaddress.IPv4Network("10.0.0.1/32") in user_map["user2"].allowed_cidrs

    # Check localadmin as well
    assert "localadmin" in user_map
    assert len(user_map["localadmin"].allowed_cidrs) == 2
    assert ipaddress.IPv4Network("127.0.0.0/8") in user_map["localadmin"].allowed_cidrs
    assert ipaddress.IPv6Network("::1/128") in user_map["localadmin"].allowed_cidrs
