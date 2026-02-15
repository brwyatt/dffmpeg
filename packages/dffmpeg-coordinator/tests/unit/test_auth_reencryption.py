import os
from base64 import b64encode
from typing import Dict

import aiosqlite
import pytest
import pytest_asyncio

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth.sqlite import SQLiteAuthRepository


@pytest_asyncio.fixture
async def reencrypt_repo(tmp_path):
    db_path = tmp_path / "test_reencrypt.db"

    # Generate 3 keys
    keys: Dict[str, str] = {}
    for i in range(1, 4):
        raw_key = b64encode(os.urandom(32)).decode("ascii")
        keys[f"key_{i}"] = f"fernet:{raw_key}"

    repo = SQLiteAuthRepository(
        engine="sqlite",
        path=str(db_path),
        encryption_keys=keys,
        default_encryption_key_id="key_1",
    )
    await repo.setup()
    return repo


@pytest.mark.anyio
async def test_reencrypt_identity(reencrypt_repo: SQLiteAuthRepository):
    client_id = "test_client"
    role = "client"
    # 44 chars exactly
    hmac_key = "12345678901234567890123456789012345678901234"

    # Create identity with default key (key_1)
    identity = AuthenticatedIdentity(client_id=client_id, role=role, hmac_key=hmac_key, authenticated=False)
    await reencrypt_repo.add_identity(identity)

    # Verify initial state (key_id="key_1")
    async with aiosqlite.connect(reencrypt_repo.path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT key_id, hmac_key FROM auth WHERE client_id = ?", (client_id,)) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row["key_id"] == "key_1"
            assert row["hmac_key"] != hmac_key  # Should be encrypted

    # Re-encrypt with key_2
    success = await reencrypt_repo.reencrypt_identity(client_id, key_id="key_2")
    assert success is True

    # Verify new state (key_id="key_2")
    async with aiosqlite.connect(reencrypt_repo.path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT key_id, hmac_key FROM auth WHERE client_id = ?", (client_id,)) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row["key_id"] == "key_2"

    # Verify we can still read it
    fetched = await reencrypt_repo.get_identity(client_id, include_hmac_key=True)
    assert fetched is not None
    assert fetched.hmac_key == hmac_key


@pytest.mark.anyio
async def test_decrypt_identity(reencrypt_repo: SQLiteAuthRepository):
    client_id = "test_client_decrypt"
    hmac_key = "12345678901234567890123456789012345678901234"

    identity = AuthenticatedIdentity(client_id=client_id, role="client", hmac_key=hmac_key)
    await reencrypt_repo.add_identity(identity)

    # Decrypt (remove encryption)
    await reencrypt_repo.reencrypt_identity(client_id, decrypt=True)

    # Verify state (key_id=None, hmac_key=plain)
    async with aiosqlite.connect(reencrypt_repo.path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT key_id, hmac_key FROM auth WHERE client_id = ?", (client_id,)) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row["key_id"] is None
            assert row["hmac_key"] == hmac_key

    # Verify retrieval works
    fetched = await reencrypt_repo.get_identity(client_id, include_hmac_key=True)
    assert fetched is not None
    assert fetched.hmac_key == hmac_key


@pytest.mark.anyio
async def test_get_identities_not_using_key(reencrypt_repo: SQLiteAuthRepository):
    # Add 3 identities:
    # 1. key_1 (default)
    # 2. key_2
    # 3. None (plain)

    valid_key = "12345678901234567890123456789012345678901234"

    id1 = AuthenticatedIdentity(client_id="c1", role="client", hmac_key=valid_key)
    await reencrypt_repo.add_identity(id1)  # Uses default key_1

    id2 = AuthenticatedIdentity(client_id="c2", role="client", hmac_key=valid_key)
    # Manually encrypt with key_2
    enc_key_2, _ = reencrypt_repo._encrypt(valid_key, "key_2")
    await reencrypt_repo._upsert_identity(id2, enc_key_2, "key_2")

    id3 = AuthenticatedIdentity(client_id="c3", role="client", hmac_key=valid_key)
    # Manually insert plain
    await reencrypt_repo._upsert_identity(id3, valid_key, None)

    # Test: Find everything NOT using key_1 (Expect c2, c3)
    results = await reencrypt_repo.get_identities_not_using_key("key_1")
    client_ids = sorted(list(results))
    assert client_ids == ["c2", "c3"]

    # Test: Find everything NOT using key_2 (Expect c1, c3)
    results = await reencrypt_repo.get_identities_not_using_key("key_2")
    client_ids = sorted(list(results))
    assert client_ids == ["c1", "c3"]

    # Test: Find everything NOT using None (Expect c1, c2)
    # Finding encrypted records basically
    results = await reencrypt_repo.get_identities_not_using_key(None)
    client_ids = sorted(list(results))
    assert client_ids == ["c1", "c2"]

    # Test: Find everything NOT using key_3 (Expect c1, c2, c3) - key_3 doesn't exist but logic holds
    results = await reencrypt_repo.get_identities_not_using_key("key_3")
    client_ids = sorted(list(results))
    assert client_ids == ["c1", "c2", "c3"]
