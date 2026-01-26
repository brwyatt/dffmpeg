import pytest
import aiosqlite
import os
from base64 import b64encode
from dffmpeg.coordinator.db.auth.sqlite import SQLiteAuthRepository

@pytest.fixture
async def auth_repo(tmp_path):
    db_path = tmp_path / "test_auth.db"
    # Setup some encryption keys
    key_id = "1"
    raw_key = b64encode(os.urandom(32)).decode('ascii')
    encryption_keys = {
        key_id: f"fernet:{raw_key}"
    }
    
    repo = SQLiteAuthRepository(
        engine="sqlite",
        path=str(db_path),
        encryption_keys=encryption_keys,
        default_encryption_key_id=key_id,
    )
    await repo.setup()
    return repo

@pytest.mark.anyio
async def test_sqlite_auth_encryption_flow(auth_repo):
    client_id = "test_client"
    role = "client"
    raw_hmac_key = b64encode(os.urandom(32)).decode("ascii")
    
    # Manually insert an encrypted key into the DB to test retrieval
    # Normally we'd have a 'create_identity' method, but for now we test retrieval
    encrypted_key, key_id = auth_repo._encrypt(raw_hmac_key)
    
    async with aiosqlite.connect(auth_repo.path) as db:
        await db.execute(
            f"INSERT INTO {auth_repo.tablename} (client_id, role, hmac_key, key_id) VALUES (?, ?, ?, ?)",
            (client_id, role, encrypted_key, key_id)
        )
        await db.commit()
    
    # Now retrieve it
    identity = await auth_repo.get_identity(client_id, include_hmac_key=True)
    assert identity is not None
    assert identity.client_id == client_id
    assert identity.hmac_key == raw_hmac_key # Should be decrypted!

@pytest.mark.anyio
async def test_sqlite_auth_plain_compatibility(auth_repo):
    # Test that it can still read unencrypted keys (key_id is NULL or empty)
    client_id = "plain_client"
    role = "worker"
    raw_hmac_key = b64encode(os.urandom(32)).decode("ascii")
    
    async with aiosqlite.connect(auth_repo.path) as db:
        await db.execute(
            f"INSERT INTO {auth_repo.tablename} (client_id, role, hmac_key, key_id) VALUES (?, ?, ?, ?)",
            (client_id, role, raw_hmac_key, None)
        )
        await db.commit()
        
    identity = await auth_repo.get_identity(client_id, include_hmac_key=True)
    assert identity is not None
    assert identity.hmac_key == raw_hmac_key
