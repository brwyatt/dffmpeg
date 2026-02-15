from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import TIMESTAMP, Column, MetaData, String, Table, func

from dffmpeg.common.crypto import CryptoManager
from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class AuthRepository(BaseDB):
    metadata = MetaData()
    table = Table(
        "auth",
        metadata,
        Column("client_id", String(255), primary_key=True),
        Column("role", String(50), nullable=False),
        Column("hmac_key", String(255), nullable=False),
        Column("key_id", String(255), nullable=True),
        Column("created_at", TIMESTAMP, server_default=func.current_timestamp()),
    )

    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.auth", engine, cls))

    def __init__(
        self,
        *args,
        encryption_keys: Optional[Dict[str, str]] = None,
        default_encryption_key_id: Optional[str] = None,
        **kwargs,
    ):
        self._crypto = CryptoManager(encryption_keys or {})
        self._default_key_id = default_encryption_key_id

    async def get_identity(self, client_id: str, include_hmac_key: bool = False) -> Optional[AuthenticatedIdentity]:
        raise NotImplementedError()

    async def add_identity(self, identity: AuthenticatedIdentity) -> None:
        raise NotImplementedError()

    async def list_identities(self, include_hmac_key: bool = False) -> Iterable[AuthenticatedIdentity]:
        raise NotImplementedError()

    async def delete_identity(self, client_id: str) -> bool:
        raise NotImplementedError()

    async def reencrypt_identity(self, client_id: str, key_id: Optional[str] = None, decrypt: bool = False) -> bool:
        raise NotImplementedError()

    async def get_identities_not_using_key(self, key_id: Optional[str] = None, limit: int = 100) -> Iterable[str]:
        raise NotImplementedError()

    def _encrypt(self, hmac_key: str, key_id: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Encrypts an HMAC key using the specified or default encryption key.
        Returns (encrypted_b64, key_id).
        """
        key_id = key_id or self._default_key_id
        if not key_id:
            # If no encryption configured, return as is (plain)
            return hmac_key, None

        return self._crypto.encrypt(hmac_key, key_id), key_id

    def _decrypt(self, encrypted_hmac_key: str, key_id: str | None) -> str:
        """
        Decrypts an HMAC key using the specified key_id.
        """
        if not key_id:
            return encrypted_hmac_key

        return self._crypto.decrypt(encrypted_hmac_key, key_id)
