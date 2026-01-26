from typing import Dict, Optional, Tuple

from dffmpeg.common.crypto import CryptoManager
from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class AuthRepository(BaseDB):
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

    async def get_identity(
        self, client_id: str, include_hmac_key: bool = False
    ) -> Optional[AuthenticatedIdentity]:
        raise NotImplementedError()

    def _encrypt(self, hmac_key: str, key_id: Optional[str] = None) -> Tuple[str, str]:
        """
        Encrypts an HMAC key using the specified or default encryption key.
        Returns (encrypted_b64, key_id).
        """
        key_id = key_id or self._default_key_id
        if not key_id:
            # If no encryption configured, return as is (plain)
            return hmac_key, ""

        return self._crypto.encrypt(hmac_key, key_id), key_id

    def _decrypt(self, encrypted_hmac_key: str, key_id: str | None) -> str:
        """
        Decrypts an HMAC key using the specified key_id.
        """
        if not key_id:
            return encrypted_hmac_key

        return self._crypto.decrypt(encrypted_hmac_key, key_id)
