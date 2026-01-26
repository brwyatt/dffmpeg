from typing import Dict, Optional

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB


class SQLiteAuthRepository(AuthRepository, SQLiteDB):
    def __init__(self, *args, tablename: str = "auth", **kwargs):
        AuthRepository.__init__(self, *args, **kwargs)
        SQLiteDB.__init__(self, *args, tablename=tablename, **kwargs)

    async def get_identity(
        self, client_id: str, include_hmac_key: bool = False
    ) -> Optional[AuthenticatedIdentity]:
        result = await self.get_row(
            f"SELECT client_id, role, hmac_key, key_id FROM {self.tablename} WHERE client_id = ?",
            (client_id,),
        )

        if not result:
            return

        hmac_key = result["hmac_key"]
        if include_hmac_key:
            hmac_key = self._decrypt(hmac_key, result["key_id"])

        identity = AuthenticatedIdentity(
            client_id=result["client_id"],
            role=result["role"],
            hmac_key=hmac_key if include_hmac_key else None,
            authenticated=False,
        )
        return identity

    async def add_identity(self, identity: AuthenticatedIdentity) -> None:
        if not identity.hmac_key:
            raise ValueError("hmac_key is required to add an identity")

        encrypted_key, key_id = self._encrypt(identity.hmac_key)
        await self.execute(
            f"INSERT OR REPLACE INTO {self.tablename} (client_id, role, hmac_key, key_id) VALUES (?, ?, ?, ?)",
            (identity.client_id, identity.role, encrypted_key, key_id),
        )

    @property
    def table_create(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.tablename} (
                client_id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                hmac_key TEXT NOT NULL,
                key_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
