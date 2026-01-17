from typing import Optional

from dffmpeg.common.models import AuthenticatedIdentity

from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB


class SQLiteAuthRepository(AuthRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "auth", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def get_identity(self, client_id: str, include_hmac_key: bool = False) -> Optional[AuthenticatedIdentity]:
        result = await self.get_row(
            f"SELECT client_id, role, hmac_key FROM {self.tablename} WHERE client_id = ?",
            (client_id,)
        )

        if not result:
            return

        identity = AuthenticatedIdentity(
            client_id=result["client_id"],
            role=result["role"],
            hmac_key=result["hmac_key"] if include_hmac_key else None,
            authenticated=False,
        )
        return identity

    @property
    def table_create(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.tablename} (
                client_id TEXT PRIMARY KEY,
                role TEXT NOT NULL,
                hmac_key TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
