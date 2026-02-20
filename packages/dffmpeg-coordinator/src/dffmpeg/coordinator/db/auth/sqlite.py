from typing import Optional

from sqlalchemy.dialects.sqlite import insert

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth.sqlalchemy import SQLAlchemyAuthRepository
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB


class SQLiteAuthRepository(SQLAlchemyAuthRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "auth", **kwargs):
        # Initialize generic base (AuthRepository)
        SQLAlchemyAuthRepository.__init__(self, *args, **kwargs)
        # Initialize engine (SQLiteDB)
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def _upsert_identity(self, identity: AuthenticatedIdentity, encrypted_key: str, key_id: Optional[str]):
        identity_data = identity.model_dump(mode="json")

        stmt = (
            insert(self.table)
            .values(
                client_id=identity_data["client_id"],
                role=identity_data["role"],
                hmac_key=encrypted_key,
                key_id=key_id,
                allowed_cidrs=identity_data["allowed_cidrs"],
            )
            .on_conflict_do_update(
                index_elements=["client_id"],
                set_=dict(
                    role=identity_data["role"],
                    hmac_key=encrypted_key,
                    key_id=key_id,
                    allowed_cidrs=identity_data["allowed_cidrs"],
                ),
            )
        )
        sql, params = self.compile_query(stmt)
        await self.execute(sql, params)
