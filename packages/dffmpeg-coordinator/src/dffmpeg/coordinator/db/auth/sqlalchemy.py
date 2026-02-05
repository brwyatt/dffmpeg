from typing import Optional

from sqlalchemy import select, update

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB


class SQLAlchemyAuthRepository(AuthRepository, SQLAlchemyDB):
    async def get_identity(self, client_id: str, include_hmac_key: bool = False) -> Optional[AuthenticatedIdentity]:
        query = select(self.table).where(self.table.c.client_id == client_id)
        sql, params = self.compile_query(query)
        result = await self.get_row(sql, params)

        if not result:
            return None

        hmac_key = result["hmac_key"]
        if include_hmac_key:
            hmac_key = self._decrypt(hmac_key, result["key_id"])

        return AuthenticatedIdentity(
            client_id=result["client_id"],
            role=result["role"],
            hmac_key=hmac_key if include_hmac_key else None,
            authenticated=False,
        )

    async def _upsert_identity(self, identity: AuthenticatedIdentity, encrypted_key: str, key_id: str):
        # Portable implementation: SELECT then UPDATE/INSERT
        query = select(self.table.c.client_id).where(self.table.c.client_id == identity.client_id)
        sql, params = self.compile_query(query)
        exists = await self.get_row(sql, params)

        if exists:
            query = (
                update(self.table)
                .where(self.table.c.client_id == identity.client_id)
                .values(role=identity.role, hmac_key=encrypted_key, key_id=key_id)
            )
        else:
            query = self.table.insert().values(
                client_id=identity.client_id,
                role=identity.role,
                hmac_key=encrypted_key,
                key_id=key_id,
            )

        sql, params = self.compile_query(query)
        await self.execute(sql, params)

    async def add_identity(self, identity: AuthenticatedIdentity) -> None:
        if not identity.hmac_key:
            raise ValueError("hmac_key is required to add an identity")

        encrypted_key, key_id = self._encrypt(identity.hmac_key)
        await self._upsert_identity(identity, encrypted_key, key_id)
