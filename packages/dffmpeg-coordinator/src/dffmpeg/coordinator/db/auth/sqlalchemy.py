from typing import Iterable, Optional

from sqlalchemy import delete, or_, select, update

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

    async def _upsert_identity(self, identity: AuthenticatedIdentity, encrypted_key: str, key_id: Optional[str]):
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

    async def list_identities(self, include_hmac_key: bool = False) -> Iterable[AuthenticatedIdentity]:
        query = select(self.table)
        sql, params = self.compile_query(query)
        results = await self.get_rows(sql, params)

        identities = []
        for row in results:
            hmac_key = row["hmac_key"]
            if include_hmac_key:
                hmac_key = self._decrypt(hmac_key, row["key_id"])

            identities.append(
                AuthenticatedIdentity(
                    client_id=row["client_id"],
                    role=row["role"],
                    hmac_key=hmac_key if include_hmac_key else None,
                    authenticated=False,
                )
            )
        return identities

    async def delete_identity(self, client_id: str) -> bool:
        query = delete(self.table).where(self.table.c.client_id == client_id)
        sql, params = self.compile_query(query)
        count = await self.execute_and_return_rowcount(sql, params)
        return count > 0

    async def reencrypt_identity(self, client_id: str, key_id: Optional[str] = None, decrypt: bool = False) -> bool:
        identity = await self.get_identity(client_id, include_hmac_key=True)
        if not identity:
            return False

        if decrypt:
            encrypted_key = identity.hmac_key
            key_id = None
        else:
            encrypted_key, key_id = self._encrypt(str(identity.hmac_key), key_id)

        await self._upsert_identity(identity, str(encrypted_key), key_id)
        return True

    async def get_identities_not_using_key(self, key_id: Optional[str] = None, limit: int = 100) -> Iterable[str]:
        query = select(self.table.c.client_id).limit(limit)

        if not key_id:
            # Find records where key_id IS NOT NULL AND key_id != ''
            query = query.where(self.table.c.key_id.is_not(None)).where(self.table.c.key_id != "")
        else:
            # Find records where key_id != key_id OR key_id IS NULL
            query = query.where(
                or_(
                    self.table.c.key_id != key_id,
                    self.table.c.key_id.is_(None),
                )
            )

        sql, params = self.compile_query(query)
        results = await self.get_rows(sql, params)
        return [row["client_id"] for row in results]
