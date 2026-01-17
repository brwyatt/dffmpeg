from types import CoroutineType
from typing import Any, Iterable, Tuple
import aiosqlite
import asyncio


class SQLiteDB():
    def __init__(self, path: str, tablename: str):
        self.path = path
        self.tablename = tablename
        asyncio.run(self._init_db())

    async def _init_db(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(self.table_create)
            await db.commit()

    async def get_rows(self, query: str, params: Tuple[str]) -> Iterable[aiosqlite.Row]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def get_row(self, query: str, params: Tuple[str]) -> aiosqlite.Row | None:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    @property
    def table_create(self) -> str:
        raise NotImplemented()
