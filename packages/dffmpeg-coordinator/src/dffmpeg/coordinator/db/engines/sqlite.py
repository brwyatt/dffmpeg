from datetime import datetime
from typing import Iterable, Tuple
import aiosqlite

from dffmpeg.coordinator.db.engines import BaseDB


sql_types = str | int | datetime | None


class SQLiteDB(BaseDB):
    def __init__(self, path: str, tablename: str):
        self.path = path
        self.tablename = tablename

    async def setup(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(self.table_create)
            await db.commit()

    async def get_rows(self, query: str, params: Iterable[sql_types]) -> Iterable[aiosqlite.Row]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def get_row(self, query: str, params: Iterable[sql_types]) -> aiosqlite.Row | None:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def execute(self, query: str, params: Iterable[sql_types]) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(query, params)

    @property
    def table_create(self) -> str:
        raise NotImplementedError()
