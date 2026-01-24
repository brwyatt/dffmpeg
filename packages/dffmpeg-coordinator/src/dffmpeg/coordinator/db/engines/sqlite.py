from datetime import datetime
from typing import Iterable

import aiosqlite

from dffmpeg.coordinator.db.engines import BaseDB

sql_types = str | int | datetime | None


class SQLiteDB(BaseDB):
    """
    SQLite implementation of the BaseDB engine.
    Provides methods for executing queries and managing connections using aiosqlite.

    Attributes:
        path (str): File path to the SQLite database.
        tablename (str): Name of the table this repository manages.
    """

    def __init__(self, path: str, tablename: str):
        self.path = path
        self.tablename = tablename

    async def setup(self):
        """
        Initializes the database by creating the table if it doesn't exist.
        """
        async with aiosqlite.connect(self.path) as db:
            await db.execute(self.table_create)
            await db.commit()

    async def get_rows(self, query: str, params: Iterable[sql_types]) -> Iterable[aiosqlite.Row]:
        """
        Executes a SELECT query and returns all matching rows.

        Args:
            query (str): The SQL query string.
            params (Iterable[sql_types]): Parameters to substitute into the query.

        Returns:
            Iterable[aiosqlite.Row]: The resulting rows.
        """
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchall()

    async def get_row(self, query: str, params: Iterable[sql_types]) -> aiosqlite.Row | None:
        """
        Executes a SELECT query and returns the first matching row.

        Args:
            query (str): The SQL query string.
            params (Iterable[sql_types]): Parameters to substitute into the query.

        Returns:
            Optional[aiosqlite.Row]: The resulting row, or None if no match found.
        """
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def execute(self, query: str, params: Iterable[sql_types]) -> None:
        """
        Executes a write operation (INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL query string.
            params (Iterable[sql_types]): Parameters to substitute into the query.
        """
        async with aiosqlite.connect(self.path) as db:
            await db.execute(query, params)
            await db.commit()

    @property
    def table_create(self) -> str:
        """
        Abstract property that should return the CREATE TABLE SQL statement.
        """
        raise NotImplementedError()
