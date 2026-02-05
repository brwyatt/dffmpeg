import json
import sqlite3
from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional

import aiosqlite
from sqlalchemy.dialects import sqlite

from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB

sql_types = str | int | float | datetime | None


sqlite3.register_adapter(date, lambda x: x.isoformat())
sqlite3.register_adapter(datetime, lambda x: x.isoformat())
sqlite3.register_adapter(list, lambda x: json.dumps(x))
sqlite3.register_adapter(dict, lambda x: json.dumps(x))
sqlite3.register_converter("date", lambda x: date.fromisoformat(x.decode()))
sqlite3.register_converter("datetime", lambda x: datetime.fromisoformat(x.decode()))
sqlite3.register_converter("TIMESTAMP", lambda x: datetime.fromisoformat(x.decode()))


class SQLiteDB(SQLAlchemyDB):
    """
    SQLite-specific implementation of the SQLAlchemyDB engine.
    Provides methods for executing queries and managing connections using aiosqlite.

    Attributes:
        path (str): File path to the SQLite database.
        tablename (str): Name of the table this repository manages.
    """

    def __init__(self, *args, path: str, tablename: str, **kwargs):
        self.path = path
        self.tablename = tablename
        self._dialect = sqlite.dialect(paramstyle="named")

    @property
    def dialect(self):
        return self._dialect

    def _connect(self):
        return aiosqlite.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    async def setup(self):
        """
        Initializes the database by creating the table if it doesn't exist.
        """
        await self.execute(self.table_create)

    async def get_rows(self, query: str, params: Optional[Iterable[sql_types]] = None) -> Iterable[Dict[str, Any]]:
        """
        Executes a SELECT query and returns all matching rows.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[sql_types]]): Parameters to substitute into the query.

        Returns:
            Iterable[Dict[str, Any]]: The resulting rows.
        """
        if params is None:
            params = tuple()

        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_row(self, query: str, params: Optional[Iterable[sql_types]] = None) -> Optional[Dict[str, Any]]:
        """
        Executes a SELECT query and returns the first matching row.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[sql_types]]): Parameters to substitute into the query.

        Returns:
            Optional[Dict[str, Any]]: The resulting row, or None if no match found.
        """
        if params is None:
            params = tuple()

        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def execute(self, query: str, params: Optional[Iterable[sql_types]] = None) -> None:
        """
        Executes a write operation (INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[sql_types]]): Parameters to substitute into the query.
        """
        if params is None:
            params = tuple()

        async with self._connect() as db:
            await db.execute(query, params)
            await db.commit()

    async def execute_and_return_rowcount(self, query: str, params: Optional[Iterable[sql_types]] = None) -> int:
        """
        Executes a write operation and returns the number of affected rows.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[sql_types]]): Parameters to substitute into the query.

        Returns:
            int: The number of affected rows.
        """
        if params is None:
            params = tuple()

        async with self._connect() as db:
            cursor = await db.execute(query, params)
            await db.commit()
            return cursor.rowcount
