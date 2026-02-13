import json
import ssl
from typing import Any, Dict, Iterable, Optional

import aiomysql
from sqlalchemy.dialects import mysql

from dffmpeg.common.models import ComponentHealth
from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB


class MySQLDB(SQLAlchemyDB):
    """
    MySQL/MariaDB-specific implementation of the SQLAlchemyDB engine.
    Provides methods for executing queries and managing connections using aiomysql.

    Attributes:
        host (str): Hostname of the MySQL server.
        port (int): Port of the MySQL server.
        user (str): Username for authentication.
        password (str): Password for authentication.
        database (str): Name of the database.
        tablename (str): Name of the table this repository manages.
        use_ssl (bool): Whether to use SSL for the connection.
        ssl_ca (str): Path to CA certificate.
        ssl_cert (str): Path to client certificate (mTLS).
        ssl_key (str): Path to client private key (mTLS).
        ssl_verify (bool): Whether to verify server certificate.
    """

    def __init__(
        self,
        *args,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "dffmpeg",
        tablename: str,
        use_ssl: bool = False,
        ssl_ca: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        ssl_verify: bool = True,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.tablename = tablename
        self.use_ssl = use_ssl
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.ssl_verify = ssl_verify
        self._dialect = mysql.dialect(paramstyle="pyformat")

    @property
    def dialect(self):
        return self._dialect

    def _connect(self):
        ssl_config = None
        if self.use_ssl or self.ssl_ca or self.ssl_cert or self.ssl_key:
            # Use system CAs by default if cafile is not provided
            ssl_config = ssl.create_default_context(cafile=self.ssl_ca)

            # Support for mTLS if both cert and key are provided
            if self.ssl_cert and self.ssl_key:
                ssl_config.load_cert_chain(certfile=self.ssl_cert, keyfile=self.ssl_key)

            if not self.ssl_verify:
                ssl_config.check_hostname = False
                ssl_config.verify_mode = ssl.CERT_NONE

        return aiomysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            autocommit=True,
            ssl=ssl_config,
        )

    async def setup(self) -> None:
        """
        Initializes the database by creating the table if it doesn't exist.
        """
        await self.execute(self.table_create)

    def _serialize_params(self, params: Optional[Iterable[Any]]) -> Optional[Iterable[Any]]:
        """
        Serializes complex types (dict, list) in params to JSON strings for aiomysql.
        Handles both dictionary-style and sequence-style parameters.
        """
        if params is None:
            return None

        if isinstance(params, dict):
            return {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in params.items()}

        if isinstance(params, (list, tuple)):
            return [(json.dumps(v) if isinstance(v, (dict, list)) else v) for v in params]

        return params

    async def get_rows(self, query: str, params: Optional[Iterable[Any]] = None) -> Iterable[Dict[str, Any]]:
        """
        Executes a SELECT query and returns all matching rows.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[Any]]): Parameters to substitute into the query.

        Returns:
            Iterable[Dict[str, Any]]: The resulting rows.
        """
        params = self._serialize_params(params)
        async with await self._connect() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params or ())
                return await cursor.fetchall()

    async def get_row(self, query: str, params: Optional[Iterable[Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Executes a SELECT query and returns the first matching row.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[Any]]): Parameters to substitute into the query.

        Returns:
            Optional[Dict[str, Any]]: The resulting row, or None if no match found.
        """
        params = self._serialize_params(params)
        async with await self._connect() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params or ())
                return await cursor.fetchone()

    async def execute(self, query: str, params: Optional[Iterable[Any]] = None) -> None:
        """
        Executes a write operation (INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[Any]]): Parameters to substitute into the query.
        """
        params = self._serialize_params(params)
        async with await self._connect() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())

    async def execute_and_return_rowcount(self, query: str, params: Optional[Iterable[Any]] = None) -> int:
        """
        Executes a write operation and returns the number of affected rows.

        Args:
            query (str): The SQL query string.
            params (Optional[Iterable[Any]]): Parameters to substitute into the query.

        Returns:
            int: The number of affected rows.
        """
        params = self._serialize_params(params)
        async with await self._connect() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
                return cursor.rowcount

    async def health_check(self) -> ComponentHealth:
        """
        Check the health of the MySQL database.
        """
        try:
            await self.get_row("SELECT 1")
            return ComponentHealth(status="online")
        except Exception as e:
            return ComponentHealth(status="unhealthy", detail=str(e))
