from typing import Any, Dict, Iterable, Optional, Tuple, Union

from sqlalchemy import Table
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import ClauseElement

from dffmpeg.coordinator.db.engines import BaseDB


class SQLAlchemyDB(BaseDB):
    """
    Base Class for Database Engines that use SQLAlchemy for query generation.
    """

    @property
    def dialect(self) -> Dialect:
        """
        The SQLAlchemy dialect to use for compilation.
        """
        raise NotImplementedError()

    @property
    def table(self) -> Table:
        """
        The SQLAlchemy Table object defined in the Repository.
        """
        raise NotImplementedError()

    def _connect(self) -> Any:
        """
        Returns a context manager for the database connection.
        """
        raise NotImplementedError()

    def compile_query(self, query: ClauseElement) -> Tuple[str, Union[Dict[str, Any], Tuple[Any, ...]]]:
        """
        Compiles a SQLAlchemy query into a SQL string and parameters.

        Args:
            query (ClauseElement): The SQLAlchemy expression to compile.

        Returns:
            Tuple[str, Any]: A tuple containing the SQL string and the parameters.
                             Params will be a dict if paramstyle is 'named'/'pyformat',
                             or a tuple if 'qmark'/'format'.
        """
        compiled = query.compile(dialect=self.dialect, compile_kwargs={"render_postcompile": True})
        params = compiled.params or {}

        if self.dialect.paramstyle in ("named", "pyformat"):
            return str(compiled), dict(params)

        # For positional params (qmark, format, numeric), we MUST order them
        # compiled.positiontup contains the keys in order
        positiontup = getattr(compiled, "positiontup", None)
        if positiontup:
            return str(compiled), tuple(params[k] for k in positiontup)

        # No parameters or fallback
        return str(compiled), tuple()

    @property
    def table_create(self) -> str:
        """
        Generates the CREATE TABLE statement using self.table (expected from Repo).
        """
        return str(CreateTable(self.table, if_not_exists=True).compile(dialect=self.dialect))

    async def execute(self, query: str, params: Optional[Iterable[Any]] = None) -> None:
        raise NotImplementedError()

    async def execute_and_return_rowcount(self, query: str, params: Optional[Iterable[Any]] = None) -> int:
        raise NotImplementedError()

    async def get_rows(self, query: str, params: Optional[Iterable[Any]] = None) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError()

    async def get_row(self, query: str, params: Optional[Iterable[Any]] = None) -> Optional[Dict[str, Any]]:
        raise NotImplementedError()
