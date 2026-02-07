from dffmpeg.common.models import ComponentHealth


class BaseDB:
    async def setup(self) -> None:
        pass

    @property
    def table_create(self) -> str | None:
        return

    async def health_check(self) -> ComponentHealth:
        """
        Check the health of the database engine.

        Returns:
            ComponentHealth: The health status of the database.
        """
        raise NotImplementedError()
