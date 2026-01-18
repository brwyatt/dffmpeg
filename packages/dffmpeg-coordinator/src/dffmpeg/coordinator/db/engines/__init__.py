class BaseDB():
    async def setup(self) -> None:
        pass

    @property
    async def table_create(self) -> str:
        pass
