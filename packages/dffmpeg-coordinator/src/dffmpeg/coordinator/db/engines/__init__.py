class BaseDB():
    async def setup(self) -> None:
        pass

    @property
    def table_create(self) -> str | None:
        return
