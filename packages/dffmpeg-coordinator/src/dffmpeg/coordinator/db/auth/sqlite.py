from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth import AuthRepository


class SQLiteAuthRepository(AuthRepository):
    def __init__(self, *args, path: str, tablename: str = "auth", **kwargs):
        print("HELLO WORLD")
