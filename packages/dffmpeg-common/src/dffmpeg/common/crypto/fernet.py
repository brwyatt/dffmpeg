import os
from base64 import b64encode

from cryptography.fernet import Fernet

from dffmpeg.common.crypto import BaseEncryption


class FernetEncryption(BaseEncryption):
    def __init__(self, key: str):
        super().__init__(key)
        # Fernet key must be 32 url-safe base64-encoded bytes
        # If the input is already base64, we might need to ensure it's the right format
        # but BaseEncryption already decodes it. Fernet wants the b64 bytes.
        self._fernet = Fernet(b64encode(self.key))

    def encrypt(self, data: str) -> str:
        return self._fernet.encrypt(data.encode()).decode("ascii")

    def decrypt(self, data: str) -> str:
        return self._fernet.decrypt(data.encode()).decode()

    @classmethod
    def generate_key(cls) -> str:
        return b64encode(os.urandom(32)).decode("ascii")
