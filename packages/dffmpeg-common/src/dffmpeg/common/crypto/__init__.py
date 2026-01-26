from base64 import b64decode
from importlib.metadata import entry_points
from typing import Dict, Type, TypeVar, cast

T = TypeVar("T", bound="BaseEncryption")


class BaseEncryption:
    def __init__(self, key: str):
        self.key = b64decode(key.encode("ascii"))

    def encrypt(self, data: str) -> str:
        """Encrypt a string and return a B64 encoded string"""
        raise NotImplementedError()

    def decrypt(self, data: str) -> str:
        """Decrypt a B64 encoded string and return the original string"""
        raise NotImplementedError()


def load_encryption_provider(algo: str) -> Type[BaseEncryption]:
    available_entrypoints = entry_points(group="dffmpeg.common.crypto")
    matching = [x for x in available_entrypoints if x.name == algo]

    if len(matching) != 1:
        available_names = ", ".join([x.name for x in available_entrypoints])
        raise ValueError(f'Invalid encryption algorithm "{algo}"! ' f"Expected one of: {available_names}")

    loaded = matching[0].load()

    if not isinstance(loaded, type) or not issubclass(loaded, BaseEncryption):
        raise TypeError(f"Entrypoint {algo} loaded {matching[0].name}, " f"which is not a subclass of BaseEncryption")

    return cast(Type[BaseEncryption], loaded)


class CryptoManager:
    def __init__(self, keys: Dict[str, str]):
        """
        keys: A dictionary of key_id to key_string.
              key_string should be in the format "algo:key_b64"
        """
        self._keys = keys
        self.loaded_providers = self.load_crypto_providers()
        self._providers: Dict[str, BaseEncryption] = {}

    def _get_provider(self, key_id: str) -> BaseEncryption:
        if key_id not in self._providers:
            if key_id not in self._keys:
                raise ValueError(f"Unknown key ID: {key_id}")

            key_str = self._keys[key_id]
            if ":" not in key_str:
                raise ValueError(f"Invalid key format for {key_id}. Expected 'algo:key_b64'")

            algo, key_b64 = key_str.split(":", 1)
            if algo not in self.loaded_providers:
                raise ValueError(f"Invalid algorithm: {algo}")

            self._providers[key_id] = self.loaded_providers[algo](key_b64)

        return self._providers[key_id]

    def load_crypto_providers(self) -> Dict[str, Type[BaseEncryption]]:
        available_entrypoints = entry_points(group="dffmpeg.common.crypto")

        if len(available_entrypoints) < 1:
            ValueError("No encryption methods found")

        loaded = {}

        for x in available_entrypoints:
            cls = x.load()
            if not isinstance(cls, type) or not issubclass(cls, BaseEncryption):
                raise TypeError(f"Loaded entrypoint {x.name} for dffmpeg.common.crypto is not a valid BaseEncryption!")
            loaded[x.name] = cls

        return loaded

    def encrypt(self, data: str, key_id: str) -> str:
        provider = self._get_provider(key_id)
        return provider.encrypt(data)

    def decrypt(self, data: str, key_id: str) -> str:
        provider = self._get_provider(key_id)
        return provider.decrypt(data)
