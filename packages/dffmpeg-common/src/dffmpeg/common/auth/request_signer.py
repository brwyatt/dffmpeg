import hashlib
import hmac
import time
from base64 import b64decode, b64encode
from logging import getLogger
from os import urandom
from typing import Tuple, Union

logger = getLogger(__name__)


class RequestSigner:
    drift = 300

    def __init__(self, secret_key: str):
        self.secret = b64decode(secret_key.encode("ascii"))

    def generate_signature(self, method: str, path: str, timestamp: str, payload: Union[bytes, str]) -> str:
        """Generate HMAC signature from specific request attributes"""
        if type(payload) is str:
            payload = payload.encode()
        payload_hash = hashlib.sha256(payload).hexdigest()  # type: ignore
        canonical = f"{method.upper()}|{path}|{timestamp}|{payload_hash}"
        logger.info(f"Signing canonical string: {canonical}")

        hash = b64encode(hmac.new(self.secret, canonical.encode(), hashlib.sha256).digest()).decode("ascii")

        logger.info(f"HMAC Signature: {hash}")

        return hash

    def sign(self, method: str, path: str, payload: Union[bytes, str] = b"") -> Tuple[str, str]:
        """Returns (timestamp, signature) for use in headers."""
        logger.debug(f"Generating signature for: {method}|{path}")
        timestamp = str(int(time.time()))
        signature = self.generate_signature(method, path, timestamp, payload)
        return timestamp, signature

    def verify(self, method: str, path: str, timestamp: str, signature: str, payload: Union[bytes, str] = b"") -> bool:
        """Verify signature and check timestamp for drift to prevent replay."""
        logger.debug(f"Verifying signature for: {method}|{path}|{timestamp} -- {signature}")

        # Check Clock Drift to prevent replay
        if abs(int(time.time()) - int(timestamp)) > self.drift:
            logger.warning(f"Request timestamp has drift > {self.drift}!")
            return False

        # Re-calculate expected signature
        expected = self.generate_signature(method, path, timestamp, payload)
        logger.debug(f"Calculated signature for request: {expected}")

        # Compare
        result = hmac.compare_digest(expected, signature)
        if result:
            logger.info("HMACs matched")
        else:
            logger.warning("HMACs do not match!")

        return result

    @classmethod
    def generate_key(cls) -> str:
        return b64encode(urandom(32)).decode("ascii")
