import os
from base64 import b64encode

import pytest

from dffmpeg.common.crypto import CryptoManager
from dffmpeg.common.crypto.fernet import FernetEncryption


def test_fernet_encryption():
    key = b64encode(os.urandom(32)).decode("ascii")
    fernet = FernetEncryption(key)

    original = "secret_hmac_key"
    encrypted = fernet.encrypt(original)
    assert encrypted != original

    decrypted = fernet.decrypt(encrypted)
    assert decrypted == original


def test_crypto_manager_pluggable(monkeypatch):
    # Mock entrypoint loading if necessary, but we can use real Fernet if registered
    # For a unit test, we'll just test the logic with real Fernet

    key_id = "1"
    raw_key = b64encode(os.urandom(32)).decode("ascii")
    keys = {key_id: f"fernet:{raw_key}"}

    manager = CryptoManager(keys)

    original = "my_hmac_key"
    encrypted = manager.encrypt(original, key_id)
    assert encrypted != original

    decrypted = manager.decrypt(encrypted, key_id)
    assert decrypted == original


def test_crypto_manager_missing_key():
    manager = CryptoManager({})
    with pytest.raises(ValueError, match="Unknown key ID: missing"):
        manager.encrypt("data", "missing")


def test_crypto_manager_invalid_format():
    manager = CryptoManager({"k1": "invalid_format"})
    with pytest.raises(ValueError, match="Invalid key format"):
        manager.encrypt("data", "k1")
