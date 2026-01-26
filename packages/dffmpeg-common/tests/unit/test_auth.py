import time

from dffmpeg.common.auth.request_signer import RequestSigner


def test_request_signer_flow():
    secret = RequestSigner.generate_key()
    signer = RequestSigner(secret)

    method = "GET"
    path = "/api/v1/test"
    payload = "hello world"

    timestamp, signature = signer.sign(method, path, payload)

    assert signer.verify(method, path, timestamp, signature, payload) is True


def test_request_signer_invalid_signature():
    secret = RequestSigner.generate_key()
    signer = RequestSigner(secret)

    method = "GET"
    path = "/api/v1/test"
    payload = "hello world"

    timestamp, signature = signer.sign(method, path, payload)

    # Modify payload
    assert signer.verify(method, path, timestamp, signature, "wrong payload") is False
    # Modify path
    assert signer.verify(method, "/wrong/path", timestamp, signature, payload) is False
    # Modify method
    assert signer.verify("POST", path, timestamp, signature, payload) is False


def test_request_signer_drift():
    secret = RequestSigner.generate_key()
    signer = RequestSigner(secret)

    method = "GET"
    path = "/api/v1/test"
    payload = ""

    # Create an old timestamp
    old_timestamp = str(int(time.time()) - 400)  # Beyond 300s drift
    signature = signer.generate_signature(method, path, old_timestamp, payload)

    assert signer.verify(method, path, old_timestamp, signature, payload) is False


def test_request_signer_different_secrets():
    secret1 = RequestSigner.generate_key()
    secret2 = RequestSigner.generate_key()

    signer1 = RequestSigner(secret1)
    signer2 = RequestSigner(secret2)

    method = "GET"
    path = "/api/v1/test"

    timestamp, signature = signer1.sign(method, path)

    # Verifying with wrong secret should fail
    assert signer2.verify(method, path, timestamp, signature) is False
