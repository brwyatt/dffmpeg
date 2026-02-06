import pytest
from pathlib import Path
from dffmpeg.common.config_utils import load_hmac_key, inject_transport_defaults
from dffmpeg.common.transports import ClientTransportConfig
from dffmpeg.common.models.config import CoordinatorConnectionConfig

def test_load_hmac_key_direct():
    data = {"hmac_key": "secret"}
    key = load_hmac_key(data, Path("/tmp/config.yaml"))
    assert key == "secret"

def test_load_hmac_key_file_absolute(tmp_path):
    key_file = tmp_path / "secret.key"
    key_file.write_text("file_secret")
    
    data = {"hmac_key_file": str(key_file)}
    key = load_hmac_key(data, Path("/tmp/config.yaml"))
    assert key == "file_secret"
    assert data["hmac_key"] == "file_secret"

def test_load_hmac_key_file_relative(tmp_path):
    config_dir = tmp_path / "conf"
    config_dir.mkdir()
    config_file = config_dir / "config.yaml"
    
    key_file = config_dir / "secret.key"
    key_file.write_text("rel_secret")
    
    data = {"hmac_key_file": "secret.key"}
    key = load_hmac_key(data, config_file)
    assert key == "rel_secret"

def test_load_hmac_key_precedence(tmp_path):
    key_file = tmp_path / "secret.key"
    key_file.write_text("file_secret")
    
    data = {
        "hmac_key": "direct_secret",
        "hmac_key_file": str(key_file)
    }
    key = load_hmac_key(data, Path("/tmp/config.yaml"))
    assert key == "file_secret"

def test_load_hmac_key_missing():
    with pytest.raises(ValueError):
        load_hmac_key({}, Path("/tmp/config.yaml"))

def test_inject_transport_defaults():
    transports = ClientTransportConfig()
    coord = CoordinatorConnectionConfig(host="test", port=1234)
    
    inject_transport_defaults(
        transports,
        coord,
        client_id="cid",
        hmac_key="key",
        default_poll_wait=10
    )
    
    settings = transports.transport_settings["http_polling"]
    assert settings["client_id"] == "cid"
    assert settings["hmac_key"] == "key"
    assert settings["coordinator_url"] == "http://test:1234/"
    assert settings["poll_wait"] == 10

def test_inject_transport_defaults_preserve():
    transports = ClientTransportConfig(
        transport_settings={
            "http_polling": {"poll_wait": 20}
        }
    )
    coord = CoordinatorConnectionConfig()
    
    inject_transport_defaults(
        transports,
        coord,
        client_id="cid",
        hmac_key="key"
    )
    
    settings = transports.transport_settings["http_polling"]
    assert settings["poll_wait"] == 20
    assert settings["client_id"] == "cid"
