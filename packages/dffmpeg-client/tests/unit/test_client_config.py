import pytest
import yaml
from pydantic import ValidationError

from dffmpeg.client.config import load_config


def test_load_config_basic(tmp_path):
    config_data = {"client_id": "client-1", "hmac_key": "secret"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(str(config_file))
    assert config.client_id == "client-1"
    assert config.hmac_key == "secret"


def test_load_config_missing_hmac(tmp_path):
    config_data = {"client_id": "client-1"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValueError, match="hmac_key must be provided"):
        load_config(str(config_file))


def test_load_config_missing_client_id(tmp_path):
    config_data = {"hmac_key": "secret"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValidationError) as excinfo:
        load_config(str(config_file))
    assert "client_id" in str(excinfo.value)


def test_load_config_env_vars(monkeypatch, tmp_path):
    # Ensure CWD is empty so no default config is loaded
    monkeypatch.chdir(tmp_path)

    monkeypatch.setenv("DFFMPEG_CLIENT_ID", "env-client")
    monkeypatch.setenv("DFFMPEG_HMAC_KEY", "env-secret")

    # We don't provide a config file, so it should rely on env vars
    config = load_config()
    assert config.client_id == "env-client"
    assert config.hmac_key == "env-secret"


def test_load_config_env_var_coordinator(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    monkeypatch.setenv("DFFMPEG_CLIENT_ID", "env-client")
    monkeypatch.setenv("DFFMPEG_HMAC_KEY", "env-secret")
    monkeypatch.setenv("DFFMPEG_COORDINATOR_URL", "https://api.example.com:8443/api/v1")

    config = load_config()
    assert config.coordinator.scheme == "https"
    assert config.coordinator.host == "api.example.com"
    assert config.coordinator.port == 8443
    assert config.coordinator.path_base == "/api/v1"


def test_load_config_env_var_file_path(tmp_path, monkeypatch):
    config_data = {"client_id": "file-client", "hmac_key": "file-secret"}
    config_file = tmp_path / "custom_config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    monkeypatch.setenv("DFFMPEG_CLIENT_CONFIG", str(config_file))

    config = load_config()
    assert config.client_id == "file-client"
    assert config.hmac_key == "file-secret"


def test_load_config_precedence_env_over_file(tmp_path, monkeypatch):
    config_data = {"client_id": "file-client", "hmac_key": "file-secret"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    monkeypatch.setenv("DFFMPEG_CLIENT_ID", "env-client")

    config = load_config(str(config_file))
    assert config.client_id == "env-client"  # Env var wins
    assert config.hmac_key == "file-secret"  # From file


def test_load_config_hmac_from_file(tmp_path):
    key_file = tmp_path / "key.txt"
    key_file.write_text("file-secret-key")

    config_data = {"client_id": "client-1", "hmac_key_file": "key.txt"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(str(config_file))
    assert config.hmac_key == "file-secret-key"
