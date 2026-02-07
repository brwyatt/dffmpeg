import pytest
import yaml
from pydantic import ValidationError

from dffmpeg.worker.config import load_config


def test_load_config_nonexistent(tmp_path):
    # If explicit file path is missing, should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yml")


def test_load_config_missing_client_id(tmp_path):
    # If file exists but missing client_id, should fail validation
    config_data = {"hmac_key": "secret-key"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValidationError) as excinfo:
        load_config(config_file)
    assert "client_id" in str(excinfo.value)


def test_load_config_basic_auth(tmp_path):
    config_data = {"client_id": "worker-1", "hmac_key": "secret-key"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    assert config.client_id == "worker-1"
    assert config.hmac_key == "secret-key"
    # defaults
    assert config.coordinator.scheme == "http"


def test_load_config_missing_hmac(tmp_path):
    config_data = {"client_id": "worker-1"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    # Validates ok via pydantic (optional field), but fails our post-validation check
    with pytest.raises(ValueError) as excinfo:
        load_config(config_file)
    assert "hmac_key must be provided" in str(excinfo.value)


def test_load_config_hmac_from_file(tmp_path):
    key_file = tmp_path / "key.txt"
    key_file.write_text("  file-secret-key  ")  # with whitespace

    config_data = {"client_id": "worker-1", "hmac_key_file": "key.txt"}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    assert config.hmac_key == "file-secret-key"


def test_load_config_hmac_precedence(tmp_path, caplog):
    key_file = tmp_path / "key.txt"
    key_file.write_text("file-secret-key")

    config_data = {"client_id": "worker-1", "hmac_key": "inline-key", "hmac_key_file": str(key_file)}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    assert config.hmac_key == "file-secret-key"
    assert "precedence" in caplog.text


def test_load_config_hmac_file_not_found(tmp_path, caplog):
    config_data = {
        "client_id": "worker-1",
        "hmac_key_file": "missing.txt",
        # no hmac_key provided, so this should eventually fail
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValueError):
        load_config(config_file)

    assert "HMAC key file not found" in caplog.text


def test_load_config_with_paths_and_binaries(tmp_path):
    config_data = {
        "client_id": "worker-1",
        "hmac_key": "secret",
        "binaries": {"ffmpeg": "/usr/bin/ffmpeg"},
        "paths": {"Movies": "/mnt/media/movies"},
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    assert config.binaries == {"ffmpeg": "/usr/bin/ffmpeg"}
    assert config.paths == {"Movies": "/mnt/media/movies"}
