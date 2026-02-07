import pytest
import yaml

from dffmpeg.coordinator.config import CoordinatorConfig, load_config


def test_load_config_default_explicit_missing(tmp_path):
    # If explicit file path is missing, should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yml")


def test_load_config_none(tmp_path, monkeypatch):
    # Ensure CWD is clean
    monkeypatch.chdir(tmp_path)
    config = load_config(None)
    assert isinstance(config, CoordinatorConfig)


def test_load_config_env_var(tmp_path, monkeypatch):
    config_data = {"database": {"defaults": {"engine": "sqlite", "path": "env.db"}}}
    config_file = tmp_path / "env_config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    monkeypatch.setenv("DFFMPEG_COORDINATOR_CONFIG", str(config_file))
    config = load_config()
    assert config.database.defaults["path"] == "env.db"


def test_load_config_basic(tmp_path):
    config_data = {"database": {"defaults": {"engine": "sqlite", "path": "test.db"}}}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    assert config.database.defaults["engine"] == "sqlite"
    assert config.database.defaults["path"] == "test.db"


def test_load_config_with_external_keys(tmp_path):
    keys_data = {"1": "fernet:key1", "2": "fernet:key2"}
    keys_file = tmp_path / "keys.yml"
    with open(keys_file, "w") as f:
        yaml.dump(keys_data, f)

    config_data = {
        "database": {
            "repositories": {"auth": {"encryption_keys_file": str(keys_file), "default_encryption_key_id": "1"}}
        }
    }
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    auth_repo_config = config.database.repositories.get("auth")
    assert auth_repo_config is not None
    assert auth_repo_config.get("encryption_keys") == keys_data
    assert auth_repo_config.get("default_encryption_key_id") == "1"


def test_load_config_with_relative_keys_file(tmp_path):
    # Keys file relative to config file
    keys_data = {"1": "fernet:key1"}
    keys_file = tmp_path / "keys.yml"
    with open(keys_file, "w") as f:
        yaml.dump(keys_data, f)

    config_data = {"database": {"repositories": {"auth": {"encryption_keys_file": "keys.yml"}}}}
    config_file = tmp_path / "config.yml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = load_config(config_file)
    auth_repo_config = config.database.repositories.get("auth", {})
    assert auth_repo_config.get("encryption_keys") == keys_data
