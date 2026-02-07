import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

from dffmpeg.common.models.config import CoordinatorConnectionConfig
from dffmpeg.common.transports import ClientTransportConfig

logger = logging.getLogger(__name__)


def find_config_file(app_name: str, env_var: str | None = None, explicit_path: Path | str | None = None) -> Path | None:
    """
    Locates the configuration file for the specified application.

    Search order:
    1. Explicit path (if provided) - Raises FileNotFoundError if missing
    2. Environment variable path (if provided) - Raises FileNotFoundError if missing
    3. ./dffmpeg-{app_name}.yaml
    4. ~/.config/dffmpeg/{app_name}.yaml
    5. /etc/dffmpeg/{app_name}.yaml
    6. {sys.prefix}/dffmpeg-{app_name}.yaml

    Args:
        app_name (str): The application name (e.g., 'coordinator', 'worker', 'client').
        env_var (str, optional): The name of the environment variable to check for a config path.
        explicit_path (Path | str, optional): A specific path to check first.

    Returns:
        Path | None: The path to the configuration file, or None if not found.

    Raises:
        FileNotFoundError: If an explicit path or environment variable path is provided but does not exist.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at explicit path: {path}")
        return path

    if env_var and (env_val := os.environ.get(env_var)):
        path = Path(env_val)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at path specified by {env_var}: {path}")
        return path

    filename = f"dffmpeg-{app_name}.yaml"

    candidates = [
        Path.cwd() / filename,
        Path.home() / ".config" / "dffmpeg" / f"{app_name}.yaml",
        Path("/etc/dffmpeg") / f"{app_name}.yaml",
        Path(sys.prefix) / filename,
    ]

    for path in candidates:
        if path.exists() and path.is_file():
            logger.debug(f"Found config at {path}")
            return path

    return None


def load_hmac_key(data: Dict[str, Any], config_path: Path) -> str:
    """
    Loads HMAC key from data or file referenced in data.
    Modifies data in-place if hmac_key_file is used.
    Returns the key.
    Raises ValueError if key cannot be loaded.
    """
    hmac_key_file = data.get("hmac_key_file")
    if hmac_key_file:
        if data.get("hmac_key"):
            logger.warning("Both 'hmac_key' and 'hmac_key_file' are defined. 'hmac_key_file' will take precedence.")

        key_path = Path(hmac_key_file)
        if not key_path.is_absolute():
            # If config_path is a directory (CWD check case), parent is parent.
            # If config_path is a file, parent is dir.
            # Expect config_path to be the file path.
            key_path = config_path.parent / key_path

        if key_path.exists():
            try:
                with open(key_path, "r") as f:
                    key = f.read().strip()
                    data["hmac_key"] = key
                    return key
            except Exception as e:
                logger.error(f"Failed to read HMAC key file: {e}")
                # Don't raise here, fall through to check data['hmac_key']
        else:
            logger.warning(f"HMAC key file not found at {key_path}")

    key = data.get("hmac_key")
    if not key:
        raise ValueError("hmac_key must be provided either directly or via hmac_key_file")
    return key


def inject_transport_defaults(
    transports_config: ClientTransportConfig,
    coordinator_config: CoordinatorConnectionConfig,
    client_id: str,
    hmac_key: str,
    default_poll_wait: int = 5,
):
    """
    Injects default connection settings into transport configurations (specifically http_polling).
    """
    # Construct coordinator URL
    coord = coordinator_config
    base_url = (
        f"{coord.scheme}://{coord.host}:{coord.port}{'' if coord.path_base.startswith('/') else '/'}{coord.path_base}"
    )

    defaults = {
        "client_id": client_id,
        "hmac_key": hmac_key,
        "coordinator_url": base_url,
        "poll_wait": default_poll_wait,
    }

    # Helper to inject into specific transport settings
    def inject(transport_name):
        settings = transports_config.transport_settings.get(transport_name, {})
        for k, v in defaults.items():
            if k not in settings:
                settings[k] = v
        transports_config.transport_settings[transport_name] = settings

    # Currently we only know about http_polling needing these specific defaults
    inject("http_polling")
