import logging
from pathlib import Path
from typing import Any, Dict

from dffmpeg.common.models.config import CoordinatorConnectionConfig
from dffmpeg.common.transports import ClientTransportConfig

logger = logging.getLogger(__name__)


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
