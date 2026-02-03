# DFFmpeg Worker

The worker agent that runs on encoding nodes. It polls the Coordinator for assigned jobs and executes them using the local FFmpeg binary.

## Configuration

Configuration is handled via a YAML file (default: `config.yaml`).

### Key Settings

*   **`client_id`**: (Required) Unique identifier for this worker.
*   **`hmac_key`** / **`hmac_key_file`**: (Required) Shared secret for authenticating with the Coordinator.
*   **`coordinator`**: Connection details.
    *   `host`: Coordinator hostname/IP.
    *   `port`: Coordinator port (default: 8000).
    *   `scheme`: Protocol (`http` or `https`).
*   **`transports`**: Client transport configuration.
    *   `enabled_transports`: List of active transports (e.g., `["http_polling"]`).
    *   `transport_settings`: Specific settings (e.g., polling intervals).
*   **`binaries`**: Mapping of logical binary names to local file paths.
    *   Example: `ffmpeg: /usr/bin/ffmpeg`
*   **`paths`**: Path mappings to translate Coordinator (source) paths to Worker (local) paths.

## Running

```bash
dffmpeg-worker
```

Command line arguments can override configuration:
```bash
dffmpeg-worker --config /path/to/custom_config.yaml
```

## Known Issues / TODOs

*   **Capabilities Detection:** The worker currently has a hardcoded/empty list of capabilities. Dynamic detection of supported codecs/formats is not yet implemented.
