# DFFmpeg Worker

The worker agent that runs on encoding nodes. It polls the Coordinator for assigned jobs and executes them using the local FFmpeg binary.

## Configuration

Configuration is handled via a YAML file (default: `dffmpeg-worker.yaml`).

The worker searches for configuration in the following order:
1.  CLI Argument (`--config`)
2.  Environment Variable (`DFFMPEG_WORKER_CONFIG`)
3.  Current Working Directory (`./dffmpeg-worker.yaml`)
4.  User Config (`~/.config/dffmpeg/worker.yaml`)
5.  System Config (`/etc/dffmpeg/worker.yaml`)
6.  Venv Root (`sys.prefix/dffmpeg-worker.yaml`)

### Key Settings

*   **`client_id`**: (Required) Unique identifier for this worker.
*   **`hmac_key`** / **`hmac_key_file`**: (Required) Shared secret for authenticating with the Coordinator.
*   **`coordinator`**: Connection details.
    *   `host`: Coordinator hostname/IP.
    *   `port`: Coordinator port (default: 8000).
    *   `scheme`: Protocol (`http` or `https`).
*   **`transports`**: Client transport configuration.
    *   `enabled_transports`: List of active transports (e.g., `["mqtt", "http_polling"]`). If empty, defaults to `["http_polling"]`. The order of this list defines the worker's preference during negotiation with the Coordinator.
    *   `transport_settings`: Specific settings for each transport type.
        *   **`mqtt`**:
            *   `host`: MQTT broker hostname.
            *   `port`: MQTT broker port (default: 1883).
            *   `username`: (Optional) Auth username.
            *   `password`: (Optional) Auth password.
            *   `use_tls`: Use TLS for the connection (default: false).
*   **`binaries`**: Mapping of logical binary names to local file paths.
    *   Example: `ffmpeg: /usr/bin/ffmpeg`
*   **`paths`**: Path mappings to translate Coordinator (source) paths to Worker (local) paths.
*   **`mount_management`**: Environment health monitoring (see below).

### Mount Point Monitoring

The worker can monitor and manage filesystem mount points to ensure network shares or encrypted volumes are available before it advertises its path mappings to the Coordinator. This prevents "black hole" workers from accepting jobs when their source or destination files are inaccessible.

Settings under `mount_management`:

*   **`recovery`**: (Default: `true`) If `true`, the worker will attempt to restore missing mounts using `systemctl start`.
*   **`sudo`**: (Default: `false`) If `true`, recovery commands are prepended with `sudo`. Requires passwordless sudo configuration for the worker user.
*   **`mounts`**: A list of paths or objects representing required mounts.
    *   Simple path: `- /mnt/nas`
    *   Dependency object:
        ```yaml
        - path: /mnt/media
          dependencies: [/mnt/nas]
        ```

**Dependency Tree Logic:**
The worker builds a dependency tree of all managed mounts. A mount is only considered "Healthy" if it is locally mounted **and** all its dependencies (ancestors or explicit links) are healthy.

**Path Pruning:**
Path variable mappings (defined in `paths`) are validated against this tree. A path is only advertised to the Coordinator if:
1.  All managed mounts that are **parents** of the path are healthy.
2.  All managed mounts that are **children** of the path are healthy.
3.  Any explicit **dependencies** of those mounts are healthy.

This ensures that `/mnt/nas/media/movies` remains active even if `/mnt/nas/media/tv` is unmounted, as long as the common parent `/mnt/nas` is healthy. However, if `/mnt/nas` itself is unmounted, both will be pruned.

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
