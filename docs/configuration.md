# Configuration Reference

This document provides a comprehensive reference for configuring the DFFmpeg components.

Each component (Coordinator, Worker, Client) uses a YAML configuration file. The file location is searched in the following order:

1.  CLI Argument (`--config`)
2.  Environment Variable (`DFFMPEG_<COMPONENT>_CONFIG`)
3.  Current Working Directory (`./dffmpeg-<component>.yaml`)
4.  User Config (`~/.config/dffmpeg/<component>.yaml`)
5.  System Config (`/etc/dffmpeg/<component>.yaml`)
6.  Venv Root (`sys.prefix/dffmpeg-<component>.yaml`)

## Coordinator Configuration

The Coordinator manages the cluster state, job queue, and worker registry.

**Default File:** `dffmpeg-coordinator.yaml`

### General Settings

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `host` | string | `127.0.0.1` | The interface to bind the API server to. |
| `port` | integer | `8000` | The port to listen on. |
| `web_dashboard_enabled` | boolean | `true` | Enable the built-in web dashboard at `/status`. |
| `default_job_heartbeat_interval` | integer | `5` | Default interval (seconds) for job heartbeats if not specified by the client. |
| `dev_mode` | boolean | `false` | Enable development mode (auto-reload, verbose logging). Can be set via `DFFMPEG_COORDINATOR_DEV=1`. |
| `allowed_binaries` | list[string] | `["ffmpeg", "ffprobe"]` | List of supported binary names that workers can register and clients can request. |
| `trusted_proxies` | list[string] | `["127.0.0.1"]` | List of trusted proxy IPs/CIDRs. If set, the Coordinator will respect `X-Forwarded-For` headers from these addresses. |

### Database Configuration (`database`)

Configure the database backend. Supports SQLite and MySQL/MariaDB.

#### Engine Defaults (`engine_defaults`)

Define default settings for database engines.

```yaml
database:
  engine_defaults:
    sqlite:
      path: "./dffmpeg.db"
    mysql:
      host: "localhost"
      port: 3306
      user: "dffmpeg"
      password: "password"
      database: "dffmpeg"
      use_ssl: false
      ssl_verify: true
      # Optional mTLS:
      # ssl_ca: /path/to/ca.pem
      # ssl_cert: /path/to/client-cert.pem
      # ssl_key: /path/to/client-key.pem
```

#### Repositories (`repositories`)

Override settings for specific repositories (`auth`, `jobs`, `messages`, `workers`).

```yaml
database:
  repositories:
    auth:
      engine: sqlite # Use SQLite for auth
      encryption_keys_file: "/path/to/keys.yaml" # Load encryption keys from external file
    jobs:
      engine: mysql # Use MySQL for jobs
      tablename: "jobs_v2" # Override table name
```

### Janitor Configuration (`janitor`)

Configure background cleanup tasks.

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `interval` | integer | `10` | How often the janitor runs (seconds). |
| `worker_threshold_factor` | float | `1.5` | Multiplier for registration interval to determine worker staleness. |
| `job_heartbeat_threshold_factor` | float | `1.5` | Multiplier for heartbeat interval to determine job staleness. |
| `job_assignment_timeout` | integer | `30` | Max time (seconds) a job stays in `assigned` state before retry. |
| `job_pending_retry_delay` | integer | `5` | Delay before retrying a pending job. |
| `job_pending_timeout` | integer | `30` | Max time a job stays in `pending` state. |

### Transports Configuration (`transports`)

See [Transports Documentation](transports.md) for detailed configuration.

---

## Worker Configuration

The Worker executes FFmpeg jobs.

**Default File:** `dffmpeg-worker.yaml`

### General Settings

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `client_id` | string | **Required** | Unique identifier for this worker. |
| `hmac_key` | string | **Required** | Shared secret key for authentication. |
| `hmac_key_file` | string | `null` | Path to file containing the HMAC key (alternative to `hmac_key`). |
| `registration_interval` | integer | `15` | How often (seconds) to send heartbeat/registration to Coordinator. |
| `log_batch_size` | integer | `100` | Max number of log lines to batch before sending. |
| `log_batch_delay` | float | `0.25` | Max delay (seconds) before sending a partial log batch. |

### Coordinator Connection (`coordinator`)

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `host` | string | `127.0.0.1` | Coordinator hostname/IP. |
| `port` | integer | `8000` | Coordinator port. |
| `scheme` | string | `http` | Protocol (`http` or `https`). |
| `path_base` | string | `/` | Base path for the API (e.g., `/api/v1`). Useful if behind a reverse proxy using a path. |

### Mount Management (`mount_management`)

Monitor filesystem mounts to ensure data availability.

```yaml
mount_management:
  recovery: true      # Attempt to remount if missing (systemctl start)
  sudo: false         # Use sudo for recovery commands
  mounts:
    - /mnt/media      # Simple path check
    - path: /mnt/tv   # Complex check with dependencies
      dependencies:
        - /mnt/media
```

### Binaries (`binaries`)

Map logical binary names to local executables.

```yaml
binaries:
  ffmpeg: /usr/bin/ffmpeg
  ffprobe: /usr/bin/ffprobe
```

These names must match what is defined in the Coordinator's `allowed_binaries` configuration. Only binaries that appear in both the worker's configuration and the coordinator's allowed list will be available for jobs.

### Path Mappings (`paths`)

Translate Coordinator path variables to local paths.

```yaml
paths:
  Movies: /mnt/media/movies
  TV: /mnt/media/tv
```

---

## Client Configuration

The Client submits jobs to the Coordinator.

**Default File:** `dffmpeg-client.yaml`

### General Settings

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `client_id` | string | **Required** | Unique identifier for this client. |
| `hmac_key` | string | **Required** | Shared secret key for authentication. |
| `hmac_key_file` | string | `null` | Path to file containing the HMAC key. |
| `job_heartbeat_interval` | integer | `5` | Interval (seconds) for job heartbeats. |

### Coordinator Connection (`coordinator`)

Same as Worker configuration.

### Path Mappings (`paths`)

Same as Worker configuration. Used to translate local paths to Coordinator variables during submission.

### Transports (`transports`)

See [Transports Documentation](transports.md).
