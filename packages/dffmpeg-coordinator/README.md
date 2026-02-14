# DFFmpeg Coordinator

The central server component of DFFmpeg. It manages:
*   Job Queues
*   Worker Registration
*   Job Assignment (via Scheduler)
*   Stale Job/Worker Cleanup (via Janitor)
*   Client API

## Configuration

For detailed configuration options, see the [Configuration Reference](../../../docs/configuration.md).

Configuration is handled via a YAML file (default: `dffmpeg-coordinator.yaml`).

## Running

The coordinator runs via the `dffmpeg-coordinator` CLI.

```bash
dffmpeg-coordinator
```

For development (auto-reload):
```bash
dffmpeg-coordinator --dev
```

You can also override the host and port:
```bash
dffmpeg-coordinator --host 0.0.0.0 --port 9000
```

## API Documentation

FastAPI automatically generates interactive API documentation. Once the server is running, you can access:

*   **Swagger UI:** `http://localhost:8000/docs`
*   **ReDoc:** `http://localhost:8000/redoc`

## Web Dashboard

The coordinator provides a built-in, read-only status dashboard that displays worker health and recent job history.

*   **URL:** `http://localhost:8000/status` (The root URL `/` also redirects here by default).

The dashboard can be disabled by setting `web_dashboard_enabled: false` in the configuration.

## Administration

The coordinator includes an administrative CLI tool, `dffmpeg-admin`, for managing the database directly (e.g., adding users, rotating keys).

### Usage

```bash
dffmpeg-admin [global options] <command> [subcommand] [options]
```

**Global Options:**
*   `--config`, `-c`: Path to the coordinator configuration file.

**Commands:**

*   **`user list`**: List all registered users.
    *   `--show-key`: Display the decrypted HMAC key for each user.
*   **`user show <client_id>`**: Display details for a specific user.
    *   `--show-key`: Display the decrypted HMAC key.
*   **`user add <client_id> --role <role>`**: Register a new user and generate an HMAC key.
    *   `--role`: One of `client`, `worker`, `admin` (default: `client`).
*   **`user rotate-key <client_id>`**: Generate a new HMAC key for an existing user.
*   **`user delete <client_id>`**: Remove a user from the database.

> **Note:** The `add` and `rotate-key` commands will display the newly generated HMAC key. This key must be securely provided to the client or worker.
