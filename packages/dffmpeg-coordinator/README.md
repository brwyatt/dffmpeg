# DFFmpeg Coordinator

The central server component of DFFmpeg. It manages:
*   Job Queues
*   Worker Registration
*   Job Assignment (via Scheduler)
*   Stale Job/Worker Cleanup (via Janitor)
*   Client API

## Configuration

Configuration is handled via a YAML file (default: `config.yaml`).

### Key Sections

*   **`database`**: Database connection settings.
    *   `repositories`: Configuration for individual repositories (auth, jobs, messages, workers).
        *   `auth`: Can include `encryption_keys_file` or `encryption_keys` for credential storage.
    *   `engine_defaults`: Default settings for database engines (e.g., SQLite path).
*   **`transports`**: Transport settings.
    *   `enabled_transports`: List of active transport mechanisms (e.g., `["http_polling"]`).
    *   `transport_settings`: Specific settings for each transport type.
*   **`janitor`**: Background task settings.
    *   `interval`: How often the janitor runs (in seconds).
    *   `worker_threshold_factor`: Multiplier for determining when a worker is considered stale.
    *   `job_assignment_timeout`: Max time (seconds) a job can stay in "assigned" state before retry.

## Running (Development)

The coordinator is a FastAPI application.

```bash
uvicorn dffmpeg.coordinator.api.main:app --reload
```
