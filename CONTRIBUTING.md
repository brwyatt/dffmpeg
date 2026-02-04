# Contributing to DFFmpeg

Thank you for your interest in contributing to `dffmpeg`! This guide will help you get started with the development environment and understand our coding standards.

## Getting Started

### Development Environment

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/brwyatt/dffmpeg.git
    cd dffmpeg
    ```

2.  **Install Dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements-dev.txt
    ```
    This will install all packages with dev dependencies in editable mode.

3.  **Run Tests:**
    ```bash
    python3 -m pytest
    ```

## Coding Standards

*   **Python Version:** Currently targeting Python 3.12+.
*   **Async First:** The project uses `asyncio` extensively. All I/O operations (database, network) must be non-blocking.
*   **Pydantic V2:** Use Pydantic V2 for all data models and configuration parsing.
*   **Type Hinting:** Comprehensive type hinting is required.

## Plugin System (Entrypoints)

`dffmpeg` uses Python's `entry_points` system for modularity. If you are adding a new Transport or Database backend, you will need to register it in the package's `pyproject.toml`.

### Available Entrypoints

*   **`dffmpeg.db.auth`**: Auth repositories by engine (e.g., "sqlite").
*   **`dffmpeg.db.jobs`**: Job repositories by engine.
*   **`dffmpeg.db.messages`**: Message repositories by engine.
*   **`dffmpeg.db.workers`**: Worker repositories by engine.
*   **`dffmpeg.transports.server`**: Server-side transports (Coordinator) by transport name (e.g., "http_polling").
*   **`dffmpeg.transports.client`**: Client-side transports (Worker/Client) by transport name (e.g., "http_polling").
*   **`dffmpeg.common.crypto`**: Encryption providers (e.g., Fernet).

## Tips & Tricks

### Generating HMAC Keys

Since we use HMAC-SHA256 for authentication, you will need to generate shared secret keys for your workers and clients. You can use the built-in helper method:

```bash
python3 -c "from dffmpeg.common.auth.request_signer import RequestSigner; print(RequestSigner.generate_key())"
```

Currently, these will need to be added to the auth database table manually, along with the `client_id` (a string identifier) and a `role` ("admin", "client", "worker").
