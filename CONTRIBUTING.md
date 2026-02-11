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

### Database Architecture

The database layer follows a hierarchical DAO pattern to support multiple engines while maximizing code reuse:

1.  **Repository Interface (`__init__.py`)**: Defines the abstract base class and the SQLAlchemy `Table` schema.
2.  **Generic Implementation (`sqlalchemy.py`)**: Implements the repository methods using generic SQLAlchemy Core constructs (SELECT, INSERT, UPDATE). This layer contains the bulk of the logic and is dialect-agnostic.
3.  **Engine-Specific Implementation (`sqlite.py`, etc.)**: Inherits from the generic implementation and the specific engine class (`SQLiteDB`). It overrides methods only when necessary for dialect-specific optimizations (e.g., `INSERT OR REPLACE` vs generic check-and-set).

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

### Managing Users & Keys

Use the `dffmpeg-admin` tool provided by the coordinator package to manage users and generate HMAC keys:

```bash
# Add a new client
dffmpeg-admin user add my-client --role client

# Add a new worker
dffmpeg-admin user add worker01 --role worker
```

See [Security Model](docs/security-model.md) for more details on authentication.
