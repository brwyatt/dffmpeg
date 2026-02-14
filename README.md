# dffmpeg

`dffmpeg` is a centrally-coordinated FFmpeg worker job manager.

This project is heavily inspired by [joshuaboniface/rffmpeg](https://github.com/joshuaboniface/rffmpeg), but re-imagined for distributed environments where clients and workers are decoupled.

## Overview

`dffmpeg` introduces a **central coordinator** to manage job distribution, state tracking, and failure recovery. It features:

*   **Centralized State & Resilience**: Durable job queues and heartbeat monitoring.
*   **Path Independence**: Uses logical path variables instead of absolute paths, allowing flexible mount points.
*   **Secure & Pluggable**: HMAC-signed communication and support for multiple backends (RabbitMQ, MQTT, SQLite, etc.).

For a deep dive into the system design, see [Architecture](docs/architecture.md).

## Project Structure

The project is a monorepo containing:

*   **`dffmpeg-coordinator`**: The central server (FastAPI).
*   **`dffmpeg-worker`**: The worker agent that runs on encoding nodes.
*   **`dffmpeg-client`**: The CLI/Library for submitting jobs.
*   **`dffmpeg-common`**: Shared libraries and models.

## Current Status

*   **Coordinator**: Functional (API, DB, Scheduling, Janitor, Dashboard, Admin CLI).
*   **Worker**: Functional (Polling, Execution, Mount Monitoring). Note: Capabilities detection is currently a stub.
*   **Client**: Functional (CLI, Library, Proxy). Supports active monitoring and background/detached job submission.

## Development Setup

This project is currently in active development.

### Configuration
Configuration is handled via YAML files. For a complete reference, see [Configuration Reference](docs/configuration.md).

For details on configuring Transports (RabbitMQ, MQTT) and required permissions, see [Transport Configuration](docs/transports.md).

## Documentation

*   [Getting Started](docs/getting-started.md): Step-by-step guide to setting up a cluster.
*   [Configuration Reference](docs/configuration.md): Detailed configuration options for all components.
*   [Architecture](docs/architecture.md): System design, components, and scenarios (Development vs. Production).
*   [Security Model](docs/security-model.md): Authentication and key management.
*   [Transport Configuration](docs/transports.md): RabbitMQ and MQTT setup details.

### Running Tests
Tests are run using `pytest`. Ensure your python environment is set up.

```bash
python3 -m pytest
```

You can target specific tests:
```bash
python3 -m pytest packages/dffmpeg-coordinator/tests/unit/test_janitor.py
```
