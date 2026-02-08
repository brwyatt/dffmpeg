# dffmpeg

`dffmpeg` is a centrally-coordinated FFmpeg worker job manager.

This project is heavily inspired by [joshuaboniface/rffmpeg](https://github.com/joshuaboniface/rffmpeg), but re-imagined for distributed environments where clients and workers are decoupled.

## Design Philosophy

### Why dffmpeg?
While `rffmpeg` excels at simple, direct remote execution (SSH into a worker and run a command), it can struggle in complex or high-load environments. `dffmpeg` introduces a **central coordinator** to address these challenges:

*   **Centralized State:** Instead of clients "blindly" picking a worker, they submit jobs to a Coordinator. The Coordinator manages the state of the cluster, handling queueing, assignment, and retries.
*   **Path Independence ("Path-Blind"):** In `dffmpeg`, the Coordinator never deals with absolute paths. It uses variables (e.g., `$Source/video.mkv`). Path translation happens only at the edges (Client and Worker), allowing each node to have different mount points or storage configurations.
*   **Resilience:** Jobs are durable. If a worker crashes or a node goes offline, the Coordinator detects the failure (via heartbeat monitoring). Assignments that haven't started are re-queued to active workers, while interrupted running jobs are marked as failed to notify the client, preventing "zombie" jobs from hanging indefinitely. When a job fails, the worker reports the process exit code back to the coordinator and client for easier troubleshooting.

## Core Architecture

`dffmpeg` is built on a few key technical principles:

*   **ULIDs:** We use [ULIDs](https://github.com/ulid/spec) (Universally Unique Lexicographically Sortable Identifiers) for all IDs. They provide collision-free generation without central coordination and are sortable by time.
*   **HMAC Security:** All internal communication (Client <-> Coordinator <-> Worker) is signed using HMAC-SHA256. This ensures message integrity and authenticity without the overhead of full mTLS for every connection.
*   **Pluggability:**
    *   **Transports:** The system supports multiple communication backends. While HTTP Polling is the default, it is designed to support RabbitMQ, MQTT, or other message queues.
    *   **Databases:** The storage layer is modular. Currently using SQLite, but adaptable to PostgreSQL or other engines via the DAO pattern.

## Project Structure

The project is a monorepo containing:

*   **`dffmpeg-coordinator`**: The central server (FastAPI).
*   **`dffmpeg-worker`**: The worker agent that runs on encoding nodes.
*   **`dffmpeg-client`**: The CLI/Library for submitting jobs.
*   **`dffmpeg-common`**: Shared libraries and models.

## Current Status

*   **Coordinator**: Functional (API, DB, Scheduling, Janitor, Dashboard, Admin CLI).
*   **Worker**: Functional (Polling, Execution). Note: Capabilities detection is currently a stub.
*   **Client**: Functional (CLI, Library, Proxy).

## Getting Started (Development)

This project is currently in active development.

### Configuration
Configuration is handled via YAML files within each package. Please refer to the specific package documentation for detailed configuration options.

### Running Tests
Tests are run using `pytest`. Ensure your python environment is set up.

```bash
python3 -m pytest
```

You can target specific tests:
```bash
python3 -m pytest packages/dffmpeg-coordinator/tests/unit/test_janitor.py
```
