# dffmpeg

`dffmpeg` is a centrally-coordinated FFmpeg worker job manager.

This project is heavily inspired by [joshuaboniface/rffmpeg](https://github.com/joshuaboniface/rffmpeg), but instead of clients directly pushing requests to workers over SSH, work requests are pushed to a central coordinator that assigns work to active workers using HTTP polling and/or message queues.

While this requires a greater infrastructure setup, it seeks to provide the following additional helpful properties as a result:

*   Multiple clients can effectively balance load on the workers
*   Coordinator can re-assign or retry jobs
*   Path mapping support, allowing for workers and clients to have different local mount locations
    *   Work assignments can take available mounts/maps, avoiding assigning work to workers that lack proper access
*   High availability support - host failure does not result in lost tracking of work

## Project Structure

The project is a monorepo containing:

*   **`dffmpeg-coordinator`**: The central server (FastAPI).
*   **`dffmpeg-worker`**: The worker agent that runs on encoding nodes.
*   **`dffmpeg-client`**: The CLI/Library for submitting jobs.
*   **`dffmpeg-common`**: Shared libraries and models.

## Current Status

*   **Coordinator**: Functional (API, DB, Scheduling, Janitor).
*   **Worker**: Functional (Polling, Execution). Note: Capabilities detection is currently a stub.
*   **Client**: Stub/Placeholder.

## Getting Started (Development)

This project is currently in active development.

### Configuration
Configuration is handled via YAML files for each package. Please refer to the specific package documentation for detailed configuration options.

### Running Tests
Tests are run using `pytest`. Ensure your python environment is set up.

```bash
python3 -m pytest
```

You can target specific tests:
```bash
python3 -m pytest packages/dffmpeg-coordinator/tests/unit/test_janitor.py
```
