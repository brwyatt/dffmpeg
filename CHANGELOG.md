# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Fixed

### Changed

## [0.6.0] - 2026-10-04

### Upgrade Note
- **Byte-Transparent Standard-Stream Data Plane**: This release introduces high-throughput, byte-transparent stdout binary streaming. This separates control plane and data plane concerns to support direct binary stream outputs (like `-f image2pipe pipe:1` or output piping) without control plane or database bloat.
- **Backward Compatibility**: Fully backward-compatible capability negotiation is included. Legacy clients are supported transparently without breaking.

### Added
- **Standard-Stream Data Plane (Binary Stdout)**: Realized a high-throughput, byte-transparent streaming architecture that separates signaling/control from binary payloads. (#48)
- **Coordinator Stream Storage Engine**: Integrated atomic chunk writing, resumption support, and automatic sliding-window chunk pruning to limit disk usage. (#48)
- **Worker Adaptive Line Buffer with One-Way Latch**: Worker dynamically switches from text logging to high-performance binary chunks on detection of non-text bytes or 64KB line overflow. (#48)
- **Client Binary Stream Piping with Terminal Safety**: Client streams binary chunks directly to `sys.stdout.buffer` and halts/warns if raw binary is directed to an interactive terminal. (#48)

### Fixed
- **Prevent Path Traversal**: Tightened security on stream storage manager download endpoints.
- **Dangling Coroutines & Warning Suppression**: Resolved potential pending tasks on stream disconnects.
- **Avoid Logging Secrets**: Cleaned up loggers to prevent exposing credentials.

### Changed
- **Extended Dependency Ranges**: Relaxed cryptography dependency range limit and bumped minor dev dependencies.

## [0.5.0] - 2026-07-02

### Upgrade Note
- **HTTP Polling Backend Proxying**: The Coordinator can now proxy HTTP polling and streaming requests to an underlying RabbitMQ or MQTT broker. To utilize this, configure `backend_transport: "rabbitmq"` (or `"mqtt"`) under the coordinator's `http_polling` transport settings.
- **Durable Queue Cleanup (RabbitMQ)**: If you are migrating a cluster where workers or clients previously connected directly to RabbitMQ to use this HTTP-polling-backed proxy setup, RabbitMQ may still have old, worker-specific durable queues bound to the exchanges (e.g., `dffmpeg.worker.{worker_id}`). Since the Coordinator now uses a single, coordinator-wide persistent queue and dynamic bindings, these old durable queues are no longer used and should be manually deleted from the RabbitMQ Management UI to prevent them from accumulating duplicate messages.

### Added
- **Message-Bus Backed HTTP Polling Proxy**: Workers and clients can now benefit from central message bus scalability while communicating strictly via HTTP.
- **Durable Handshake Recovery**: Handshake and in-flight messages are automatically preserved in the DB if a streaming client unexpectedly disconnects mid-delivery, and are safely drained/deduplicated on reconnection.
- **Global Cache-Control Middlewares**: Enforced cache-preventing headers globally across all Coordinator API and Web Dashboard endpoints.
- **Self-Healing Proxy Streaming**: Automatically injects `X-Accel-Buffering: no` and `Connection: keep-alive` headers on NDJSON streams to natively bypass buffering in reverse-proxies like Nginx.

### Fixed
- **Hanging Coroutines Warning**: Resolved `Task was destroyed but it is pending!` warnings by ensuring all pending stream-receive tasks are cleanly canceled and gathered.
- **Proxy-Friendly Web Status Resources**: Fixed dashboard resource paths to load correctly behind reverse proxies with custom path prefixes.
- **Graceful Metadata Sanitization**: Fixed an edge-case crash in `sanitize_transport_metadata` when handling non-dict/null payloads.

### Changed
- Switched repository type checking configuration from Pylance to **Based Pyright**.

## [0.4.0] - 2026-06-12

### Added
- **Graceful Shutdown of Workers**: Implemented a two-phase shutdown sequence (drain existing jobs on first SIGTERM/SIGINT, then fast-teardown on second signal or systemd timeout). (#28)
- **Graceful Shutdown of Coordinator**: Added a configurable shutdown delay logic, instant Janitor teardown, and 503 health reporting to allow load-balancers to gracefully drain connections. (#28)

### Changed
- Updated cryptography requirement from `<48.0.0,>=46.0.0` to `>=46.0.0,<49.0.0` in `packages/dffmpeg-common`. (#27)

## [0.3.0] - 2026-05-09

### Upgrade Note
- **2-Stage Worker Registration**: This release introduces a "Reachability Check" where workers must verify their transport connection before being marked as fully online. To ensure a smooth transition, **update all workers to 0.3.0 before updating the coordinator**. Old workers connecting to a new coordinator will fail to complete the registration handshake and remain offline.

### Added
- 2-step worker registration (Reachability Check / Transport Handshake Verification) (#20).
- Dark Mode to Web UI.

### Changed
- Updated cryptography requirement.

## [0.2.0] - 2026-03-22

### Added
- On-Demand Janitor task scheduling via API and Admin CLI (`dffmpeg-admin janitor`).
- Metrics endpoint (`/metrics`) for system observability.
- IP Restrictions for the Coordinator Web Status page (`allowed_dashboard_ips`).
- Allow batched processing of worker messages.

### Fixed
- RabbitMQ connection and reconnection handling (#18).
- Database datetime issue when running on MySQL/MariaDB.
- Excluded current worker from task re-assignment to prevent immediate recursive assignment failures.

## [0.1.1] - 2026-02-28

### Added
- Support for `cwd` (Working Directory) in Job Requests and database models (#10, #12).
- Initial Database Migration mechanism to safely evolve schema (#12).
- Improved retry logic for heartbeat and registration loops (#9, #11).

### Fixed
- Jobs occasionally failing with `exit_code=null` despite underlying process succeeding (#6).
- Client hangs after job success due to dropped status messages (#7, #8).
- Aggressive Janitor timeouts and insufficient client/worker heartbeat retries (#9, #11).
- Jellyfin attachment extraction failures due to missing worker working directory support (#10, #12).

## [0.1.0] - 2026-02-20

### Added
- Initial beta release of DFFmpeg.
- **Coordinator**: Centralized job management, API, and database support (SQLite/MySQL).
- **Worker**: FFmpeg execution agent with support for path mapping and dynamic binary selection.
- **Client**: CLI for submitting jobs, querying status, and viewing logs.
- **Transports**: Support for HTTP Polling, MQTT, and RabbitMQ messaging.
- **Admin CLI**: Tool for managing users and system status.
- **Docs**: Comprehensive documentation for configuration and deployment.
