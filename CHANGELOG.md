# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
