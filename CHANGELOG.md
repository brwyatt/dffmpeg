# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added
- Initial beta release of DFFmpeg.
- **Coordinator**: Centralized job management, API, and database support (SQLite/MySQL).
- **Worker**: FFmpeg execution agent with support for path mapping and dynamic binary selection.
- **Client**: CLI for submitting jobs, querying status, and viewing logs.
- **Transports**: Support for HTTP Polling, MQTT, and RabbitMQ messaging.
- **Admin CLI**: Tool for managing users and system status.
- **Docs**: Comprehensive documentation for configuration and deployment.
