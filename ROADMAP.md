# DFFmpeg Roadmap

This document outlines the development phases for DFFmpeg, leading up to version 1.0 and beyond.

## Phase 1: Beta (Functional & Usable)
*Goal: A functional system suitable for home production environments.*

- [x] **MariaDB/MySQL Support**: Implement MariaDB/MySQL database backend via the DAO pattern.
- [x] **MQTT Transport**: Implement MQTT transport for worker-coordinator communication.
- [x] **Worker Status Code Reporting**: Relay process exit codes from worker to coordinator and client.
- [x] **Admin Management CLI (`dffmpeg-admin`)**: Create a tool for direct database management (e.g., bootstrapping users).
- [x] **Mount Point Monitoring**: Verify worker path mappings are mounted before registration.
- [x] **Active/Background Modes**: Client supports submitting jobs in active (monitored) or background (detached) modes, with heartbeat support.
- [ ] **CLI Parity (Part 1)**:
    - [ ] **CLI Status Window**: Add `--window` / `-w` to `dffmpeg-client status` for time-filtered views.
    - [ ] **Client Cluster View**: Add `dffmpeg-client workers` to view cluster load.
    - [ ] **Shared Rendering**: Implement shared output formatting for Client and Admin CLIs.
- [ ] **Packaging**: Automate building and publishing release artifacts (GitHub Releases).
- [x] **Quick-Start Guide**: The "short path" setup for people who just want something working.
- [x] **Worker Version Reporting**: Workers report their version on registration for compatibility checks.

## Phase 2: Version 1.0 (Production Ready)
*Goal: A stable, well-tested, and documented release.*

- [ ] **CLI Parity (Part 2)**:
    - [ ] **Admin Job List**: Add `job list` and `job status` to Admin CLI, matching Client capabilities.
- [ ] **Operational Health**:
    - [ ] **Coordinator Health Table**: Track coordinator instances in the DB for HA visibility.
    - [ ] **Janitor & Cleanup**: Implement on-demand cleanup tasks in Admin CLI (e.g., `dffmpeg-admin janitor clean-jobs`).
    - [x] **Dynamic Binary Validation**: Move allowed binaries list to Coordinator configuration.
- [x] **Documentation Completion**: Full setup guides, API references, and architecture documentation in `docs/`.
- [ ] **Dynamic Configuration (DB-backed)**: Move configuration settings to the database with Admin CLI management (Config table).
- [ ] **Automated End-to-End Testing**: Robust `pytest` suite covering the full job lifecycle.
- [ ] **Security Audit**: Final review of HMAC implementation and path signing.

## Phase 3: Optional / "Maybe" (v1.x or v2.0)
*Goal: Expanded compatibility and features.*

- [ ] **PostgreSQL Support**: Implement PostgreSQL database backend.
- [ ] **Cassandra Support**: Implement Cassandra database backend.
- [x] **RabbitMQ Transport**: Implement RabbitMQ transport.
- [ ] **Improved Transport Negotiation**: Better/fairer selection of transport.
- [ ] **Worker Capabilities**: Dynamic detection of FFmpeg features (codecs, formats).

## Phase 4: Post-1.0
*Goal: Advanced features and management tools.*

- [ ] **Advanced Scheduling**: Job priority and worker affinity logic.
