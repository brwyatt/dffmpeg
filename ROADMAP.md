# DFFmpeg Roadmap

This document outlines the development phases for DFFmpeg, leading up to version 1.0 and beyond.

## Phase 1: Beta (Functional & Usable)
*Goal: A functional system suitable for home production environments.*

- [ ] **MariaDB/MySQL Support**: Implement MariaDB/MySQL database backend via the DAO pattern.
- [ ] **MQTT Transport**: Implement MQTT transport for worker-coordinator communication.
- [x] **Worker Status Code Reporting**: Relay process exit codes from worker to coordinator and client.
- [x] **Admin Management CLI (`dffmpeg-admin`)**: Create a tool for direct database management (e.g., bootstrapping users).
- [x] **Mount Point Monitoring**: Verify worker path mappings are mounted before registration.

## Phase 2: Version 1.0 (Production Ready)
*Goal: A stable, well-tested, and documented release.*

- [ ] **Client Full Status**: CLI command (something like `dffmpeg-client status --all`) for viewing complete cluster status (similar to the web dashboard).
- [ ] **Documentation Completion**: Full setup guides, API references, and architecture documentation in `docs/`.
- [ ] **Automated End-to-End Testing**: Robust `pytest` suite covering the full job lifecycle (submission, assignment, execution, completion).
- [ ] **Security Audit**: Final review of HMAC implementation and path signing.

## Phase 3: Optional / "Maybe" (v1.x or v2.0)
*Goal: Expanded compatibility and features.*

- [ ] **PostgreSQL Support**: Implement PostgreSQL database backend.
- [ ] **RabbitMQ Transport**: Implement RabbitMQ transport.
- [ ] **Worker Capabilities**: Dynamic detection of FFmpeg features (codecs, formats).

## Phase 4: Post-1.0
*Goal: Advanced features and management tools.*

- [ ] **Advanced Scheduling**: Job priority and worker affinity logic.
