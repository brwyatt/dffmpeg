# Architecture

This document describes the design and components of the DFFmpeg distributed system.

## High-Level Overview

DFFmpeg is designed to coordinate distributed FFmpeg encoding jobs. It separates the **job submission** (Client), **job management** (Coordinator), and **job execution** (Worker).

### Key Concepts

*   **Coordinator-Centric**: The Coordinator is the single source of truth for job state.
*   **Pull-Based Execution**: Workers poll the Coordinator for jobs (or receive notifications to poll).
*   **Path-Blind**: The Coordinator stores logical paths (using variables), allowing clients and workers to have different mount points.
*   **Stateless Protocol**: Communication is primarily stateless HTTP, authenticated via HMAC.

## Scenarios

### 1. Simple Architecture (Development / Small Scale)
This is the default configuration and is ideal for development, testing, or small single-server deployments.

*   **Database**: SQLite (local file).
*   **Transport**: HTTP Polling (no external broker required).

```mermaid
graph TD
    subgraph "Worker Node"
        W[Worker]
        FF[FFmpeg]
    end

    subgraph "Client Node"
        C[Client CLI]
    end

    subgraph "Coordinator Node"
        Coord[Coordinator API]
        DB[(SQLite)]
    end

    C -- HTTP POST (Submit) --> Coord
    W -- HTTP GET (Poll) --> Coord
    Coord -- Read/Write --> DB
    W -- Exec --> FF
```

### 2. High Availability (HA) Reference Architecture
This setup represents a proven production-grade environment, designed for resilience and horizontal scalability.

*   **Load Balancing**: Redundant HAProxy pairs (Active/Passive via Keepalived) providing Virtual IPs (VIPs) for each service tier.
*   **Message Broker**: 3x RabbitMQ hosts (Clustered) behind an HAProxy VIP.
*   **Database**: 3x MariaDB/Galera Cluster hosts behind an HAProxy VIP.
*   **Coordinator**: 2x `dffmpeg-coordinator` instances (Active/Active) behind an HAProxy VIP.
*   **Transport**: RabbitMQ (AMQP) for low latency and durability.

```mermaid
graph TD
    subgraph "Clients"
        Client1[Client CLI]
        Client2[Client CLI]
    end

    subgraph "Workers"
        Worker1[Worker Agent]
        Worker2[Worker Agent]
        Worker3[Worker Agent]
        Worker4[Worker Agent]
    end

    subgraph "Coordinator HAProxy Pair"
        APP_VIP("Coordinator VIP (Active)")
        APP_VIP_stby("Coordinator VIP (Standby)")
    end
    subgraph "MQ HAProxy Pair"
        MQ_VIP("MQ VIP (Active)")
        MQ_VIP_stby("MQ VIP (Standby)")
    end
    subgraph "DB HAProxy Pair"
        DB_VIP("DB VIP (Active)")
        DB_VIP_stby("DB VIP (Standby)")
    end

    subgraph "Coordinators"
        C1[Coordinator 1]
        C2[Coordinator 2]
    end

    subgraph "Transport Layer"
        MQ1[RabbitMQ 1]
        MQ2[RabbitMQ 2]
        MQ3[RabbitMQ 3]
    end

    subgraph "Database Layer"
        DB1[(MariaDB Galera 1)]
        DB2[(MariaDB Galera 2)]
        DB3[(MariaDB Galera 3)]
    end

    %% External Traffic
    Client1 & Client2 & Worker1 & Worker2 & Worker3 & Worker4 -- HTTP --> APP_VIP
    APP_VIP --> C1 & C2

    Client1 & Client2 & Worker1 & Worker2 & Worker3 & Worker4 -- AMQP --> MQ_VIP
    MQ_VIP --> MQ1 & MQ2 & MQ3

    %% Internal Traffic
    C1 & C2 -- SQL --> DB_VIP
    DB_VIP --> DB1 & DB2 & DB3

    C1 & C2 -- AMQP --> MQ_VIP
```

### 3. Real-Time Updates with MQTT
MQTT is ideal for lightweight status updates to clients and workers, especially in IoT-like networks.

*   **Transport**: MQTT.
*   **Note**: HTTP is still used for all "Uplink" communication (submitting jobs, updating status). MQTT is "Downlink" only (notifications).

```mermaid
graph TD
    subgraph "Message Broker"
        MQTT[MQTT Broker]
    end

    subgraph "Coordinator"
        Coord[Coordinator API]
    end

    subgraph "Worker"
        W[Worker]
    end

    subgraph "Client"
        C[Client]
    end

    %% Uplink (HTTP)
    C -- HTTP POST (Submit) --> Coord
    W -- HTTP POST (Update Status) --> Coord

    %% Downlink (MQTT)
    Coord -- Publish --> MQTT
    MQTT -- Subscribe (Job Updates) --> C
    MQTT -- Subscribe (Commands) --> W
```

## Component Details

### Coordinator
*   **API**: FastAPI application serving the REST API.
*   **Scheduler**: Determines which job goes to which worker based on capabilities (future) and load.
*   **Janitor**: Background task that cleans up stale jobs and workers (e.g., if a worker crashes and stops sending heartbeats).

### Worker
*   **Executor**: Runs the actual FFmpeg process. Captures stdout/stderr and streams it back to the Coordinator.
*   **Mount Manager**: Verifies that required paths are mounted before accepting work.

### Client
*   **Submission**: Parses local paths, converts them to variables, and submits the job.
*   **Monitor**: Polls (or listens via MQTT/AMQP) for job status and logs.

## State Diagrams

### Worker Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Offline
    Offline --> Online: Register
    Online --> Offline: Deregister
    Online --> Offline: Timeout (Janitor)
    Online --> Online: Heartbeat / Activity
```

### Job Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Pending

    Pending --> Assigned: Scheduler Assigns
    Pending --> Failed: Timeout (Stale)
    Pending --> Canceled: User Cancel

    Assigned --> Running: Worker Accepts
    Assigned --> Pending: Timeout (Retry)
    Assigned --> Canceled: User Cancel

    Running --> Completed: Success
    Running --> Failed: Failure / Timeout
    Running --> Canceling: User Cancel / Monitor Timeout

    Canceling --> Canceled: Worker Confirms / Forced

    Completed --> [*]
    Failed --> [*]
    Canceled --> [*]
```
