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

### 1. Basic / Development Setup
In the simplest deployment, all components can run on a single machine or a small LAN.

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

### 2. Production with RabbitMQ
For production environments, RabbitMQ provides durable messaging and lower latency than polling.

*   **Database**: MariaDB/MySQL (possibly Galera cluster for HA).
*   **Transport**: RabbitMQ (AMQP).
*   **High Availability**: Multiple Coordinators behind a Load Balancer (VIP).

```mermaid
graph TD
    subgraph "Infrastructure"
        MQ[RabbitMQ Cluster]
        DB[(MariaDB Cluster)]
        LB[Load Balancer / VIP]
    end

    subgraph "Coordinator Cluster"
        C1[Coordinator 1]
        C2[Coordinator 2]
    end

    subgraph "Workers"
        W1[Worker 1]
        W2[Worker 2]
    end

    subgraph "Clients"
        Client[Client CLI]
    end

    Client -- HTTP --> LB
    W1 -- HTTP --> LB
    W2 -- HTTP --> LB
    
    LB --> C1
    LB --> C2
    
    C1 -- SQL --> DB
    C2 -- SQL --> DB
    
    C1 -- Publish --> MQ
    C2 -- Publish --> MQ
    
    MQ -- Consume (Commands) --> W1
    MQ -- Consume (Commands) --> W2
    MQ -- Consume (Updates) --> Client
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
