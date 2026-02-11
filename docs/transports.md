# Transport Configuration & Permissions

This document outlines the configuration and permission requirements for the supported messaging transports in DFFmpeg.

Transports are used for asynchronous communication between the Coordinator and Workers/Clients.

## RabbitMQ

RabbitMQ is a supported transport backend. It uses AMQP 0-9-1.

### Configuration

Add the following to your `dffmpeg-*.yaml` configuration file:

```yaml
transports:
  enabled_transports:
    - "rabbitmq"
  transport_settings:
    rabbitmq:
      host: "rabbitmq.example.com"
      port: 5672  # Optional, defaults to 5672 (or 5671 for TLS)
      use_tls: true
      use_srv: false # Set to true to use SRV records for discovery
      username: "dffmpeg-user"
      password: "your-password"
      vhost: "dffmpeg" # Virtual host, defaults to "/"
      workers_exchange: "dffmpeg.workers" # Optional, defaults to "dffmpeg.workers"
      jobs_exchange: "dffmpeg.jobs" # Optional, defaults to "dffmpeg.jobs"
```

### Permissions

The following permissions are required for each component type. These are expressed as Regular Expressions (Perl-compatible) for Configure, Write, and Read operations on the vhost.

> **Note:** The permissions below assume the default exchange names `dffmpeg.workers` and `dffmpeg.jobs`. If you customize these names, adjust the regex accordingly.

#### Coordinator

The Coordinator needs full access to manage the exchanges.

| Permission | Regex | Description |
| :--- | :--- | :--- |
| **Configure** | `^dffmpeg\..*` | Can configure exchanges starting with `dffmpeg.` |
| **Write** | `^dffmpeg\..*` | Can publish to exchanges starting with `dffmpeg.` |
| **Read** | `^dffmpeg\..*` | Can bind queues to exchanges starting with `dffmpeg.` |

#### Worker

Each worker needs access to its own queue and the workers exchange. Replace `worker01` with the specific worker ID.

| Permission | Regex | Description |
| :--- | :--- | :--- |
| **Configure** | `^dffmpeg\.worker\.worker01$` | Can declare its own queue: `dffmpeg.worker.worker01` |
| **Write** | `^dffmpeg\.worker\.worker01$` | Can write to its own queue (for exclusivity/binding) |
| **Read** | `^dffmpeg\.workers$|^dffmpeg\.worker\.worker01$` | Can read from the `dffmpeg.workers` exchange and consume from its own queue. |

#### Client

Each client needs access to its job-specific queues and the jobs exchange. Replace `example-client` with the specific client ID.

| Permission | Regex | Description |
| :--- | :--- | :--- |
| **Configure** | `^dffmpeg\.job\.example-client\..*` | Can declare queues starting with `dffmpeg.job.example-client.` |
| **Write** | `^dffmpeg\.job\.example-client\..*` | Can write to its own queues. |
| **Read** | `^dffmpeg\.jobs$|^dffmpeg\.job\.example-client\..*` | Can read from the `dffmpeg.jobs` exchange and consume from its own queues. |

## MQTT

MQTT is a lightweight messaging protocol. DFFmpeg supports MQTT v3.1.1 and v5.0 (via `aiomqtt`).

### Configuration

Add the following to your `dffmpeg-*.yaml` configuration file:

```yaml
transports:
  enabled_transports:
    - "mqtt"
  transport_settings:
    mqtt:
      host: "mqtt.example.com"
      port: 8883 # Optional, defaults to 1883 (or 8883 for TLS)
      use_tls: true
      username: "dffmpeg-user"
      password: "your-password"
      topic_prefix: "dffmpeg" # Optional, defaults to "dffmpeg"
```

### Permissions

The following permissions are required for each component type.

> **Note:** The permissions below assume the default `topic_prefix` of `dffmpeg`. If you customize this, adjust the topics accordingly.

#### Coordinator

The Coordinator publishes messages to workers and clients. It does not subscribe to any topics (all uplink communication is via HTTP API).

| Permission | Topic | Description |
| :--- | :--- | :--- |
| **Publish** | `dffmpeg/workers/#` | Publish commands to any worker. |
| **Publish** | `dffmpeg/jobs/#` | Publish status updates to any client job. |
| **Subscribe** | *None* | The Coordinator does not subscribe. |

#### Worker

The Worker subscribes to its own command topic. Replace `worker01` with the specific worker ID.

| Permission | Topic | Description |
| :--- | :--- | :--- |
| **Publish** | *None* | The Worker does not publish. |
| **Subscribe** | `dffmpeg/workers/worker01` | Receive commands directed to this worker. |

#### Client

The Client subscribes to updates for its submitted jobs. Replace `example-client` with the specific client ID.

| Permission | Topic | Description |
| :--- | :--- | :--- |
| **Publish** | *None* | The Client does not publish. |
| **Subscribe** | `dffmpeg/jobs/example-client/#` | Receive updates for any job submitted by this client. |

### EMQX Configuration Example

If you are using EMQX, you can use the following ACL rules (example in `acl.conf` syntax):

```erlang
%% Coordinator
{allow, {user, "dffmpeg-coordinator"}, publish, ["dffmpeg/workers/#", "dffmpeg/jobs/#"]}.
{deny, {user, "dffmpeg-coordinator"}, subscribe, ["#"]}.

%% Worker (Pattern matching for dynamic IDs if supported by your auth plugin, otherwise explicit)
{allow, {user, "dffmpeg-worker01"}, subscribe, ["dffmpeg/workers/worker01"]}.
{deny, {user, "dffmpeg-worker01"}, publish, ["#"]}.

%% Client
{allow, {user, "dffmpeg-example-client"}, subscribe, ["dffmpeg/jobs/example-client/#"]}.
{deny, {user, "dffmpeg-example-client"}, publish, ["#"]}.
```
