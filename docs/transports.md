# Transport Configuration & Permissions

This document outlines the configuration and permission requirements for the supported messaging transports in DFFmpeg.

Transports are used for asynchronous communication between the Coordinator and Workers/Clients.

For a full list of configuration options, see the [Configuration Reference](configuration.md).

## HTTP Polling & Streaming

HTTP Polling is the default fallback transport mechanism. It requires no external message broker, routing messages purely via the Coordinator's database and API.

DFFmpeg's HTTP Polling supports **HTTP Streaming (NDJSON)**. When a client enables streaming, instead of opening a connection, timing out, and reconnecting (long-polling), the client opens a single persistent connection. The Coordinator will immediately yield new messages over this stream as they arrive.

### Configuration

Add the following to your `dffmpeg-*.yaml` configuration file to configure HTTP polling explicitly:

```yaml
transports:
  enabled_transports:
    - "http_polling"
  transport_settings:
    http_polling:
      streaming: true  # (Client/Worker Only) Enable HTTP streaming. Defaults to true.
      poll_wait: 5     # (Client/Worker Only) Timeout for long-polling. In streaming mode, dictates the keep-alive ping interval.
```

If `streaming` is enabled, the client sends an `Accept: application/x-ndjson` header. The Coordinator will respect this header and hold the connection open indefinitely, sending a keep-alive ping (a blank line) every `poll_wait` seconds to prevent load balancers from closing the idle connection. If `streaming` is false, it falls back to standard HTTP long-polling.

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
      port: 5672  # Optional, defaults to 5672
      use_tls: true
      verify_ssl: true # Verify server certificate
      use_srv: false # Set to true to use SRV records for discovery (Basic support)
      username: "dffmpeg-user"
      password: "your-password"
      vhost: "/" # Virtual host, defaults to "/"
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
