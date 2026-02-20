# Security Model

DFFmpeg is designed to operate in distributed environments where components may communicate over untrusted networks.

## Authentication

All communication between the Client, Worker, and Coordinator is authenticated using **HMAC-SHA256**.

### Why HMAC?

*   **Performance**: HMAC is computationally inexpensive compared to full public-key cryptography (e.g., mTLS).
*   **Simplicity**: It relies on a shared secret (the Key) rather than a complex PKI infrastructure.
*   **Integrity**: It guarantees that the message has not been tampered with in transit.

### Implementation Details

Each request includes the following headers:

*   `X-DFFmpeg-Client-ID`: The ID of the requester (Client or Worker).
*   `X-DFFmpeg-Signature`: The HMAC-SHA256 signature of the request.
*   `X-DFFmpeg-Timestamp`: A timestamp to prevent replay attacks (requests older than 30 seconds are rejected).

The signature is calculated over the following string:

```
{method}|{path}|{timestamp}|{sha256(payload)}
```

Example:
```
POST|/api/v1/jobs|1678886400|ba7816bf8f01...20015ad
```

### Credential Scoping

To improve security, the API enforces network-based access controls. Each registered identity (Client or Worker) can be restricted to specific IP addresses or CIDR ranges.

*   **Default:** New users are created with global access (`0.0.0.0/0` and `::/0`) unless specified otherwise.
*   **Enforcement:** The Coordinator checks the source IP of every request against the allowed CIDRs for the authenticated user. If the IP matches, the request proceeds to HMAC verification. If not, it is rejected with a `401 Unauthorized` error.
*   **Proxies:** If the Coordinator is behind a trusted reverse proxy (configured via `trusted_proxies`), it will respect the `X-Forwarded-For` header to determine the real client IP. Localhost (`127.0.0.1`) is trusted by default if unspecified.

## Key Management

Keys are managed by the Coordinator.

### Generation

Use the `dffmpeg-admin` tool to generate keys:

```bash
dffmpeg-admin user add my-worker --role worker
# Output: <BASE64_KEY>
```

### Storage

*   **Coordinator**: Stores keys in the database (encrypted at rest if `encryption_keys` are configured).
*   **Client/Worker**: Stores the key in their respective YAML configuration files.

> **Warning:** Treat these keys like passwords. Anyone with the key can impersonate the client or worker.
