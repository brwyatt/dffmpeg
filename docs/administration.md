# Administration

This guide covers administrative tasks for managing a DFFmpeg cluster, primarily using the `dffmpeg-admin` CLI tool provided by the `dffmpeg-coordinator` package.

## User Management

The Coordinator uses HMAC-based authentication. Users (Clients and Workers) must be registered in the database with a shared secret key.

The `dffmpeg-admin` tool allows you to manage these users directly.

### Usage

```bash
dffmpeg-admin [global options] <command> [subcommand] [options]
```

**Global Options:**
*   `--config`, `-c`: Path to the coordinator configuration file (default: searches standard locations).

### Commands

#### Add User

Register a new user and generate an HMAC key.

```bash
dffmpeg-admin user add <client_id> --role <role>
```

*   **`<client_id>`**: The unique identifier for the user (e.g., `worker01`, `my-client`).
*   **`--role`**: The user's role. Must be one of:
    *   `client`: Can submit jobs.
    *   `worker`: Can execute jobs.
    *   `admin`: Can perform administrative API calls (future use).

**Example:**
```bash
dffmpeg-admin user add worker01 --role worker
# Output: Generated HMAC key for worker01: <YOUR_GENERATED_KEY>
```

> **Important:** The generated key is only displayed once. Save it securely!

#### List Users

List all registered users.

```bash
dffmpeg-admin user list [--show-key]
```

*   **`--show-key`**: Display the decrypted HMAC key for each user. Use with caution!

**Example:**
```bash
dffmpeg-admin user list
# Output:
# Client ID           Role                Created At
# --------------------------------------------------
# worker01            worker              2023-10-27T10:00:00
# my-client           client              2023-10-27T10:05:00
```

#### Show User Details

Display details for a specific user.

```bash
dffmpeg-admin user show <client_id> [--show-key]
```

*   **`<client_id>`**: The user to inspect.
*   **`--show-key`**: Display the decrypted HMAC key.

#### Rotate Key

Generate a new HMAC key for an existing user. The old key will immediately become invalid.

```bash
dffmpeg-admin user rotate-key <client_id>
```

*   **`<client_id>`**: The user whose key you want to rotate.

**Example:**
```bash
dffmpeg-admin user rotate-key worker01
# Output: Rotated HMAC key for worker01: <NEW_GENERATED_KEY>
```

#### Delete User

Remove a user from the database.

```bash
dffmpeg-admin user delete <client_id>
```

*   **`<client_id>`**: The user to remove.

**Example:**
```bash
dffmpeg-admin user delete old-worker
# Output: Deleted user old-worker
```

## Database Maintenance

### Janitor Service

The Coordinator runs a background "Janitor" task to clean up stale records.

*   **Stale Workers**: Workers that haven't sent a heartbeat within the configured threshold are marked as `offline`.
*   **Stale Jobs**: Jobs that haven't received a heartbeat (if active) are marked as `failed` or canceled.

Configuration for the Janitor can be found in the [Configuration Reference](configuration.md#janitor-configuration-janitor).
