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

#### Cluster Status

Show the current status of the cluster (active workers and recent jobs).

```bash
dffmpeg-admin status [--window <seconds>]
```

#### Job Management

**List Jobs:**
List recent jobs.

```bash
dffmpeg-admin job list [--window <seconds>]
```

**Show Job Details:**
Show detailed information about a specific job.

```bash
dffmpeg-admin job show <job_id>
```

**Job Logs:**
Fetch historical logs for a job.

```bash
dffmpeg-admin job logs <job_id>
```

#### Worker Management

**List Workers:**
List all known workers.

```bash
dffmpeg-admin worker list [--window <seconds>]
```

**Show Worker Details:**
Show detailed information about a specific worker.

```bash
dffmpeg-admin worker show <worker_id>
```

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

## Security Management

The `security` command group allows you to manage encryption keys and perform security-related maintenance.

### Commands

#### Re-encrypt Stored HMAC Keys

Re-encrypt stored HMAC keys with a new encryption key, or remove encryption entirely. This is useful for rotating the server-side encryption keys.

```bash
dffmpeg-admin security re-encrypt [options]
```

**Options:**
*   **`--client-id <id>`**: Target a specific client ID.
*   **`--key-id <id>`**: The ID of the encryption key to use (as defined in `encryption_keys_file`). Defaults to the configured default key.
*   **`--decrypt`**: Remove encryption and store the HMAC key as plain text.
*   **`--limit <int>`**: Maximum number of records to process in batch mode (default: all).
*   **`--batch-size <int>`**: Number of records to process per batch (default: 100).

**Examples:**

1.  **Re-encrypt a single user:**
    ```bash
    dffmpeg-admin security re-encrypt --client-id worker01 --key-id new-key-2024
    ```

2.  **Remove encryption for a user:**
    ```bash
    dffmpeg-admin security re-encrypt --client-id worker01 --decrypt
    ```

3.  **Batch re-encrypt all users to the default key:**
    ```bash
    dffmpeg-admin security re-encrypt
    ```

4.  **Batch migrate users from an old key to a new key:**
    ```bash
    dffmpeg-admin security re-encrypt --key-id new-key-2024 --limit 1000
    ```

#### Generate Encryption Key

Generate a new random key for a specific encryption algorithm.

```bash
dffmpeg-admin security generate-key <algorithm>
```

*   **`<algorithm>`**: The encryption algorithm to use (e.g., `fernet`).

**Example:**
```bash
dffmpeg-admin security generate-key fernet
# Output: fernet:<YOUR_GENERATED_KEY>
```

### Encryption Keys Configuration

The Coordinator can encrypt stored HMAC keys using encryption keys defined in a configuration file.

NOTE: Key IDs MUST be strings, as must the value for `default_encryption_key_id`. If using numeric values, ensure they are quoted!

**Configuration (`dffmpeg-coordinator.yaml`):**

```yaml
database:
  repositories:
    auth:
      engine: sqlite
      encryption_keys_file: "/etc/dffmpeg/keys.yaml"
      default_encryption_key_id: "key-2023"
```

**Keys File (`keys.yaml`):**

The keys file maps Key IDs to Key Strings. The format for a key string is `algorithm:base64_encoded_key`.

```yaml
key-2023: "fernet:..."
key-2024: "fernet:..."
```

You can generate these keys using the `dffmpeg-admin security generate-key` command.

## Database Maintenance

### Janitor Service

The Coordinator runs a background "Janitor" task to clean up stale records.

*   **Stale Workers**: Workers that haven't sent a heartbeat within the configured threshold are marked as `offline`.
*   **Stale Jobs**: Jobs that haven't received a heartbeat (if active) are marked as `failed` or canceled.

Configuration for the Janitor can be found in the [Configuration Reference](configuration.md#janitor-configuration-janitor).
