# Getting Started

This guide walks you through setting up a complete DFFmpeg cluster, consisting of a Coordinator, a Worker, and a Client.

We will install each component into its own virtual environment under `/opt/dffmpeg/`.

## Prerequisites

*   Python 3.12+
*   `git`
*   Shared storage (e.g., NFS, SMB) accessible by both the Worker and the Client.
*   (Optional) RabbitMQ or MQTT broker.

## 0. System Preparation

Create a dedicated system user for running the services:

```bash
sudo useradd -r -s /bin/false dffmpeg
```

## 1. Setting up the Coordinator

The Coordinator manages the cluster state.

### Installation

1.  **Create directory and virtual environment**:
    ```bash
    sudo mkdir -p /opt/dffmpeg/coordinator
    sudo chown dffmpeg:dffmpeg /opt/dffmpeg/coordinator
    sudo -u dffmpeg python3 -m venv /opt/dffmpeg/coordinator/venv
    ```

2.  **Install the package**:
    ```bash
    sudo -u dffmpeg /opt/dffmpeg/coordinator/venv/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-coordinator"
    ```

### Configuration

For a full list of configuration options, including detailed database settings, see the [Configuration Reference](configuration.md).

1.  **Create a configuration file** (`/opt/dffmpeg/coordinator/dffmpeg-coordinator.yaml`):

    ```yaml
    database:
      engine_defaults:
        sqlite:
          path: "/opt/dffmpeg/coordinator/dffmpeg.db"
        mysql:
          host: "127.0.0.1"
          user: "dffmpeg"
          password: "yourpassword"
          database: "dffmpeg"
          use_ssl: true # Recommended

      # By default, all repositories use the 'sqlite' engine.
      # You can override specific repositories to use 'mysql':
      # repositories:
      #   jobs:
      #     engine: mysql
    
    # Enable web dashboard
    web_dashboard_enabled: true
    ```

2.  **Initialize the Admin User**:
    You need to create a user for the client and the worker. For more details on user management, see [Administration Guide](administration.md).

    ```bash
    # Create an admin/client user
    sudo /opt/dffmpeg/coordinator/venv/bin/dffmpeg-admin --config /opt/dffmpeg/coordinator/dffmpeg-coordinator.yaml user add my-client --role client
    # Output: Generated HMAC key for my-client: <YOUR_CLIENT_KEY>

    # Create a worker user
    sudo /opt/dffmpeg/coordinator/venv/bin/dffmpeg-admin --config /opt/dffmpeg/coordinator/dffmpeg-coordinator.yaml user add worker01 --role worker
    # Output: Generated HMAC key for worker01: <YOUR_WORKER_KEY>
    ```

3.  **Start the Coordinator**:

    ```bash
    sudo /opt/dffmpeg/coordinator/venv/bin/dffmpeg-coordinator --config /opt/dffmpeg/coordinator/dffmpeg-coordinator.yaml
    # Running on http://127.0.0.1:8000
    ```

## 2. Setting up the Worker

The Worker executes the jobs.

### Installation

1.  **Create directory and virtual environment**:
    ```bash
    sudo mkdir -p /opt/dffmpeg/worker
    sudo chown dffmpeg:dffmpeg /opt/dffmpeg/worker
    sudo -u dffmpeg python3 -m venv /opt/dffmpeg/worker/venv
    ```

2.  **Install the package**:
    ```bash
    sudo -u dffmpeg /opt/dffmpeg/worker/venv/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-worker"
    ```

### Configuration

1.  **Create a configuration file** (`/opt/dffmpeg/worker/dffmpeg-worker.yaml`):

    ```yaml
    client_id: worker01
    hmac_key: "<YOUR_WORKER_KEY>"
    
    coordinator:
      host: 127.0.0.1
      port: 8000
      scheme: http
    
    paths:
      # Map the logical variable 'Media' to the local path
      Media: "/mnt/share/media"
    
    binaries:
      ffmpeg: "/usr/bin/ffmpeg"
    ```

2.  **Start the Worker**:

    ```bash
    sudo /opt/dffmpeg/worker/venv/bin/dffmpeg-worker --config /opt/dffmpeg/worker/dffmpeg-worker.yaml
    ```

## 3. Setting up the Client

The Client submits jobs.

### Installation

1.  **Create directory and virtual environment**:
    ```bash
    sudo mkdir -p /opt/dffmpeg/client
    sudo python3 -m venv /opt/dffmpeg/client/venv
    ```

2.  **Install the package**:
    ```bash
    sudo /opt/dffmpeg/client/venv/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-client"
    ```

### Configuration

1.  **Create a configuration file** (`/opt/dffmpeg/client/dffmpeg-client.yaml`):

    ```yaml
    client_id: my-client
    hmac_key: "<YOUR_CLIENT_KEY>"
    
    coordinator:
      host: 127.0.0.1
      port: 8000
      scheme: http
    
    paths:
      # Map the same logical variable 'Media' to the client's local path
      # (This could be different from the worker's path, e.g., on Windows)
      Media: "/Volumes/Media"
    ```

### Usage

1.  **Submit a Job**:

    ```bash
    # The client translates the local path to "$Media/movies/test.mkv"
    /opt/dffmpeg/client/venv/bin/dffmpeg-client --config /opt/dffmpeg/client/dffmpeg-client.yaml submit -b ffmpeg -i /Volumes/Media/movies/test.mkv output.mp4
    ```

### Using as an FFmpeg Replacement (Proxy)

DFFmpeg includes a proxy mode that mimics the `ffmpeg` CLI interface. This allows you to use DFFmpeg as a drop-in replacement for `ffmpeg` in scripts or other applications (like Sonarr/Radarr).

1.  **Symlink the proxy**:
    Create a symlink from your desired location (e.g., `/usr/local/bin/ffmpeg`) to the `dffmpeg_proxy` executable.

    ```bash
    sudo ln -s /opt/dffmpeg/client/venv/bin/dffmpeg_proxy /usr/local/bin/dffmpeg
    # Or replace ffmpeg entirely (backup original first!)
    # sudo mv /usr/bin/ffmpeg /usr/bin/ffmpeg.orig
    # sudo ln -s /opt/dffmpeg/client/venv/bin/dffmpeg_proxy /usr/bin/ffmpeg
    ```

2.  **Environment Configuration**:
    The proxy reads configuration from the environment or default config paths. Ensure the `dffmpeg-client.yaml` is in a standard location (e.g., `~/.config/dffmpeg/client.yaml` or `/etc/dffmpeg/client.yaml`) or set `DFFMPEG_CLIENT_CONFIG`.

    ```bash
    export DFFMPEG_CLIENT_CONFIG=/opt/dffmpeg/client/dffmpeg-client.yaml
    ffmpeg -i input.mkv output.mp4
    ```

## 4. (Optional) Enabling Transports

To use RabbitMQ or MQTT, update the `transports` section in all three configuration files. See [Transport Configuration](transports.md) for details.

## 5. Deployment as System Services

For production, use the provided systemd unit files to run the Coordinator and Worker as background services.

1.  **Install the service files**:
    Copy the example unit files to `/etc/systemd/system/`. You can find them in `docs/examples/systemd/` in the repository.

    ```bash
    # Example if you have the repo checked out:
    sudo cp docs/examples/systemd/dffmpeg-coordinator.service /etc/systemd/system/
    sudo cp docs/examples/systemd/dffmpeg-worker.service /etc/systemd/system/
    ```

2.  **Reload and Enable**:

    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable --now dffmpeg-coordinator
    sudo systemctl enable --now dffmpeg-worker
    ```

3.  **Check Status**:

    ```bash
    systemctl status dffmpeg-coordinator
    systemctl status dffmpeg-worker
    ```
