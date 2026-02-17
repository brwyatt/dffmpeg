# dffmpeg

![Status](https://img.shields.io/badge/status-beta-orange)
![License](https://img.shields.io/badge/license-GPLv3-blue)
![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![Code Style](https://img.shields.io/badge/code%20style-black-000000)
[![Support me on GitHub](https://img.shields.io/github/sponsors/brwyatt?label=GitHub%20Sponsors)](https://github.com/sponsors/brwyatt)

**dffmpeg** is a distributed, centrally-coordinated FFmpeg job manager designed for scalability and resilience.

Unlike traditional ad-hoc scripts or tightly coupled clusters, `dffmpeg` decouples job submission from execution using a central coordinator. This allows jobs to be submitted from any client (e.g., a media server) and have them processed by a pool of workers, handling failures, load balancing, and network interruptions gracefully.

Heavily inspired by [joshuaboniface/rffmpeg](https://github.com/joshuaboniface/rffmpeg), but built from the ground up to support an environment with multiple clients and to not require the workers and clients to have identical mount paths for network shares, as well as to avoid SSH access.

---

## 🚀 Key Features

*   **Path-Blind Coordination**: The Coordinator never needs to know the client's or worker's local file paths. It manages logical `Variables`, and Workers translate them locally. This additionally allows the worker and clients to have different mount paths.
*   **Centralized State & Resilience**: Jobs are persisted in a database (SQLite/MySQL). Allowing for jobs to be re-queued if the assigned worker does not respond, and allow jobs to continue running during brief network interruptions through async communication.
*   **Pluggable Transport**: Supports multiple communication backends. Start simple with **HTTP Polling**, or scale up with **MQTT** or **RabbitMQ**.
*   **Secure by Default**: All communication is signed with HMAC-SHA256, identifying both Clients and Workers.
*   **Dashboard Included**: Built-in web interface for monitoring cluster health and job status and history.

## 📦 Project Structure

This monorepo contains the following packages:

| Package | Description |
| :--- | :--- |
| **`dffmpeg-coordinator`** | The central brain. Runs the API, manages the database, and assigns jobs. |
| **`dffmpeg-worker`** | Runs on encoding nodes, polls for work, and executes FFmpeg. |
| **`dffmpeg-client`** | The CLI and library for submitting jobs and querying status. |
| **`dffmpeg-common`** | Shared logic, models, and transport implementations. |

## 🛠️ Quick Start

This project is currently in **Beta**.

These instructions assume you have (at least) two hosts:
1.  **Host A (Media Server):** Runs Jellyfin/Plex, the `dffmpeg-coordinator`, and the `dffmpeg-client`.
2.  **Host B (Worker):** A separate machine that will perform the encoding. (Multiple can be added)

### Host A: Media Server Setup

1.  **Install Components:**
    Create a virtual environment in `/opt/dffmpeg` and install the packages.
    ```bash
    # Create Venv
    sudo python3 -m venv /opt/dffmpeg

    # Install Common, Coordinator, and Client
    sudo /opt/dffmpeg/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-common"
    sudo /opt/dffmpeg/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-coordinator"
    sudo /opt/dffmpeg/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-client"
    ```

2.  **Configure Coordinator:**
    Create `/opt/dffmpeg/dffmpeg-coordinator.yaml`:
    ```yaml
    database:
      engine_defaults:
        sqlite:
          path: /var/lib/dffmpeg/dffmpeg.db
    ```
    Ensure the directory exists and is writable by the user running the coordinator:
    ```bash
    sudo mkdir -p /var/lib/dffmpeg
    sudo chown -R $USER:$USER /var/lib/dffmpeg
    ```

3.  **Start the Coordinator:**
    You can run it manually for testing, or set up a system service (recommended).
    *   **Manual:** `/opt/dffmpeg/bin/dffmpeg-coordinator`
    *   **Systemd:** See example at `docs/examples/systemd/dffmpeg-coordinator.service`.

4.  **Create Users:**
    Use the admin CLI to generate credentials.
    ```bash
    # Create client user (Save the Output Key!)
    sudo /opt/dffmpeg/bin/dffmpeg-admin user add jellyfin --role client

    # Create worker user (Save the Output Key!)
    sudo /opt/dffmpeg/bin/dffmpeg-admin user add worker01 --role worker
    ```

5.  **Configure Client:**
    Create `/opt/dffmpeg/dffmpeg-client.yaml` (Ensure this file is readable by the `jellyfin` user!):
    ```yaml
    client_id: jellyfin
    hmac_key: "YOUR_CLIENT_KEY_FROM_STEP_4"
    coordinator:
      host: 127.0.0.1
      port: 8000
    paths:
      # Map local media path to a logical variable
      Movies: /media/movies
      TV: /media/tv
    ```

6.  **Replace FFmpeg:**
    Symlink the `dffmpeg` proxy to where your media server expects `ffmpeg`.
    ```bash
    # Link it (Example for Jellyfin)
    sudo ln -s /opt/dffmpeg/bin/dffmpeg_proxy /usr/lib/jellyfin-ffmpeg/ffmpeg
    sudo ln -s /opt/dffmpeg/bin/dffmpeg_proxy /usr/lib/jellyfin-ffmpeg/ffprobe
    ```

### Host B: Worker Setup

1.  **Install Worker:**
    Create a virtual environment in `/opt/dffmpeg` and install the packages.
    ```bash
    # Create Venv
    sudo python3 -m venv /opt/dffmpeg

    # Install Common and Worker
    sudo /opt/dffmpeg/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-common"
    sudo /opt/dffmpeg/bin/pip install "git+https://github.com/brwyatt/dffmpeg.git#subdirectory=packages/dffmpeg-worker"
    ```

2.  **Configure Worker:**
    Create `/opt/dffmpeg/dffmpeg-worker.yaml`:
    ```yaml
    client_id: worker01
    hmac_key: "YOUR_WORKER_KEY_FROM_STEP_4"
    coordinator:
      host: 192.168.1.10  # IP of Host A
      port: 8000
    paths:
      # Map logical variables back to local worker paths
      Movies: /mnt/share/movies
      TV: /mnt/share/tv
    binaries:
      # Point to the actual installed ffmpeg binary
      ffmpeg: /usr/bin/ffmpeg
    ```

3.  **Start Worker:**
    You can run it manually for testing, or set up a system service (recommended).
    *   **Manual:** `/opt/dffmpeg/bin/dffmpeg-worker`
    *   **Systemd:** See example at `docs/examples/systemd/dffmpeg-worker.service`.

*For more details and advanced configuration, see [Getting Started](docs/getting-started.md).*

## 📚 Documentation

*   **[Getting Started](docs/getting-started.md)**: Step-by-step setup guide.
*   **[Configuration Reference](docs/configuration.md)**: Detailed options for all components.
*   **[Architecture](docs/architecture.md)**: Deep dive into the system design.
*   **[Security Model](docs/security-model.md)**: Authentication and key management.
*   **[Transports](docs/transports.md)**: Setting up RabbitMQ or MQTT.

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md).

## 📄 License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](LICENSE) file for details.
