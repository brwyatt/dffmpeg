# DFFmpeg Client

The client component is responsible for submitting jobs to the Coordinator. It provides a CLI tool and a Python library.

## Installation

```bash
pip install dffmpeg-client
```

## Configuration

For detailed configuration options, see the [Configuration Reference](../../../docs/configuration.md).

The client requires a configuration to connect to the Coordinator.

## Path Mapping

The client automatically maps local paths to path variables required by the Coordinator.
For example, if you have `paths: {"Movies": "/mnt/media/movies"}` configured:
*   Input argument: `/mnt/media/movies/Action/movie.mkv`
*   Transformed argument sent to job: `$Movies/Action/movie.mkv`

This allows you to use standard absolute paths in your commands.

## Usage

### CLI

Submit a job (Active Mode):
By default, the client submits the job, starts a heartbeat loop to monitor it, and streams logs until completion. If the client disconnects, the job will be canceled by the coordinator.
```bash
dffmpeg-client submit -b ffmpeg -i /mnt/media/movies/input.mkv output.mp4
```

Submit a job (Background/Detached Mode):
Use the `--detach` (or `-D`) flag to submit a job and exit immediately. The job will continue running on the worker independently.
```bash
dffmpeg-client submit --detach -b ffmpeg -i /mnt/media/movies/input.mkv output.mp4
```

Check cluster status (dashboard):
```bash
dffmpeg-client status [--window <seconds>]
```

List jobs:
```bash
dffmpeg-client job list [--window <seconds>]
```

Show job details:
```bash
dffmpeg-client job show <job_id>
```

List workers:
```bash
dffmpeg-client worker list [--window <seconds>]
```

Show worker details:
```bash
dffmpeg-client worker show <worker_id>
```

Cancel a job:
```bash
dffmpeg-client job cancel <job_id>
```

Attach to a running job:
```bash
dffmpeg-client job attach <job_id>
```

View job logs:
Fetch historical logs for a job.
```bash
dffmpeg-client job logs <job_id>
```

Follow job logs:
Fetch historical logs and continue polling for new logs.
```bash
dffmpeg-client job logs --follow <job_id>
```

### Transparent Proxy

You can create a symlink to the proxy entry point to use dffmpeg as a drop-in replacement for ffmpeg.

```bash
ln -s $(which dffmpeg_proxy) /usr/local/bin/ffmpeg
```

Now, when you run `ffmpeg`, it will actually submit a job to the cluster, stream the logs back, and exit with the remote process's exit code.

```bash
ffmpeg -i /mnt/media/movies/input.mkv output.mp4
```
