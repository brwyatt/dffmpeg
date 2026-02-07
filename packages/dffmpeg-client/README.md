# DFFmpeg Client

The client component is responsible for submitting jobs to the Coordinator. It provides a CLI tool and a Python library.

## Installation

```bash
pip install dffmpeg-client
```

## Configuration

The client requires a configuration to connect to the Coordinator. It searches for configuration in the following order:

1.  CLI Argument (`--config`)
2.  Environment Variable (`DFFMPEG_CLIENT_CONFIG`)
3.  Current Working Directory (`./dffmpeg-client.yaml`)
4.  User Config (`~/.config/dffmpeg/client.yaml`)
5.  System Config (`/etc/dffmpeg/client.yaml`)
6.  Venv Root (`sys.prefix/dffmpeg-client.yaml`)

**Environment Variables** can also override specific settings:
*   `DFFMPEG_COORDINATOR_URL` (Parses scheme/host/port/path)
*   `DFFMPEG_CLIENT_ID`
*   `DFFMPEG_HMAC_KEY`

### Example `dffmpeg-client.yaml`

```yaml
client_id: "my-client-id"
hmac_key: "base64-secret-key..."

coordinator:
  scheme: http
  host: localhost
  port: 8000

paths:
  Movies: "/mnt/media/movies"
  TV: "/mnt/media/tv"
```

## Path Mapping

The client automatically maps local paths to path variables required by the Coordinator.
For example, if you have `paths: {"Movies": "/mnt/media/movies"}` configured:
*   Input argument: `/mnt/media/movies/Action/movie.mkv`
*   Transformed argument sent to job: `$Movies/Action/movie.mkv`

This allows you to use standard absolute paths in your commands.

## Usage

### CLI

Submit a job:
```bash
dffmpeg-client submit -b ffmpeg -i /mnt/media/movies/input.mkv output.mp4
```

Submit and wait for completion (streaming logs):
```bash
dffmpeg-client submit --wait -b ffmpeg -i /mnt/media/movies/input.mkv output.mp4
```

Check status:
```bash
dffmpeg-client status <job_id>
```

List recent jobs:
```bash
dffmpeg-client status
```

Cancel a job:
```bash
dffmpeg-client cancel <job_id>
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
