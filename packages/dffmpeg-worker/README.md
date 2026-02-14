# DFFmpeg Worker

The worker agent that runs on encoding nodes. It polls the Coordinator for assigned jobs and executes them using the local FFmpeg binary.

## Configuration

For detailed configuration options, see the [Configuration Reference](../../../docs/configuration.md).

Configuration is handled via a YAML file (default: `dffmpeg-worker.yaml`).

## Running

```bash
dffmpeg-worker
```

Command line arguments can override configuration:
```bash
dffmpeg-worker --config /path/to/custom_config.yaml
```

## Known Issues / TODOs

*   **Capabilities Detection:** The worker currently has a hardcoded/empty list of capabilities. Dynamic detection of supported codecs/formats is not yet implemented.
