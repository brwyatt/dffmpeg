import argparse
import os
import sys

import uvicorn

from dffmpeg.coordinator.config import load_config


def main():
    parser = argparse.ArgumentParser(description="dffmpeg Coordinator")
    parser.add_argument("--config", "-c", type=str, help="Path to config file")
    parser.add_argument("--host", type=str, help="Bind host")
    parser.add_argument("--port", type=int, help="Bind port")
    parser.add_argument("--dev", action="store_true", help="Enable development mode (reload, debug logs)")

    args = parser.parse_args()

    if args.config:
        os.environ["DFFMPEG_COORDINATOR_CONFIG"] = args.config

    try:
        # Load config to get defaults for host/port
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

    host = args.host or config.host
    port = args.port or config.port

    log_level = "info"
    if args.dev:
        log_level = "debug"

    # We must run uvicorn on the import string for reload to work
    uvicorn.run(
        "dffmpeg.coordinator.api.main:app",
        host=host,
        port=port,
        reload=args.dev,
        log_level=log_level,
    )


if __name__ == "__main__":
    main()
