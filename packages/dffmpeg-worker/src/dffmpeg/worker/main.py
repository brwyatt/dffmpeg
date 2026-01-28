import argparse
import logging

from dffmpeg.worker.config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="dffmpeg Worker")
    parser.add_argument("--config", "-c", type=str, default="config.yml", help="Path to config file")
    args = parser.parse_args()

    config = load_config(args.config)
    logger.info(f"Worker started with config: {config}")


if __name__ == "__main__":
    main()
