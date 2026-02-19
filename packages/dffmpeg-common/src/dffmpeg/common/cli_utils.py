import argparse
from typing import Callable, Optional


def add_config_arg(parser: argparse.ArgumentParser, help_text: str = "Path to config file"):
    """Adds the standard --config argument."""
    parser.add_argument("--config", "-c", help=help_text)


def add_window_arg(parser: argparse.ArgumentParser, default: int = 3600, help_text: str = "Time window in seconds"):
    """Adds the standard --window argument."""
    parser.add_argument("--window", "-w", type=int, default=default, help=f"{help_text} (default: {default})")


def add_job_id_arg(parser: argparse.ArgumentParser, help_text: str = "Job ID"):
    """Adds the standard job_id argument."""
    parser.add_argument("job_id", help=help_text)


def add_worker_id_arg(parser: argparse.ArgumentParser, help_text: str = "Worker ID"):
    """Adds the standard worker_id argument."""
    parser.add_argument("worker_id", help=help_text)


def add_client_id_arg(parser: argparse.ArgumentParser, help_text: str = "Client ID"):
    """Adds the standard client_id argument."""
    parser.add_argument("client_id", help=help_text)


def setup_subcommand(
    subparsers: argparse._SubParsersAction,
    name: str,
    help_text: str,
    func: Optional[Callable] = None,
) -> argparse.ArgumentParser:
    """Helper to create a subparser and set its default function."""
    parser = subparsers.add_parser(name, help=help_text)
    if func:
        parser.set_defaults(func=func)
    return parser


def add_worker_subcommand(
    subparsers: argparse._SubParsersAction,
    list_func: Optional[Callable] = None,
    show_func: Optional[Callable] = None,
) -> argparse._SubParsersAction:
    """
    Adds the standard 'worker' subcommand with 'list' and 'show' sub-commands.
    Returns the subparser action for adding more subcommands if needed.
    """
    worker_parser = setup_subcommand(subparsers, "worker", "Worker management")
    worker_subparsers = worker_parser.add_subparsers(dest="subcommand", required=True)

    if list_func:
        w_list = setup_subcommand(worker_subparsers, "list", "List workers", func=list_func)
        add_window_arg(w_list, default=86400, help_text="Time window for offline workers")

    if show_func:
        w_show = setup_subcommand(worker_subparsers, "show", "Show worker details", func=show_func)
        add_worker_id_arg(w_show)

    return worker_subparsers


def add_job_subcommand(
    subparsers: argparse._SubParsersAction,
    list_func: Optional[Callable] = None,
    show_func: Optional[Callable] = None,
    logs_func: Optional[Callable] = None,
    include_logs_follow: bool = False,
) -> argparse._SubParsersAction:
    """
    Adds the standard 'job' subcommand with 'list', 'show', and optionally 'logs' sub-commands.
    Returns the subparser action for adding more subcommands if needed.
    """
    job_parser = setup_subcommand(subparsers, "job", "Job management")
    job_subparsers = job_parser.add_subparsers(dest="subcommand", required=True)

    if list_func:
        j_list = setup_subcommand(job_subparsers, "list", "List jobs", func=list_func)
        add_window_arg(j_list)

    if show_func:
        j_show = setup_subcommand(job_subparsers, "show", "Show job details", func=show_func)
        add_job_id_arg(j_show)

    if logs_func:
        j_logs = setup_subcommand(job_subparsers, "logs", "Fetch job logs", func=logs_func)
        add_job_id_arg(j_logs)
        # Note: --follow is specific to logs, so we add it here if requested
        if include_logs_follow:
            j_logs.add_argument("--follow", "-f", action="store_true", help="Follow log output")

    return job_subparsers
