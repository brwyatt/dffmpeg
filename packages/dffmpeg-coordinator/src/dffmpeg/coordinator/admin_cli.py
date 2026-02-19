import argparse
import asyncio
import os
import sys
from typing import cast

from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.cli_utils import (
    add_client_id_arg,
    add_config_arg,
    add_job_subcommand,
    add_window_arg,
    add_worker_subcommand,
    setup_subcommand,
)
from dffmpeg.common.colors import Colors, colorize
from dffmpeg.common.crypto import CryptoManager
from dffmpeg.common.formatting import (
    print_job_details,
    print_job_list,
    print_worker_details,
    print_worker_list,
)
from dffmpeg.common.models import AuthenticatedIdentity, IdentityRole, JobLogsMessage
from dffmpeg.coordinator.config import load_config
from dffmpeg.coordinator.db import DB


async def user_list(db: DB, args: argparse.Namespace):
    show_key = args.show_key
    identities = await db.auth.list_identities(include_hmac_key=show_key)
    print(f"{'Client ID':<20} {'Role':<10} {'HMAC Key' if show_key else ''}")
    print("-" * (40 if show_key else 31))
    for identity in identities:
        key_str = identity.hmac_key if show_key else ""
        role_color = (
            Colors.GREEN if identity.role == "client" else Colors.BLUE if identity.role == "worker" else Colors.RED
        )
        print(f"{identity.client_id:<20} {colorize(identity.role, role_color):<21} {key_str}")


async def user_show(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    show_key = args.show_key
    identity = await db.auth.get_identity(client_id, include_hmac_key=show_key)
    if not identity:
        print(f"User '{client_id}' not found.")
        sys.exit(1)

    print(f"Client ID: {colorize(identity.client_id, Colors.CYAN)}")
    role_color = Colors.GREEN if identity.role == "client" else Colors.BLUE if identity.role == "worker" else Colors.RED
    print(f"Role:      {colorize(identity.role, role_color)}")
    if show_key:
        print(f"HMAC Key:  {identity.hmac_key}")


async def user_add(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    role = cast(IdentityRole, args.role)
    # Check if user already exists
    existing = await db.auth.get_identity(client_id)
    if existing:
        print(colorize(f"User '{client_id}' already exists.", Colors.RED))
        sys.exit(1)

    hmac_key = RequestSigner.generate_key()

    identity = AuthenticatedIdentity(client_id=client_id, role=role, hmac_key=hmac_key, authenticated=False)
    await db.auth.add_identity(identity)
    print(colorize(f"User '{client_id}' added successfully.", Colors.GREEN))
    print(f"HMAC Key: {hmac_key}")


async def user_delete(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    success = await db.auth.delete_identity(client_id)
    if success:
        print(colorize(f"User '{client_id}' deleted successfully.", Colors.GREEN))
    else:
        print(colorize(f"User '{client_id}' not found.", Colors.RED))
        sys.exit(1)


async def user_rotate_key(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    identity = await db.auth.get_identity(client_id)
    if not identity:
        print(colorize(f"User '{client_id}' not found.", Colors.RED))
        sys.exit(1)

    hmac_key = RequestSigner.generate_key()

    identity.hmac_key = hmac_key
    await db.auth.add_identity(identity)
    print(colorize(f"HMAC Key for '{client_id}' rotated successfully.", Colors.GREEN))
    print(f"New HMAC Key: {hmac_key}")


async def worker_list(db: DB, args: argparse.Namespace):
    window = args.window if hasattr(args, "window") else 3600 * 24
    online = await db.workers.get_workers_by_status("online")
    offline = await db.workers.get_workers_by_status("offline", since_seconds=window)
    workers = online + offline
    print_worker_list(workers)


async def worker_show(db: DB, args: argparse.Namespace):
    worker_id = args.worker_id
    worker = await db.workers.get_worker(worker_id)
    if not worker:
        print(colorize(f"Worker '{worker_id}' not found.", Colors.RED))
        sys.exit(1)

    print_worker_details(worker)


async def job_list(db: DB, args: argparse.Namespace):
    window = args.window if hasattr(args, "window") else 3600
    # Show requester for admin view
    jobs = await db.jobs.get_dashboard_jobs(recent_window_seconds=window)
    print_job_list(jobs, show_requester=True)


async def job_show(db: DB, args: argparse.Namespace):
    job_id_str = args.job_id
    try:
        job_id = ULID.from_str(job_id_str)
    except ValueError:
        print(colorize(f"Invalid Job ID: {job_id_str}", Colors.RED))
        sys.exit(1)

    job = await db.jobs.get_job(job_id)
    if not job:
        print(colorize(f"Job '{job_id_str}' not found.", Colors.RED))
        sys.exit(1)

    print_job_details(job)


async def job_logs(db: DB, args: argparse.Namespace):
    job_id_str = args.job_id
    try:
        job_id = ULID.from_str(job_id_str)
    except ValueError:
        print(colorize(f"Invalid Job ID: {job_id_str}", Colors.RED))
        sys.exit(1)

    job = await db.jobs.get_job(job_id)
    if not job:
        print(colorize(f"Job '{job_id_str}' not found.", Colors.RED))
        sys.exit(1)

    messages = await db.messages.get_job_messages(job_id, message_type="job_logs")

    for msg in messages:
        if isinstance(msg, JobLogsMessage):
            for log in msg.payload.logs:
                stream = sys.stdout if log.stream == "stdout" else sys.stderr
                print(log.content, file=stream)
                stream.flush()


async def status_cmd(db: DB, args: argparse.Namespace):
    print(colorize("=== Workers ===", Colors.MAGENTA))
    # Default window for status view if not specified (handled by argparse defaults usually)
    if not hasattr(args, "window"):
        args.window = 3600
    await worker_list(db, args)
    print()
    print(colorize("=== Recent Jobs ===", Colors.MAGENTA))
    await job_list(db, args)


async def security_reencrypt(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    key_id = args.key_id
    decrypt = args.decrypt
    limit = args.limit
    batch_size = args.batch_size

    if decrypt:
        if key_id is not None:
            print("WARNING: ignoring `--key-id` when `--decrypt` provided!")
        key_id = None
    elif key_id is None:
        key_id = db.auth._default_key_id

    if client_id:
        # Single mode
        print(f"{'De' if decrypt else 'Re-en'}crypting user '{client_id}'...")
        success = await db.auth.reencrypt_identity(client_id, key_id=key_id, decrypt=decrypt)
        if success:
            print(f"User '{client_id}' {'de' if decrypt else 're-en'}crypted successfully.")
        else:
            print(f"User '{client_id}' not found.")
            sys.exit(1)
    else:
        # Batch mode
        print(
            f"Batch {'de' if decrypt else 're-en'}crypting users (Limit: {limit or 'Unlimited'}, "
            f"Batch Size: {batch_size})..."
        )
        processed_count = 0

        while True:
            # Determine how many to fetch in this batch
            current_limit = batch_size
            if limit is not None:
                remaining = limit - processed_count
                if remaining <= 0:
                    break
                current_limit = min(batch_size, remaining)

            # Find candidates that are NOT using the target key configuration
            candidates = await db.auth.get_identities_not_using_key(key_id, limit=current_limit)

            candidates = list(candidates)
            if not candidates:
                break

            for cid in candidates:
                await db.auth.reencrypt_identity(cid, key_id=key_id, decrypt=decrypt)
                processed_count += 1
                print(f"{'De' if decrypt else 'Re-en'}crypted user '{cid}'")

        print(f"Finished. Total users {'de' if decrypt else 're-en'}crypted: {processed_count}")


async def security_generate_key(db: DB, args: argparse.Namespace):
    algorithm = args.algorithm
    # Use an empty dictionary for keys as we only need to generate a new key
    crypto = CryptoManager({})

    try:
        key = crypto.generate_key(algorithm)
        print(key)
    except ValueError as e:
        print(f"Error: {e}")
        print(f"Available algorithms: {', '.join(crypto.loaded_providers.keys())}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="dffmpeg Administrative CLI")
    add_config_arg(parser, help_text="Path to coordinator config file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Status
    status_parser = setup_subcommand(subparsers, "status", "Show cluster status", func=status_cmd)
    add_window_arg(status_parser)

    # User subcommands
    user_parser = setup_subcommand(subparsers, "user", "User management")
    user_subparsers = user_parser.add_subparsers(dest="subcommand", required=True)

    # user list
    list_parser = setup_subcommand(user_subparsers, "list", "List all users", func=user_list)
    list_parser.add_argument("--show-key", action="store_true", help="Display HMAC keys")

    # user show
    show_parser = setup_subcommand(user_subparsers, "show", "Show user details", func=user_show)
    add_client_id_arg(show_parser, help_text="Client ID of the user")
    show_parser.add_argument("--show-key", action="store_true", help="Display HMAC key")

    # user add
    add_parser = setup_subcommand(user_subparsers, "add", "Add a new user", func=user_add)
    add_client_id_arg(add_parser, help_text="Client ID for the new user")
    add_parser.add_argument(
        "--role", choices=["client", "worker", "admin"], default="client", help="Role for the new user"
    )

    # user delete
    delete_parser = setup_subcommand(user_subparsers, "delete", "Delete a user", func=user_delete)
    add_client_id_arg(delete_parser, help_text="Client ID of the user to delete")

    # user rotate-key
    rotate_parser = setup_subcommand(user_subparsers, "rotate-key", "Rotate a user's HMAC key", func=user_rotate_key)
    add_client_id_arg(rotate_parser, help_text="Client ID of the user")

    # Worker subcommands
    add_worker_subcommand(subparsers, list_func=worker_list, show_func=worker_show)

    # Job subcommands
    add_job_subcommand(subparsers, list_func=job_list, show_func=job_show, logs_func=job_logs)

    # Security subcommands
    security_parser = setup_subcommand(subparsers, "security", "Security management")
    security_subparsers = security_parser.add_subparsers(dest="subcommand", required=True)

    # security re-encrypt
    reencrypt_parser = setup_subcommand(
        security_subparsers, "re-encrypt", "Re-encrypt stored HMAC keys", func=security_reencrypt
    )
    reencrypt_parser.add_argument("--client-id", help="Specific Client ID to re-encrypt")
    reencrypt_parser.add_argument("--key-id", help="Target Key ID for encryption (default: configured default)")
    reencrypt_parser.add_argument(
        "--decrypt", action="store_true", help="Remove encryption (store plain text) instead of re-encrypting"
    )
    reencrypt_parser.add_argument("--limit", type=int, help="Maximum number of users to process in batch mode")
    reencrypt_parser.add_argument(
        "--batch-size", type=int, default=100, help="Number of users to process per batch (default: 100)"
    )

    # security generate-key
    gen_key_parser = setup_subcommand(
        security_subparsers, "generate-key", "Generate a new encryption key", func=security_generate_key
    )
    gen_key_parser.add_argument("algorithm", help="Encryption algorithm to use (e.g., fernet)")

    args = parser.parse_args()

    if args.config:
        os.environ["DFFMPEG_COORDINATOR_CONFIG"] = args.config

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

    db = DB(config.database)

    async def run():
        # Ensure auth table exists
        await db.auth.setup()

        if hasattr(args, "func"):
            await args.func(db, args)

    try:
        asyncio.run(run())
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
