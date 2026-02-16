import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import cast

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.colors import Colors, colorize, colorize_status
from dffmpeg.common.crypto import CryptoManager
from dffmpeg.common.models import AuthenticatedIdentity, IdentityRole
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
    online = await db.workers.get_workers_by_status("online")
    offline = await db.workers.get_workers_by_status("offline", since_seconds=3600 * 24)
    workers = online + offline

    # Sort: Online first, then by last seen (descending), then ID (ascending)
    workers.sort(key=lambda w: (w.status != "online", -(w.last_seen.timestamp() if w.last_seen else 0), w.worker_id))

    print(f"{'Worker ID':<20} {'Status':<21} {'Last Seen':<20}")
    print("-" * 52)
    for w in workers:
        last_seen_str = w.last_seen.strftime("%Y-%m-%d %H:%M:%S") if w.last_seen else "-"
        print(f"{w.worker_id:<20} {colorize_status(w.status):<21} {last_seen_str}")


async def worker_show(db: DB, args: argparse.Namespace):
    worker_id = args.worker_id
    worker = await db.workers.get_worker(worker_id)
    if not worker:
        print(colorize(f"Worker '{worker_id}' not found.", Colors.RED))
        sys.exit(1)

    last_seen_str = worker.last_seen.strftime("%Y-%m-%d %H:%M:%S") if worker.last_seen else "-"
    if worker.last_seen < datetime.now(timezone.utc) - timedelta(seconds=worker.registration_interval):
        last_seen_str = colorize(last_seen_str, color=Colors.YELLOW)
    else:
        last_seen_str = colorize(last_seen_str, color=Colors.GREEN)

    print(f"Worker ID:    {colorize(worker.worker_id, Colors.CYAN)}")
    print(f"Status:       {colorize_status(worker.status)}")
    print(f"Last Seen:    {last_seen_str}")
    print(f"Binaries:     {', '.join(worker.binaries)}")
    print(f"Capabilities: {', '.join(worker.capabilities)}")
    print(f"Paths:        {', '.join(worker.paths)}")
    print(f"Interval:     {worker.registration_interval}s")
    print(f"Transport:    {worker.transport}")


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
    parser.add_argument("--config", "-c", type=str, help="Path to coordinator config file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # User subcommands
    user_parser = subparsers.add_parser("user", help="User management")
    user_subparsers = user_parser.add_subparsers(dest="subcommand", required=True)

    # user list
    list_parser = user_subparsers.add_parser("list", help="List all users")
    list_parser.add_argument("--show-key", action="store_true", help="Display HMAC keys")
    list_parser.set_defaults(func=user_list)

    # user show
    show_parser = user_subparsers.add_parser("show", help="Show user details")
    show_parser.add_argument("client_id", help="Client ID of the user")
    show_parser.add_argument("--show-key", action="store_true", help="Display HMAC key")
    show_parser.set_defaults(func=user_show)

    # user add
    add_parser = user_subparsers.add_parser("add", help="Add a new user")
    add_parser.add_argument("client_id", help="Client ID for the new user")
    add_parser.add_argument(
        "--role", choices=["client", "worker", "admin"], default="client", help="Role for the new user"
    )
    add_parser.set_defaults(func=user_add)

    # user delete
    delete_parser = user_subparsers.add_parser("delete", help="Delete a user")
    delete_parser.add_argument("client_id", help="Client ID of the user to delete")
    delete_parser.set_defaults(func=user_delete)

    # user rotate-key
    rotate_parser = user_subparsers.add_parser("rotate-key", help="Rotate a user's HMAC key")
    rotate_parser.add_argument("client_id", help="Client ID of the user")
    rotate_parser.set_defaults(func=user_rotate_key)

    # Worker subcommands
    worker_parser = subparsers.add_parser("worker", help="Worker management")
    worker_subparsers = worker_parser.add_subparsers(dest="subcommand", required=True)

    # worker list
    w_list_parser = worker_subparsers.add_parser("list", help="List workers")
    w_list_parser.set_defaults(func=worker_list)

    # worker show
    w_show_parser = worker_subparsers.add_parser("show", help="Show worker details")
    w_show_parser.add_argument("worker_id", help="Worker ID")
    w_show_parser.set_defaults(func=worker_show)

    # Security subcommands
    security_parser = subparsers.add_parser("security", help="Security management")
    security_subparsers = security_parser.add_subparsers(dest="subcommand", required=True)

    # security re-encrypt
    reencrypt_parser = security_subparsers.add_parser("re-encrypt", help="Re-encrypt stored HMAC keys")
    reencrypt_parser.add_argument("--client-id", help="Specific Client ID to re-encrypt")
    reencrypt_parser.add_argument("--key-id", help="Target Key ID for encryption (default: configured default)")
    reencrypt_parser.add_argument(
        "--decrypt", action="store_true", help="Remove encryption (store plain text) instead of re-encrypting"
    )
    reencrypt_parser.add_argument("--limit", type=int, help="Maximum number of users to process in batch mode")
    reencrypt_parser.add_argument(
        "--batch-size", type=int, default=100, help="Number of users to process per batch (default: 100)"
    )
    reencrypt_parser.set_defaults(func=security_reencrypt)

    # security generate-key
    gen_key_parser = security_subparsers.add_parser("generate-key", help="Generate a new encryption key")
    gen_key_parser.add_argument("algorithm", help="Encryption algorithm to use (e.g., fernet)")
    gen_key_parser.set_defaults(func=security_generate_key)

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
