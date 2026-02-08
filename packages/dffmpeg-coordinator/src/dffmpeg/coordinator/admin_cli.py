import argparse
import asyncio
import os
import sys
from typing import cast

from dffmpeg.common.auth.request_signer import RequestSigner
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
        print(f"{identity.client_id:<20} {identity.role:<10} {key_str}")


async def user_show(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    show_key = args.show_key
    identity = await db.auth.get_identity(client_id, include_hmac_key=show_key)
    if not identity:
        print(f"User '{client_id}' not found.")
        sys.exit(1)

    print(f"Client ID: {identity.client_id}")
    print(f"Role:      {identity.role}")
    if show_key:
        print(f"HMAC Key:  {identity.hmac_key}")


async def user_add(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    role = cast(IdentityRole, args.role)
    # Check if user already exists
    existing = await db.auth.get_identity(client_id)
    if existing:
        print(f"User '{client_id}' already exists.")
        sys.exit(1)

    hmac_key = RequestSigner.generate_key()

    identity = AuthenticatedIdentity(client_id=client_id, role=role, hmac_key=hmac_key, authenticated=False)
    await db.auth.add_identity(identity)
    print(f"User '{client_id}' added successfully.")
    print(f"HMAC Key: {hmac_key}")


async def user_delete(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    success = await db.auth.delete_identity(client_id)
    if success:
        print(f"User '{client_id}' deleted successfully.")
    else:
        print(f"User '{client_id}' not found.")
        sys.exit(1)


async def user_rotate_key(db: DB, args: argparse.Namespace):
    client_id = args.client_id
    identity = await db.auth.get_identity(client_id)
    if not identity:
        print(f"User '{client_id}' not found.")
        sys.exit(1)

    hmac_key = RequestSigner.generate_key()

    identity.hmac_key = hmac_key
    await db.auth.add_identity(identity)
    print(f"HMAC Key for '{client_id}' rotated successfully.")
    print(f"New HMAC Key: {hmac_key}")


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
