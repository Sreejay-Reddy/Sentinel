import os
import argparse
import psycopg

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except ImportError:
    pass

from .core import inspect
from .events import history

def get_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise EnvironmentError(
            "No DATABASE_URL found. Set it in your environment or .env file.\n"
            "Example: DATABASE_URL=postgresql://user:pass@localhost/mydb"
        )
    return psycopg.connect(db_url)

def main():
    parser = argparse.ArgumentParser(prog="sen", description="Sentinel CLI")
    subparsers = parser.add_subparsers(dest="command")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect a lease by key")
    inspect_parser.add_argument("key", type=str)

    history_parser = subparsers.add_parser("history", help="Show execution history for a key")
    history_parser.add_argument("key", type=str)
    history_parser.add_argument("--limit", type=int, default=50, help="Max events to show (default: 50)")

    args = parser.parse_args()

    if args.command == "inspect":
        with get_conn() as conn:
            result = inspect(conn, args.key)

        if result is None:
            print(f"No lease found for key: {args.key}")
        else:
            print(f"status:           {result.status}")
            print(f"lease_alive:      {result.lease_alive}")
            print(f"owner_id:         {result.owner_id}")
            print(f"fencing_token:    {result.fencing_token}")
            print(f"lease_expires_at: {result.lease_expires_at}")
            print(f"lease_updated_at: {result.lease_updated_at}")
            print(f"hard_expires_at:  {result.hard_expires_at}")
            print(f"execution_result: {result.execution_result}")

    elif args.command == "history":
        with get_conn() as conn:
            events = history(conn, args.key, limit=args.limit)

        if not events:
            print(f"No history found for key: {args.key}")
        else:
            print(f"History for key: {args.key}  ({len(events)} events)\n")
            for e in events:
                print(f"  {e}")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()