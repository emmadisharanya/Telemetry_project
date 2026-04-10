import argparse
import json
import duckdb
from pathlib import Path
from query.engine import TelemetryQueryEngine


def get_all_parquet_paths():
    paths = [str(p) for p in Path('data').rglob('*.parquet')]
    return paths


def cmd_errors(args):
    engine = TelemetryQueryEngine()
    result = engine.errors_for_customer_last_hours(
        customer_id=args.customer, hours=args.hours
    )
    print(json.dumps(result.rows, indent=2, default=str))


def cmd_customers(args):
    paths = get_all_parquet_paths()
    if not paths:
        print("No data found. Run replay.py first.")
        return

    paths_str = ", ".join([f"'{p}'" for p in paths])
    conn = duckdb.connect(':memory:')
    df = conn.execute(f"""
        SELECT
            customer_id,
            COUNT(*) as total_events,
            SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors,
            SUM(CASE WHEN level = 'WARN'  THEN 1 ELSE 0 END) as warns,
            SUM(CASE WHEN level = 'INFO'  THEN 1 ELSE 0 END) as info
        FROM read_parquet([{paths_str}])
        GROUP BY customer_id
        ORDER BY total_events DESC
    """).df()
    print(df.to_string(index=False))


def cmd_stats(args):
    paths = get_all_parquet_paths()
    if not paths:
        print("No data found. Run replay.py first.")
        return

    paths_str = ", ".join([f"'{p}'" for p in paths])
    conn = duckdb.connect(':memory:')

    total = conn.execute(f"SELECT COUNT(*) as total FROM read_parquet([{paths_str}])").df()
    print(f"\nTotal events:  {total['total'][0]}")

    by_level = conn.execute(f"""
        SELECT level, COUNT(*) as count
        FROM read_parquet([{paths_str}])
        GROUP BY level
        ORDER BY count DESC
    """).df()
    print("\nBy level:")
    print(by_level.to_string(index=False))

    by_service = conn.execute(f"""
        SELECT service, COUNT(*) as count
        FROM read_parquet([{paths_str}])
        GROUP BY service
        ORDER BY count DESC
    """).df()
    print("\nBy service:")
    print(by_service.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Firetiger Telemetry CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # errors command
    errors = sub.add_parser("errors", help="Show errors for a customer")
    errors.add_argument("--customer", required=True, help="Customer ID")
    errors.add_argument("--hours", type=int, default=9999, help="Lookback window in hours (default: 9999)")

    # customers command
    sub.add_parser("customers", help="List all customer IDs and their event counts")

    # stats command
    sub.add_parser("stats", help="Show overall stats across all data")

    args = parser.parse_args()

    if args.command == "errors":
        cmd_errors(args)
    elif args.command == "customers":
        cmd_customers(args)
    elif args.command == "stats":
        cmd_stats(args)


if __name__ == "__main__":
    main()