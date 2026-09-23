"""
Query / Monitoring CLI (with Lamport clock ordering)
-------------------------------------------------------
KEY CHANGE: query_by_trace now orders by lamport_clock instead of timestamp.
This is the actual demonstration of the whole point of Unit 3's logical
clocks: even if every machine's wall clock were unreliable or unsynchronized,
the lamport_clock column still gives us the CORRECT causal order, because it
was derived from Rule 1/Rule 2 of Lamport's algorithm, not from real time.
"""
import argparse
import sqlite3

DB_PATH = "../storage/logpulse.db"


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_rows(rows, show_lamport=True):
    if not rows:
        print("No matching log records found.")
        return
    print(f"\nFound {len(rows)} record(s):\n")
    for row in rows:
        marker = "  !!" if row["log_level"] == "ERROR" else "    "
        lamport_str = f"L={row['lamport_clock']:<4}" if show_lamport else ""
        print(f"{marker} [{row['timestamp']}] {lamport_str} {row['service_id']:<20} "
              f"{row['log_level']:<6} trace={row['trace_id']:<22} {row['message']}")
    print()


def query_by_trace(conn, trace_id):
    # ORDER BY lamport_clock, not timestamp -- this is the whole point.
    return conn.execute(
        "SELECT * FROM logs WHERE trace_id = ? ORDER BY lamport_clock ASC",
        (trace_id,),
    ).fetchall()


def query_by_service(conn, service_id, limit):
    return conn.execute(
        "SELECT * FROM logs WHERE service_id = ? ORDER BY timestamp DESC LIMIT ?",
        (service_id, limit),
    ).fetchall()


def query_by_level(conn, log_level, limit):
    return conn.execute(
        "SELECT * FROM logs WHERE log_level = ? ORDER BY timestamp DESC LIMIT ?",
        (log_level, limit),
    ).fetchall()


def main():
    parser = argparse.ArgumentParser(description="LogPulse query CLI")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--trace", help="Look up all logs for a trace_id, ordered by LAMPORT CLOCK")
    group.add_argument("--service", help="Look up recent logs for a service_id")
    group.add_argument("--level", help="Look up recent logs at a log_level")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    conn = connect()

    if args.trace:
        print(f"Tracing request '{args.trace}' across all services (ordered by logical clock)...")
        rows = query_by_trace(conn, args.trace)
    elif args.service:
        print(f"Recent logs for service '{args.service}' (limit {args.limit})...")
        rows = query_by_service(conn, args.service, args.limit)
    else:
        print(f"Recent logs at level '{args.level}' (limit {args.limit})...")
        rows = query_by_level(conn, args.level, args.limit)

    print_rows(rows)
    conn.close()


if __name__ == "__main__":
    main()