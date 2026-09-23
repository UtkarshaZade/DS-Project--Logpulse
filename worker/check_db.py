"""Quick throwaway script to inspect what's landed in SQLite so far."""
import sqlite3

conn = sqlite3.connect("../storage/logpulse.db")

total = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
print(f"Total rows stored: {total}")

print("\n--- Incident trace rows ---")
rows = conn.execute(
    "SELECT service_id, log_level, timestamp, message FROM logs WHERE trace_id = 'trace-incident-001'"
).fetchall()
for row in rows:
    print(row)

print("\n--- Row counts per service ---")
counts = conn.execute(
    "SELECT service_id, COUNT(*) FROM logs GROUP BY service_id"
).fetchall()
for service, count in counts:
    print(f"{service}: {count}")

conn.close()