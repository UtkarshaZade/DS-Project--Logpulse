"""
Consumer / Storage Worker (with Distributed Mutual Exclusion)
------------------------------------------------------------------
NEW IN THIS PHASE: a periodic "archive maintenance" task that must run on
ONLY ONE worker at a time, even when multiple worker processes are running
(exactly like worker-1 and worker-2 from Phase 4).

HOW the lock works, using Redis's SET ... NX EX:
  - NX means "only set this key if it does NOT already exist". If two
    workers call this at the exact same instant, Redis guarantees only
    ONE of them gets back success -- that's the actual mutual exclusion
    guarantee, enforced by Redis itself, not by our own code racing.
  - EX gives the lock an expiry. If whichever worker holds the lock
    crashes before releasing it, the lock doesn't stay stuck forever --
    it expires on its own and another worker can eventually acquire it.
    This matters: a naive lock with no timeout can deadlock the whole
    system if its holder dies.

HONEST LIMITATION (worth knowing for viva): our release step reads the
lock's value and only deletes it if it still matches our own worker name.
This avoids accidentally deleting a lock that expired and was already
re-acquired by someone else. It is NOT fully atomic against every possible
race (a true production-grade solution, "Redlock", uses a small Lua script
so the read+delete happens as one atomic step) -- but it is enough to
correctly demonstrate the core mutual-exclusion concept here.
"""
import os
import sys
import sqlite3
import threading
import time

sys.path.insert(0, "../proto")
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
STREAM_NAME = "logpulse:logs"
GROUP_NAME = "logpulse-workers"
DB_PATH = "../storage/logpulse.db"

ARCHIVE_LOCK_KEY = "lock:archive_task"
ARCHIVE_LOCK_TTL_SECONDS = 15   # auto-release if holder crashes mid-task
ARCHIVE_CHECK_INTERVAL_SECONDS = 10  # how often each worker attempts to grab the lock


def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    # NOTE: we deliberately do NOT use WAL mode here. WAL depends on
    # consistent file-locking between every process touching the file --
    # that assumption breaks when one writer is inside a Docker container
    # (via a Windows-to-Linux bind mount) and another writes the same file
    # directly from Windows. We learned this the hard way: WAL caused
    # actual database corruption in that mixed setup. busy_timeout alone
    # is safer here -- it just makes a writer wait and retry instead of
    # failing immediately when the file is briefly locked.
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id TEXT UNIQUE,
            timestamp TEXT,
            service_id TEXT,
            trace_id TEXT,
            log_level TEXT,
            message TEXT,
            lamport_clock INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_service ON logs(service_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trace ON logs(trace_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_level ON logs(log_level)")
    conn.commit()
    return conn


def ensure_consumer_group(r):
    try:
        r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"[worker] created consumer group '{GROUP_NAME}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"[worker] consumer group '{GROUP_NAME}' already exists, joining it")
        else:
            raise


def archive_maintenance_loop(consumer_name, redis_client, conn):
    """
    Runs forever in the background. Every ARCHIVE_CHECK_INTERVAL_SECONDS,
    this worker tries to become the ONE worker allowed to run the
    maintenance sweep this round. Most attempts by most workers will FAIL
    to acquire the lock -- that's expected and correct, not an error.
    """
    while True:
        time.sleep(ARCHIVE_CHECK_INTERVAL_SECONDS)

        # nx=True is the "only if it doesn't exist" mutual exclusion check.
        acquired = redis_client.set(ARCHIVE_LOCK_KEY, consumer_name, nx=True, ex=ARCHIVE_LOCK_TTL_SECONDS)

        if not acquired:
            holder = redis_client.get(ARCHIVE_LOCK_KEY)
            print(f"[{consumer_name}] archive lock held by '{holder}' -- skipping this round")
            continue

        try:
            print(f"[{consumer_name}] ACQUIRED archive lock -- running maintenance sweep exclusively")
            count = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
            print(f"[{consumer_name}] maintenance sweep: {count} total rows currently stored")
            time.sleep(3)  # simulate the sweep taking real time
            print(f"[{consumer_name}] maintenance sweep complete")
        finally:
            # Only release if we STILL hold it (guards against releasing a
            # lock that expired and was already re-acquired by someone else).
            if redis_client.get(ARCHIVE_LOCK_KEY) == consumer_name:
                redis_client.delete(ARCHIVE_LOCK_KEY)
                print(f"[{consumer_name}] released archive lock")


def run_worker(consumer_name="worker-1"):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    conn = init_db()
    ensure_consumer_group(r)

    # Background thread for the mutual-exclusion maintenance task, running
    # independently of the main consume-and-store loop below.
    archive_thread = threading.Thread(
        target=archive_maintenance_loop, args=(consumer_name, r, conn), daemon=True
    )
    archive_thread.start()

    print(f"[{consumer_name}] listening on stream '{STREAM_NAME}' as part of group '{GROUP_NAME}'...")

    while True:
        entries = r.xreadgroup(GROUP_NAME, consumer_name, {STREAM_NAME: ">"}, count=10, block=5000)
        if not entries:
            continue

        for stream_name, records in entries:
            for stream_id, fields in records:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO logs "
                        "(stream_id, timestamp, service_id, trace_id, log_level, message, lamport_clock) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (stream_id, fields["timestamp"], fields["service_id"],
                         fields["trace_id"], fields["log_level"], fields["message"],
                         int(fields.get("lamport_clock", 0))),
                    )
                    conn.commit()
                    r.xack(STREAM_NAME, GROUP_NAME, stream_id)
                    print(f"[{consumer_name}] stored {fields['service_id']} | {fields['log_level']} "
                          f"| lamport={fields.get('lamport_clock', 0)}")
                except Exception as e:
                    print(f"[{consumer_name}] FAILED to store {stream_id}: {e}")


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
    run_worker(name)