"""
Dashboard Backend
--------------------
Exposes three read-only JSON endpoints. This server doesn't touch the
Redis Stream, doesn't write to SQLite, and doesn't know anything about
gRPC -- it's a thin, separate consumer of data other parts of the system
already produced. That separation matters: the dashboard can crash or be
restarted at any time without affecting log collection, storage, or the
mutual-exclusion maintenance task at all.

WHY POLLING instead of a WebSocket push (an honest trade-off, not a
shortcut we're hiding): a WebSocket needs its own persistent connection
management, reconnect logic, and a background thread pushing updates --
more moving parts, more ways to fail under time pressure. Polling every
couple of seconds from the frontend is simpler, and for a log dashboard
refreshed every 2-3 seconds, the user experience difference is minimal.
"""
import os
import sqlite3

import redis
from flask import Flask, jsonify, request, send_from_directory

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
DB_PATH = os.environ.get("DB_PATH", "../storage/logpulse.db")

KNOWN_SERVICES = ["auth-service", "inventory-service", "payment-gateway", "order-service"]

app = Flask(__name__, static_folder="static", static_url_path="")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # lets us convert rows to dicts easily
    return conn


@app.route("/api/health")
def api_health():
    """Same idea as cli/health.py from Phase B, just returned as JSON."""
    result = {}
    for service in KNOWN_SERVICES:
        last_seen = redis_client.get(f"heartbeat:{service}")
        result[service] = {
            "status": "up" if last_seen is not None else "down",
            "last_seen": last_seen,
        }
    return jsonify(result)


@app.route("/api/logs")
def api_logs():
    """Recent logs, optionally filtered by level or service_id."""
    level = request.args.get("level")
    service = request.args.get("service")
    limit = min(int(request.args.get("limit", 50)), 200)  # cap to avoid huge responses

    conn = get_db()
    query = "SELECT * FROM logs WHERE 1=1"
    params = []
    if level:
        query += " AND log_level = ?"
        params.append(level)
    if service:
        query += " AND service_id = ?"
        params.append(service)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


@app.route("/api/trace/<trace_id>")
def api_trace(trace_id):
    """Same idea as cli/query.py --trace, ordered by lamport_clock."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM logs WHERE trace_id = ? ORDER BY lamport_clock ASC",
        (trace_id,),
    ).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


@app.route("/api/lock-status")
def api_lock_status():
    """
    Exposes the SAME lock the two workers fight over in Phase C, so the
    mutual-exclusion concept -- previously only visible in raw terminal
    logs -- becomes something anyone can watch happen in the browser.
    """
    holder = redis_client.get("lock:archive_task")
    ttl = redis_client.ttl("lock:archive_task") if holder else None
    return jsonify({"holder": holder, "ttl_remaining": ttl})


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)