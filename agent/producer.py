"""
Producer Agent (with Heartbeat / Beacon Protocol)
-----------------------------------------------------
WHY a separate heartbeat, instead of just watching the normal log stream:
A service could go silent for legitimate reasons (nothing happened worth
logging) or because it CRASHED -- from the outside, silence looks the same
either way. A beacon protocol fixes this by having each service actively
broadcast "I'm still here" on a fixed schedule, independent of whether it
has anything else to report.

HOW we detect failure WITHOUT extra cleanup code:
We write our heartbeat into Redis using SETEX, which sets a key AND an
expiry time in one call. As long as we keep refreshing it faster than it
expires, the key stays present. The moment we crash or freeze, we stop
refreshing it, and Redis AUTOMATICALLY deletes the key once the TTL runs
out -- nobody has to notice we're gone and clean up after us. Checking
"is this service alive" becomes as simple as "does this key exist right
now" -- no timestamps to compare, no manual timeout logic.
"""
import argparse
import os
import random
import sys
import threading
import time
from datetime import datetime, timezone

import grpc
import redis

sys.path.insert(0, "../proto")
from logpulse_pb2 import LogRecord
from logpulse_pb2_grpc import LogAggregatorStub

AGGREGATOR_ADDRESS = f"{os.environ.get('AGGREGATOR_HOST', 'localhost')}:50051"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
INCIDENT_TRACE_ID = "trace-incident-001"
LAMPORT_HANDOFF_KEY = f"lamport:handoff:{INCIDENT_TRACE_ID}"

HEARTBEAT_INTERVAL_SECONDS = 3   # how often we refresh our beacon
HEARTBEAT_TTL_SECONDS = 8        # how long the beacon survives without a refresh
                                   # (deliberately > interval, so normal network jitter doesn't cause false "down")

NORMAL_MESSAGES = {
    "auth-service": [("INFO", "User login successful"), ("INFO", "Token refreshed"), ("DEBUG", "Session validated")],
    "inventory-service": [("INFO", "Stock check passed"), ("INFO", "Item reserved"), ("DEBUG", "Cache hit for SKU lookup")],
    "payment-gateway": [("INFO", "Payment authorized"), ("INFO", "Card validated"), ("DEBUG", "Gateway heartbeat OK")],
    "order-service": [("INFO", "Order created"), ("INFO", "Order confirmed"), ("DEBUG", "Order status updated")],
}

INCIDENT_MESSAGES = {
    "payment-gateway": ("ERROR", "Payment timeout after 30s - upstream bank gateway unresponsive"),
    "order-service": ("ERROR", "Order failed: payment confirmation not received, rolling back"),
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def heartbeat_loop(service_name, redis_client):
    """
    Runs forever in its own background thread, completely independent of
    the log-generating loop below. Even if log-generation stalled, the
    heartbeat thread keeps beaconing on its own schedule.
    """
    key = f"heartbeat:{service_name}"
    while True:
        redis_client.setex(key, HEARTBEAT_TTL_SECONDS, now_iso())
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def generate_logs(service_name, interval_seconds, trigger_incident_after, redis_client):
    tick = 0
    lamport_clock = 0

    while True:
        tick += 1
        is_incident = trigger_incident_after and tick == trigger_incident_after and service_name in INCIDENT_MESSAGES

        if is_incident:
            level, message = INCIDENT_MESSAGES[service_name]
            trace_id = INCIDENT_TRACE_ID

            if service_name == "order-service":
                received = redis_client.get(LAMPORT_HANDOFF_KEY)
                if received is not None:
                    received_clock = int(received)
                    old_clock = lamport_clock
                    lamport_clock = max(lamport_clock, received_clock) + 1
                    print(f"[{service_name}] received causal signal from payment-gateway "
                          f"(their clock={received_clock}, my old clock={old_clock}) "
                          f"-> new clock = max({old_clock}, {received_clock}) + 1 = {lamport_clock}")
                else:
                    lamport_clock += 1
            else:
                lamport_clock += 1
                if service_name == "payment-gateway":
                    redis_client.set(LAMPORT_HANDOFF_KEY, lamport_clock)
        else:
            level, message = random.choice(NORMAL_MESSAGES[service_name])
            trace_id = f"trace-{service_name[:3]}-{tick:04d}"
            lamport_clock += 1

        record = LogRecord(
            timestamp=now_iso(),
            service_id=service_name,
            trace_id=trace_id,
            log_level=level,
            message=message,
            lamport_clock=lamport_clock,
        )
        print(f"[{service_name}] -> {level} | {trace_id} | lamport={lamport_clock} | {message}")
        yield record
        time.sleep(interval_seconds)


def connect_with_retry(address, max_retries=10, delay_seconds=2):
    """
    WHY this matters: docker-compose starting our Aggregator's CONTAINER
    doesn't mean the Aggregator's PYTHON PROCESS has finished starting up
    and is actually listening yet. If an agent tries to connect too early,
    it gets a hard "Connection refused" and would otherwise crash.

    Real distributed systems constantly deal with dependencies not being
    ready yet -- the standard fix is exactly this: retry with a short
    delay instead of giving up immediately.
    """
    channel = grpc.insecure_channel(address)
    stub = LogAggregatorStub(channel)

    for attempt in range(1, max_retries + 1):
        try:
            # grpc.channel_ready_future actively waits for the connection
            # to become usable, with a timeout, instead of just hoping.
            grpc.channel_ready_future(channel).result(timeout=delay_seconds)
            print(f"connected to aggregator at {address} (attempt {attempt})")
            return stub
        except grpc.FutureTimeoutError:
            print(f"aggregator not ready yet, retrying... (attempt {attempt}/{max_retries})")

    raise RuntimeError(f"Could not connect to aggregator at {address} after {max_retries} attempts")


def main():
    parser = argparse.ArgumentParser(description="LogPulse producer agent")
    parser.add_argument("--service", required=True, choices=list(NORMAL_MESSAGES.keys()))
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--incident-at-tick", type=int, default=5)
    args = parser.parse_args()

    stub = connect_with_retry(AGGREGATOR_ADDRESS)
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Background heartbeat thread -- runs independently of the main
    # log-streaming loop, "daemon=True" so it doesn't block program exit.
    hb_thread = threading.Thread(
        target=heartbeat_loop, args=(args.service, redis_client), daemon=True
    )
    hb_thread.start()
    print(f"[{args.service}] heartbeat thread started (every {HEARTBEAT_INTERVAL_SECONDS}s, "
          f"TTL {HEARTBEAT_TTL_SECONDS}s)")

    print(f"[{args.service}] agent starting, streaming to {AGGREGATOR_ADDRESS}...")

    try:
        ack = stub.StreamLogs(generate_logs(args.service, args.interval, args.incident_at_tick, redis_client))
        print(f"[{args.service}] stream closed, server ack: {ack.records_received} records")
    except KeyboardInterrupt:
        print(f"\n[{args.service}] agent stopped by user")


if __name__ == "__main__":
    main()