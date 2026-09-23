"""
Health Check CLI (Beacon consumer)
-------------------------------------
This tool doesn't touch SQLite at all -- unlike query.py, which reads
PAST/historical data, this reads Redis directly for CURRENT, live status.
That distinction matters: SQLite tells you what happened; Redis heartbeats
tell you what's happening RIGHT NOW.

Checking "is this service alive" is just: does its heartbeat key exist?
No timestamp math, no manual timeout comparison -- Redis already deleted
the key if the service went silent for longer than its TTL.
"""
import os
import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379

KNOWN_SERVICES = ["auth-service", "inventory-service", "payment-gateway", "order-service"]


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    print("LogPulse Service Health (live, via heartbeat beacon)\n")
    for service in KNOWN_SERVICES:
        key = f"heartbeat:{service}"
        last_seen = r.get(key)
        if last_seen is not None:
            ttl = r.ttl(key)
            print(f"  [UP]   {service:<20} last beacon: {last_seen}  (expires in {ttl}s if not refreshed)")
        else:
            print(f"  [DOWN] {service:<20} no active heartbeat -- service is not responding")
    print()


if __name__ == "__main__":
    main()