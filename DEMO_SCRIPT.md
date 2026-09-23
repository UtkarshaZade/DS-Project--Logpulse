# LogPulse — Live Demo Script (FA-1 + FA-2)

Goal: in ~8-10 minutes, show a real distributed system running, prove root-cause analysis via Lamport-ordered tracing, and demonstrate beacon-based failure detection and mutual exclusion live.

**Before you walk in:**
- `docker-compose down`, delete `storage/logpulse.db`, keep Docker Desktop running
- Have 2-3 windows ready: one for `docker-compose up --build`, one for the dashboard browser tab, one for CLI commands
- Know that the incident fires ~10 seconds after the agents start

---

## 1. Set up the story (30 sec)
> "LogPulse is a distributed log aggregation pipeline for a 4-microservice e-commerce system. The core problem: when a request fails across multiple services, no single service's logs tell the whole story. We centralize everything and reconstruct any request's path using a shared trace ID — and this version adds synchronization concepts: logical clocks, a failure-detection beacon, and distributed mutual exclusion."

## 2. Launch the system (1 min)
```bash
docker-compose up --build
```
> "This single command brings up 8 containers: Redis, the gRPC Aggregator, 2 independent storage workers, 4 producer agents, and a web dashboard — a real tiered, event-driven distributed system."

## 3. Open the dashboard (1-2 min)
Open `http://localhost:5000`.
> "All 4 services show green — that's a live beacon protocol. Each agent refreshes a heartbeat key in Redis every 3 seconds with a short expiry. If a service goes silent, Redis deletes the key automatically — nobody has to notice or announce the failure."

Point at the live log feed scrolling below.

## 4. Show the incident + Lamport ordering (1-2 min)
Wait for the incident (~10-15 sec in), then type `trace-incident-001` into the trace box.
> "Here's the causal reconstruction. Payment times out, then order fails right after — but notice the ordering isn't based on wall-clock time. It's based on this Lamport clock column. Order-service received payment-gateway's exact clock value through Redis and computed max(mine, theirs)+1 — that jump is highlighted right here. Even if these two machines' clocks were out of sync, this ordering would still be provably correct."

## 5. Prove the beacon protocol live (1-2 min)
In a terminal:
```bash
docker stop logpulse-agent-payment
```
Point back at the dashboard — within ~8 seconds, `payment-gateway`'s card flips to red, with zero code change or manual detection logic.
> "That flip is automatic. Redis simply expired the key. This is the beacon protocol from the syllabus, working live."

Restart it:
```bash
docker-compose up -d agent-payment
```

## 6. Prove mutual exclusion live (1-2 min)
Point at the terminal logs for `worker` and `worker2` (or `docker logs -f logpulse-worker` / `logpulse-worker2` in two windows).
> "Both workers periodically try to acquire a maintenance lock in Redis. Watch — only one gets 'ACQUIRED' at a time; the other logs 'lock held by X, skipping.' This alternates over time but they're never both running it simultaneously. That's real distributed mutual exclusion, using Redis's atomic SET-if-not-exists."

## 7. CLI tools, briefly (30 sec)
```bash
cd cli
python health.py
```
> "Same beacon data, from the command line — useful for scripting or automation outside the browser."

## 8. Close (30 sec)
> "So altogether: RPC via gRPC, stream-oriented communication, message-oriented communication through Redis, a tiered middleware architecture, full Docker Compose orchestration — plus, for this round, logical clocks with Lamport's algorithm, a beacon protocol for failure detection, and Redis-based distributed mutual exclusion. All genuinely demonstrated live, not just described."

## If something breaks live
- **A container won't start / port conflict:** `docker-compose down` then `docker-compose up --build` again.
- **Incident not showing yet:** wait a few more seconds — it fires on a fixed tick, it will appear.
- **Dashboard shows stale/no data:** hard-refresh the browser tab (Ctrl+Shift+R); polling refreshes every 3s regardless.
- **Evaluator asks about vector clocks / election / Postgres:** these are the documented "what we'd add with more time" items in the README — say so directly, and mention the real SQLite corruption you hit as concrete evidence for why Postgres would be the production choice.
