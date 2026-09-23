# LogPulse

A distributed log aggregation and observability pipeline, built to demonstrate core distributed-systems concepts through a real, working implementation rather than a toy example.

## Problem it solves

In a microservices architecture, a single user request can fail across multiple independent services, and no single service's logs tell the whole story. LogPulse collects structured logs from every service in real time, centralizes them, and lets you reconstruct the full path of any single request across the whole system using a shared `trace_id` — turning scattered, per-service log lines back into one coherent incident story.

## Demo scenario

A simulated e-commerce cluster with 4 independent microservices: `auth-service`, `inventory-service`, `payment-gateway`, `order-service`.

One scripted incident demonstrates root-cause analysis: `payment-gateway` times out, and `order-service` fails as a direct downstream consequence — both tagged with the same `trace_id`, and causally linked via a Lamport clock handoff (see below).

## Architecture

```
[auth-service]  ─┐
[inventory-service] ─┤
[payment-gateway]  ─┼──gRPC (client-streaming)──▶ [Aggregator] ──XADD──▶ [Redis Stream] ◀──XREADGROUP── [Worker-1, Worker-2] ──▶ [SQLite]
[order-service]  ─┘        │                                                    ▲                              │
                            │ heartbeat (TTL beacon)                            │ archive lock (mutual excl.)  │
                            ▼                                                    │                              ▼
                          [Redis]  ◀──────────────────────────────────────────────                    [Dashboard (Flask)] ──▶ browser
```

**Tiers:**
1. **Producer Agents** — simulate each microservice, streaming structured logs over a persistent gRPC connection. Each agent also runs a background **heartbeat thread**, refreshing a TTL'd Redis key every 3 seconds as a liveness beacon.
2. **Central Aggregator** — a gRPC server accepting concurrent streams from all agents, forwarding every record into a Redis Stream.
3. **Consumer Worker Pool** (2 workers) — read from the Redis Stream via a Consumer Group and durably write to SQLite. A background thread on each worker periodically contends for a **Redis-based mutual exclusion lock** to run an exclusive maintenance sweep.
4. **CLI Tools** — `query.py` (search by trace/service/level) and `health.py` (live service status via heartbeats).
5. **Dashboard** — a Flask backend + browser UI showing live health, a filterable log feed, and trace lookup with Lamport-ordered causal reconstruction.

## Tech stack

| Layer | Choice |
|---|---|
| Language | Python |
| RPC / Serialization | gRPC + Protocol Buffers (proto3) |
| Message Broker | Redis Streams (Consumer Groups) |
| Storage | SQLite |
| Dashboard | Flask (REST) + vanilla HTML/JS (polling) |
| Orchestration | Docker & Docker Compose |

## Project structure

```
logpulse/
├── proto/            # .proto contract + generated gRPC stubs
├── aggregator/        # gRPC server -> Redis Stream
├── agent/             # producer agents (heartbeat + Lamport clock + incident logic)
├── worker/            # consumer workers -> SQLite (+ mutual exclusion maintenance task)
├── cli/               # query.py, health.py
├── dashboard/         # Flask backend + static frontend
├── storage/           # SQLite database (created at runtime)
└── docker-compose.yml
```

## Running it

**Requirements:** Docker Desktop installed and running.

From the project root:
```bash
docker-compose up --build
```

This builds and starts: Redis, the Aggregator, 2 workers, 4 producer agents, and the dashboard — 8 containers total. The scripted incident fires automatically a few seconds after startup.

**Dashboard:** open `http://localhost:5000` in a browser.

**CLI tools** (in a separate terminal, with the Python venv activated):
```bash
cd cli
python query.py --trace trace-incident-001
python health.py
```

To stop everything:
```bash
docker-compose down
```

To reset to a clean run:
```bash
docker-compose down
rm storage/logpulse.db      # Windows PowerShell: Remove-Item storage\logpulse.db
docker-compose up --build
```

## FA-1 Syllabus Mapping (Units 1-2)

| Concept | Implementation |
|---|---|
| Distributed Systems Goals (Transparency & Openness) | Standardized `.proto` contract lets any language/service connect without shared code |
| System Architectures | Tiered: Producers → Middleware (Aggregator + Broker) → Consumers/Storage |
| Design Issues (Scalability & Fault Isolation) | Message queue decouples fast producers from slower storage writes; Consumer Groups allow horizontal worker scaling |
| Middleware | Redis Stream as the message-broker layer |
| RPC | `StreamLogs` remote procedure defined via Protocol Buffers |
| Message-Oriented Communication | Asynchronous handoff between Aggregator and Workers via Redis Stream |
| Stream-Oriented Communication | Client-streaming gRPC — continuous log push without per-line reconnection |

## FA-2 Syllabus Mapping (Unit 3: Synchronization)

| Concept | Implementation |
|---|---|
| Logical Clocks / Lamport's Algorithm | Every `LogRecord` carries a `lamport_clock`. Agents increment locally (Rule 1); `order-service` applies the receive rule (Rule 2) via a Redis handoff from `payment-gateway`, proving causal ordering independent of wall-clock time |
| Beacon Protocol | Each agent refreshes a TTL'd Redis key every 3s; absence of the key (auto-expired) means the service is down — no explicit failure announcement needed |
| Mutual Exclusion | Redis `SET key value NX EX ttl` guarantees only one worker at a time runs the periodic archive maintenance task, with automatic lock expiry if a holder crashes |
| Vector Clocks, Election Algorithms, Global State (Chandy-Lamport) | Discussed as natural extensions beyond current scope — see below |

## FA-2 Unit 4 (Emerging Paradigms) — discussion, not full implementation

LogPulse's tiered producer/broker/consumer split mirrors the same shape as production systems like the ELK stack, Datadog, or Kafka-based pipelines. Kubernetes manifests (as an alternative to Docker Compose) would be the natural next step for demonstrating container orchestration at scale; this was scoped out to prioritize the Unit 3 implementations and the dashboard within the available timeline.

## What we'd add with more time

- **Vector clocks**: extending the single Lamport counter to a per-service vector, to distinguish true causality from mere concurrency
- **Leader election** (Bully/Ring algorithm) across multiple Aggregator instances for fault tolerance
- **Chandy-Lamport global snapshots** of the whole pipeline's state
- **PostgreSQL** instead of SQLite — genuinely necessary for reliable multi-writer access (we hit real SQLite corruption running 2 workers across a Windows/Docker boundary, which is direct evidence for this)
- **WebSocket push** instead of dashboard polling
- **Kubernetes manifests** alongside Docker Compose
