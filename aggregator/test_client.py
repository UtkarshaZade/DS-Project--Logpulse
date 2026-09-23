"""
Throwaway test -- NOT part of the final project. Just proves the
gRPC -> Aggregator -> Redis Stream pipeline actually works end to end
before we build the real producer agents in Phase 3.
"""
import sys
import grpc

sys.path.insert(0, "../proto")
from logpulse_pb2 import LogRecord
from logpulse_pb2_grpc import LogAggregatorStub


def generate_fake_records():
    # This generator function is what makes it a "client-streaming" call --
    # we yield multiple messages and grpc sends each one down the same
    # open connection as we produce them.
    fake_logs = [
        ("2026-08-22T10:00:00Z", "auth-service", "trace-001", "INFO", "User login successful"),
        ("2026-08-22T10:00:05Z", "inventory-service", "trace-001", "INFO", "Stock check passed"),
        ("2026-08-22T10:00:10Z", "payment-gateway", "trace-001", "ERROR", "Payment timeout"),
    ]
    for ts, sid, tid, lvl, msg in fake_logs:
        yield LogRecord(timestamp=ts, service_id=sid, trace_id=tid, log_level=lvl, message=msg)


def main():
    # This is the client side: open a channel (persistent TCP connection
    # under HTTP/2) to the aggregator, then get a Stub -- the local proxy
    # object generated from our .proto contract that lets us call StreamLogs
    # as if it were a normal Python function.
    channel = grpc.insecure_channel("localhost:50051")
    stub = LogAggregatorStub(channel)

    ack = stub.StreamLogs(generate_fake_records())
    print(f"Server acknowledged: {ack.records_received} records received, status={ack.status}")


if __name__ == "__main__":
    main()