"""
Central Aggregator Server (with Lamport clock forwarding)
-------------------------------------------------------------
Only change from Phase 2: the XADD call now also includes lamport_clock.
Without this, the field would arrive correctly from the agent over gRPC,
but silently get dropped right here before ever reaching Redis -- exactly
the bug we just hit.
"""

import os
import sys
import time
import grpc
from concurrent import futures

sys.path.insert(0, "../proto")
import redis
from logpulse_pb2 import StreamAck
from logpulse_pb2_grpc import LogAggregatorServicer, add_LogAggregatorServicer_to_server

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = 6379
STREAM_NAME = "logpulse:logs"


class AggregatorServicer(LogAggregatorServicer):
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def StreamLogs(self, request_iterator, context):
        count = 0
        for record in request_iterator:
            self.redis_client.xadd(STREAM_NAME, {
                "timestamp": record.timestamp,
                "service_id": record.service_id,
                "trace_id": record.trace_id,
                "log_level": record.log_level,
                "message": record.message,
                "lamport_clock": record.lamport_clock,  # <-- the missing field
            })
            count += 1
            print(f"[aggregator] ingested log from {record.service_id} "
                  f"(trace={record.trace_id}, level={record.log_level}, lamport={record.lamport_clock})")

        return StreamAck(records_received=count, status="OK")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_LogAggregatorServicer_to_server(AggregatorServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[aggregator] LogPulse Aggregator listening on port 50051...")
    print(f"[aggregator] pushing all records into Redis Stream '{STREAM_NAME}'")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()