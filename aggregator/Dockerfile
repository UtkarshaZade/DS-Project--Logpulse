FROM python:3.11-slim

WORKDIR /app

# Copy the generated proto stubs and the aggregator code
COPY proto/ /app/proto/
COPY aggregator/ /app/aggregator/

RUN pip install --no-cache-dir grpcio grpcio-tools redis protobuf

WORKDIR /app/aggregator
CMD ["python", "server.py"]