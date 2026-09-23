FROM python:3.11-slim

WORKDIR /app

COPY proto/ /app/proto/
COPY agent/ /app/agent/

RUN pip install --no-cache-dir grpcio grpcio-tools redis protobuf

WORKDIR /app/agent
CMD python producer.py --service ${SERVICE_NAME} --interval 2 --incident-at-tick 5