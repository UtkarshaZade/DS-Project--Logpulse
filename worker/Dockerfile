FROM python:3.11-slim

WORKDIR /app

COPY proto/ /app/proto/
COPY worker/ /app/worker/

RUN pip install --no-cache-dir redis

WORKDIR /app/worker
CMD ["python", "consumer.py", "worker-1"]