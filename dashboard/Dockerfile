FROM python:3.11-slim

WORKDIR /app

COPY dashboard/ /app/dashboard/

RUN pip install --no-cache-dir flask redis

WORKDIR /app/dashboard
CMD ["python", "app.py"]