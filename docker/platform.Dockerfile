FROM python:3.11-slim-bookworm

# Java is required by Spark. procps provides 'ps', used by Spark scripts.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1