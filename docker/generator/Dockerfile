# Event generator: Python + confluent-kafka + Faker. confluent-kafka ships arm64 wheels with librdkafka bundled.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/*

COPY docker/generator/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONPATH=/app
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
