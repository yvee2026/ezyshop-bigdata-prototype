# Spark 3.5 + Delta 3.2 processing image (arm64 and amd64).
# bookworm, not trixie: openjdk-17 is not packaged in Debian trixie, and Spark 3.5 supports Java 8/11/17.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/*

# Resolve JAVA_HOME independent of CPU architecture (arm64 vs amd64 paths differ).
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Pre-download Spark packages at build time so runs do not depend on Maven Central.
COPY docker/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py \
 && mv /opt/ivy/jars /opt/jars \
 && rm -rf /opt/ivy /tmp/fetch_jars.py \
 && ls /opt/jars

# VADER lexicon ships inside the wheel; nothing else to download.
WORKDIR /app
ENV PYTHONPATH=/app \
    SPARK_JARS_DIR=/opt/jars

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
