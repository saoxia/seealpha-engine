# Optional Python strategy host layer. Build the native Engine image first:
# docker build -f docker/engine.Dockerfile -t seealpha-engine:native .
# docker build -f docker/engine-python.Dockerfile -t seealpha-engine:python .
# This image is not the default deployment. The Rust Engine remains the only
# long-lived server; Python is spawned as an isolated, bounded strategy child.
ARG ENGINE_BASE_IMAGE=seealpha-engine:native
FROM ${ENGINE_BASE_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
ENV ENGINE_PYTHON=/usr/bin/python3 \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY python/strategy_host /app/python/strategy_host
COPY packages /app/packages
