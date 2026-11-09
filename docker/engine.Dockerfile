# Build from the repository root: docker build -f docker/engine.Dockerfile -t seealpha-engine:latest .
FROM rust:1.91-slim AS converter
WORKDIR /build
COPY engine/pipeline/rust_converter/ ./rust_converter/
RUN cargo build --locked --release --manifest-path rust_converter/Cargo.toml

FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN python -m pip install --no-cache-dir uv==0.10.0
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-default-groups --no-install-project
# Only Engine-owned code and the shared market/HTTP contracts are shipped.
# No API, database migration, web frontend or admin source in the image.
COPY core/ ./core/
COPY engine/ ./engine/
COPY nautilus_support.py ./
COPY packages/ ./packages/
COPY --from=converter /build/rust_converter/target/release/nautilus-data-converter /app/engine/pipeline/rust_converter/target/release/nautilus-data-converter
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8766
CMD ["python", "-m", "engine"]
