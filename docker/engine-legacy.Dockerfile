# Opt-in legacy Python Engine, preserved until Rust feature parity.
# Build separately: docker build -f docker/engine-legacy.Dockerfile -t seealpha-engine:legacy .
# NEVER mount the same ENGINE_STATE / writable Catalog into this and the Rust Engine.
FROM rust:1.100-slim AS converter
WORKDIR /build
COPY engine/market_data/repair/rust_converter/ ./rust_converter/
RUN cargo build --locked --release --manifest-path rust_converter/Cargo.toml

FROM python:3.15-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    ENGINE_HOST=0.0.0.0 ENGINE_PORT=8766 \
    ENGINE_AUTH_MODE=token \
    ENGINE_STATE=/app/.local/state/legacy-engine
WORKDIR /app
RUN python -m pip install --no-cache-dir uv==0.13.9
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-default-groups --no-install-project
COPY core/ ./core/
COPY engine/ ./engine/
COPY nautilus_support.py ./
COPY packages/ ./packages/
COPY --from=converter /build/rust_converter/target/release/nautilus-data-converter /app/engine/market_data/repair/rust_converter/target/release/nautilus-data-converter
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8766
CMD ["python", "-m", "engine"]
