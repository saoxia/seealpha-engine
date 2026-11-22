# Build from the repository root:
#   docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY docker/seealpha-postgres-schema.sql ./docker/seealpha-postgres-schema.sql
COPY engine ./engine
COPY benchmarks ./benchmarks
RUN cargo build --locked --release -p seealpha-engine -p seealpha-binance-market-data-source --bins
# The Nautilus standalone crate is intentionally excluded from the engine workspace.
# Build its native account observer explicitly; never launch it by default.
RUN cargo build --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
    --release --bin native_account_observer

# Default deployment: a Rust runtime without Python interpreter, host, or strategy packages.
FROM debian:bookworm-slim
RUN mkdir -p /etc/postgresql-common \
    && printf 'create_main_cluster = false\\n' > /etc/postgresql-common/createcluster.conf \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 postgresql-15 postgresql-client-15 \
    && rm -rf /var/lib/apt/lists/*
ENV ENGINE_PROJECT=/app \
    ENGINE_HOST=127.0.0.1 ENGINE_PORT=8766 \
    ENGINE_STATE=/app/.local/state/engine \
    SEEALPHA_POSTGRES_DATA_DIR=/app/.local/state/postgresql
WORKDIR /app
COPY --from=build /src/target/release/seealpha-engine /usr/local/bin/seealpha-engine
COPY --from=build /src/target/release/binance-trade-archive-fetch /usr/local/bin/binance-trade-archive-fetch
COPY --from=build /src/engine/pipeline/sources/shared/nautilus/target/release/native_account_observer /usr/local/bin/native_account_observer
COPY docker/nautilus-postgres-schema /usr/local/share/nautilus-postgres-schema
# PostgreSQL listens only on container-local loopback (not EXPOSEd).
# The persistent state volume holds PostgreSQL and immutable strategy artifacts.
COPY docker/entrypoint-with-postgres.sh /usr/local/bin/entrypoint-with-postgres.sh
RUN chmod 0755 /usr/local/bin/entrypoint-with-postgres.sh
# Intentionally no Python interpreter or Python strategy files.
EXPOSE 8766
ENTRYPOINT ["/usr/local/bin/entrypoint-with-postgres.sh"]
CMD ["seealpha-engine"]
