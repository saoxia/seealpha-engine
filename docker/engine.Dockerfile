# Build from the repository root:
#   docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
FROM rust:1.100-alpine3.22 AS build
# musl-native compilation; build tools never enter the runtime image.
RUN apk add --no-cache build-base pkgconf openssl-dev binutils cmake perl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY docker/seealpha-postgres-schema.sql ./docker/seealpha-postgres-schema.sql
COPY engine ./engine
COPY benchmarks ./benchmarks
RUN cargo build --locked --release -p seealpha-engine -p seealpha-binance-market-data-source -p seealpha-market --bins
# The Nautilus standalone crate is intentionally excluded from the engine workspace.
# Build its native account observer explicitly; never launch it by default.
RUN cargo build --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml \
    --release --bin native_account_observer --bin seealpha-nautilus-binance-data

# Separate debug information from runtime executables; preserve GNU debuglinks.
RUN mkdir -p /src/debug-symbols && \
    for bin in /src/target/release/seealpha-engine \
               /src/target/release/binance-trade-archive-fetch \
               /src/target/release/pipeline_worker \
               /src/engine/pipeline/sources/shared/nautilus/target/release/native_account_observer \
               /src/engine/pipeline/sources/shared/nautilus/target/release/seealpha-nautilus-binance-data; do \
        name="$(basename "$bin")"; \
        objcopy --only-keep-debug "$bin" "/src/debug-symbols/$name.debug" && \
        strip --strip-all "$bin" && \
        objcopy --add-gnu-debuglink="/src/debug-symbols/$name.debug" "$bin"; \
    done

# Export independently; not present in the default runtime image.
FROM scratch AS debug-symbols
COPY --from=build /src/debug-symbols/ /symbols/

# Default deployment: a Rust runtime without Python interpreter, host, or strategy packages.
FROM alpine:3.22 AS runtime
# Retain PostgreSQL major version 15 and the single-container deployment.
# Alpine PostgreSQL packages use /usr/libexec/postgresql15 instead of Debian's /usr/lib/postgresql/15/bin.
RUN apk add --no-cache ca-certificates openssl bash su-exec postgresql15 postgresql15-client
ENV PATH="/usr/libexec/postgresql15:${PATH}"
ENV ENGINE_PROJECT=/app \
    ENGINE_HOST=127.0.0.1 ENGINE_PORT=8766 \
    ENGINE_STATE=/app/.local/state/engine \
    SEEALPHA_POSTGRES_DATA_DIR=/app/.local/state/postgresql
WORKDIR /app
COPY --from=build /src/target/release/seealpha-engine /usr/local/bin/seealpha-engine
COPY --from=build /src/target/release/binance-trade-archive-fetch /usr/local/bin/binance-trade-archive-fetch
COPY --from=build /src/target/release/pipeline_worker /usr/local/bin/seealpha-pipeline
COPY --from=build /src/engine/pipeline/sources/shared/nautilus/target/release/native_account_observer /usr/local/bin/native_account_observer
COPY --from=build /src/engine/pipeline/sources/shared/nautilus/target/release/seealpha-nautilus-binance-data /usr/local/bin/seealpha-nautilus-binance-data
COPY docker/nautilus-postgres-schema /usr/local/share/nautilus-postgres-schema
# PostgreSQL listens only on container-local loopback (not EXPOSEd).
# The persistent state volume holds PostgreSQL and other active application state.
COPY docker/entrypoint-with-postgres.sh /usr/local/bin/entrypoint-with-postgres.sh
RUN chmod 0755 /usr/local/bin/entrypoint-with-postgres.sh
# Intentionally no Python interpreter or Python strategy files.
EXPOSE 8766
ENTRYPOINT ["/usr/local/bin/entrypoint-with-postgres.sh"]
CMD ["seealpha-engine"]
