# Build from the repository root:
#   docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
FROM rust:1.100-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --locked --release -p seealpha-engine -p seealpha-binance-market-data-source --bins

FROM python:3.15-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    ENGINE_PROJECT=/app ENGINE_PYTHON=python \
    ENGINE_HOST=127.0.0.1 ENGINE_PORT=8766 \
    ENGINE_STATE=/app/.local/state/engine
WORKDIR /app
COPY --from=build /src/target/release/seealpha-engine /usr/local/bin/seealpha-engine
COPY --from=build /src/target/release/binance-native-source /usr/local/bin/binance-native-source
COPY --from=build /src/target/release/binance-native-calculation /usr/local/bin/binance-native-calculation
COPY --from=build /src/target/release/binance-native-catalog-publish /usr/local/bin/binance-native-catalog-publish
COPY python/strategy_host ./python/strategy_host
COPY packages ./packages
COPY core/engine_contracts.py core/trading_contracts.py ./core/
EXPOSE 8766
CMD ["seealpha-engine"]
