#!/usr/bin/env bash
# Single-container local Redis + SeeAlpha Engine supervisor.
set -Eeuo pipefail

REDIS_DATA_DIR="${SEEALPHA_REDIS_DATA_DIR:-/app/.local/state/redis}"
mkdir -p "$REDIS_DATA_DIR"

redis_pid=""
engine_pid=""

shutdown() {
  trap - TERM INT
  if [[ -n "$engine_pid" ]]; then
    kill -TERM "$engine_pid" 2>/dev/null || true
  fi
  if [[ -n "$redis_pid" ]]; then
    # Graceful shutdown ensures Redis persists data according to its AOF policy.
    redis-cli -h 127.0.0.1 -p 6379 SHUTDOWN SAVE >/dev/null 2>&1 ||       kill -TERM "$redis_pid" 2>/dev/null || true
  fi
  if [[ -n "$engine_pid" ]]; then wait "$engine_pid" 2>/dev/null || true; fi
  if [[ -n "$redis_pid" ]]; then wait "$redis_pid" 2>/dev/null || true; fi
}
trap shutdown EXIT TERM INT

redis-server \
  --bind 127.0.0.1 \
  --protected-mode yes \
  --port 6379 \
  --dir "$REDIS_DATA_DIR" \
  --appendonly yes \
  --appendfsync everysec \
  --save "900 1" \
  --save "300 10" \
  --save "60 10000" \
  --daemonize no &
redis_pid=$!

ready=0
for _ in {1..40}; do
  if ! kill -0 "$redis_pid" 2>/dev/null; then
    echo "Redis exited before becoming ready" >&2
    exit 1
  fi
  if [[ "$(redis-cli -h 127.0.0.1 -p 6379 PING 2>/dev/null || true)" == PONG ]]; then
    ready=1
    break
  fi
  sleep 0.25
done
if [[ "$ready" != 1 ]]; then
  echo "Redis failed to become ready" >&2
  exit 1
fi

# Local endpoint for later native Nautilus Cache configuration; this environment
# variable alone does not enable Redis persistence inside Nautilus.
export SEEALPHA_REDIS_URL="redis://127.0.0.1:6379"

"$@" &
engine_pid=$!

# If either process stops, shut down the sibling; do not silently operate
# without the cache service. Propagate the first process's exit status.
set +e
wait -n -p finished_pid "$engine_pid" "$redis_pid"
status=$?
set -e
if [[ "${finished_pid:-}" == "$redis_pid" && "$status" -eq 0 ]]; then
  echo "Redis stopped while the Engine was running" >&2
  status=1
fi
exit "$status"
