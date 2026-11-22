#!/usr/bin/env bash
# Single-container PostgreSQL + SeeAlpha Engine. No exposed database port.
set -Eeuo pipefail

PG_VERSION=15
PG_CLUSTER=seealpha
PG_DATA="${SEEALPHA_POSTGRES_DATA_DIR:-/app/.local/state/postgresql}"
PG_RUNTIME=/run/postgresql
PG_CREDENTIAL_FILE="${SEEALPHA_POSTGRES_CREDENTIAL_FILE:-/app/.local/state/.postgres-password}"

postgres_pid=""
engine_pid=""

shutdown() {
  trap - TERM INT EXIT
  if [[ -n "$engine_pid" ]]; then
    kill -TERM "$engine_pid" 2>/dev/null || true
    wait "$engine_pid" 2>/dev/null || true
  fi
  if [[ -n "$postgres_pid" ]]; then
    runuser -u postgres -- /usr/lib/postgresql/"$PG_VERSION"/bin/pg_ctl \
      -D "$PG_DATA" -m fast -w stop >/dev/null 2>&1 || true
    wait "$postgres_pid" 2>/dev/null || true
  fi
}
trap shutdown EXIT TERM INT

install -d -o postgres -g postgres -m 0700 "$PG_DATA"
install -d -o postgres -g postgres -m 2775 "$PG_RUNTIME"
if [[ ! -f "$PG_DATA/PG_VERSION" ]]; then
  if [[ -n "$(find "$PG_DATA" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "PostgreSQL directory has files but is not initialized; refusing to overwrite" >&2
    exit 1
  fi
  runuser -u postgres -- /usr/lib/postgresql/"$PG_VERSION"/bin/initdb \
    -D "$PG_DATA" --auth-local=peer --auth-host=scram-sha-256 --encoding=UTF8 \
    --no-instructions >/dev/null
fi
if [[ "$(cat "$PG_DATA/PG_VERSION")" != "$PG_VERSION" ]]; then
  echo "PostgreSQL data version mismatch; upgrade must be explicit" >&2
  exit 1
fi

# First boot credentials are generated once and retained with the state volume.
# Do not print the password or expose the database on a network interface.
if [[ ! -f "$PG_CREDENTIAL_FILE" ]]; then
  umask 077
  python3 -c 'import secrets,sys; sys.stdout.write(secrets.token_urlsafe(36))' > "$PG_CREDENTIAL_FILE"
fi
chmod 0600 "$PG_CREDENTIAL_FILE"
PG_PASSWORD="$(cat "$PG_CREDENTIAL_FILE")"
if [[ -z "$PG_PASSWORD" ]]; then
  echo "Empty PostgreSQL credential" >&2
  exit 1
fi

# pg_ctl logs to container stderr through inherited descriptors.
runuser -u postgres -- /usr/lib/postgresql/"$PG_VERSION"/bin/postgres \
  -D "$PG_DATA" -c listen_addresses=127.0.0.1 -c port=5432 \
  -c unix_socket_directories="$PG_RUNTIME" &
postgres_pid=$!

ready=0
for _ in {1..60}; do
  if ! kill -0 "$postgres_pid" 2>/dev/null; then
    echo "PostgreSQL exited during startup" >&2
    exit 1
  fi
  if runuser -u postgres -- pg_isready -q -h "$PG_RUNTIME" -p 5432; then
    ready=1
    break
  fi
  sleep 0.25
done
if [[ "$ready" != 1 ]]; then
  echo "PostgreSQL did not become ready" >&2
  exit 1
fi

# Create a dedicated database and a non-superuser login on first startup.
if ! runuser -u postgres -- psql -h "$PG_RUNTIME" -d postgres -Atqc \
  "SELECT 1 FROM pg_roles WHERE rolname='seealpha_cache'" | grep -qx 1; then
  PG_BOOTSTRAP_PASSWORD="$PG_PASSWORD" runuser -u postgres -- env \
    PG_BOOTSTRAP_PASSWORD="$PG_PASSWORD" psql -h "$PG_RUNTIME" -d postgres \
    -v ON_ERROR_STOP=1 -q -c \
    "CREATE ROLE seealpha_cache LOGIN PASSWORD '$(printf '%s' "$PG_PASSWORD" | sed "s/'/''/g")'"
fi
if ! runuser -u postgres -- psql -h "$PG_RUNTIME" -d postgres -Atqc \
  "SELECT 1 FROM pg_database WHERE datname='seealpha_cache'" | grep -qx 1; then
  runuser -u postgres -- createdb -h "$PG_RUNTIME" -O seealpha_cache seealpha_cache
fi

# Loopback-only DSN for a future Nautilus native Cache setup (not yet wired).
export SEEALPHA_POSTGRES_URL="postgresql://seealpha_cache:${PG_PASSWORD}@127.0.0.1:5432/seealpha_cache"
unset PG_PASSWORD

"$@" &
engine_pid=$!
set +e
wait -n -p finished_pid "$engine_pid" "$postgres_pid"
status=$?
set -e
if [[ "${finished_pid:-}" == "$postgres_pid" && "$status" -eq 0 ]]; then
  echo "PostgreSQL unexpectedly stopped" >&2
  status=1
fi
exit "$status"
