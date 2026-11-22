#!/usr/bin/env bash
# Single-container PostgreSQL + SeeAlpha Engine. No exposed database port.
set -Eeuo pipefail

PG_VERSION=15
PG_CLUSTER=seealpha
PG_DATA="${SEEALPHA_POSTGRES_DATA_DIR:-/app/.local/state/postgresql}"
PG_RUNTIME=/run/postgresql
PG_CREDENTIAL_FILE="${SEEALPHA_POSTGRES_CREDENTIAL_FILE:-/app/.local/state/.postgres-password}"
new_database=0

postgres_pid=""
engine_pid=""
observer_pid=""

shutdown() {
  trap - TERM INT EXIT
  if [[ -n "$observer_pid" ]]; then
    kill -TERM "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
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
  new_database=1
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
  if [[ "$new_database" != 1 ]]; then
    echo "Existing PostgreSQL cluster has no credential file; refusing to rotate credentials" >&2
    exit 1
  fi
  od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]' > "$PG_CREDENTIAL_FILE"
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
  runuser -u postgres -- psql -h "$PG_RUNTIME" -d postgres -v ON_ERROR_STOP=1 -q <<SQL
CREATE ROLE seealpha_cache LOGIN PASSWORD '$PG_PASSWORD';
SQL
fi
if ! runuser -u postgres -- psql -h "$PG_RUNTIME" -d postgres -Atqc \
  "SELECT 1 FROM pg_database WHERE datname='seealpha_cache'" | grep -qx 1; then
  runuser -u postgres -- createdb -h "$PG_RUNTIME" -O seealpha_cache seealpha_cache
fi

# Initialize the exact pinned NautilusTrader rc9 schema once, as the database
# owner. The native Postgres cache refuses missing/incompatible schemas.
SCHEMA_DIR=/usr/local/share/nautilus-postgres-schema
schema_ready="$(runuser -u postgres -- psql -h "$PG_RUNTIME" -d seealpha_cache -Atqc \
  "SELECT CASE WHEN to_regclass('public.account_event') IS NOT NULL AND to_regclass('public.order_event') IS NOT NULL AND to_regclass('public.position_event') IS NOT NULL THEN 'yes' ELSE 'no' END")"
if [[ "$schema_ready" != "yes" ]]; then
  # Existing partial schema should never be overwritten silently.
  object_count="$(runuser -u postgres -- psql -h "$PG_RUNTIME" -d seealpha_cache -Atqc \
    "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m')")"
  if [[ "$object_count" != "0" ]]; then
    echo "Incomplete Nautilus PostgreSQL schema; refusing automatic migration" >&2
    exit 1
  fi
  for sql in types.sql functions.sql partitions.sql tables.sql; do
    runuser -u postgres -- psql -X -h "$PG_RUNTIME" -U postgres -d seealpha_cache \
      -v ON_ERROR_STOP=1 -f "$SCHEMA_DIR/$sql" >/dev/null
  done
  # Schema tables created by postgres need permissions for the account cache.
  runuser -u postgres -- psql -X -h "$PG_RUNTIME" -d seealpha_cache \
    -v ON_ERROR_STOP=1 -q <<SQL
GRANT USAGE, CREATE ON SCHEMA public TO seealpha_cache;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO seealpha_cache;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO seealpha_cache;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO seealpha_cache;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO seealpha_cache;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO seealpha_cache;
SQL
fi

# The observer reads the credential from this file. No password command-line flag.
export SEEALPHA_POSTGRES_CACHE_PASSWORD_FILE="$PG_CREDENTIAL_FILE"
# Local endpoint hint only; actual native Cache is enabled explicitly on observer.

unset PG_PASSWORD

# Explicit testnet-only observer: no strategies, no live orders and no
# automatic production account connections.
if [[ "${SEEALPHA_NATIVE_ACCOUNT_OBSERVER:-0}" == "1" ]]; then
  if [[ "${SEEALPHA_ENABLE_POSTGRES_CACHE:-0}" != "1" ]]; then
    echo "Native account observer requires native Postgres Cache enabled" >&2
    exit 1
  fi
  case "${SEEALPHA_ACCOUNT_PRODUCT:-}" in spot|futures_um) ;; *)
    echo "SEEALPHA_ACCOUNT_PRODUCT must be spot or futures_um" >&2; exit 1 ;;
  esac
  if [[ ! "${SEEALPHA_ACCOUNT_ID:-}" =~ ^[A-Za-z0-9_-]{1,128}$ ]]; then
    echo "Missing or invalid SEEALPHA_ACCOUNT_ID" >&2
    exit 1
  fi
  observer_dir=/app/.local/state/nautilus-account
  mkdir -p "$observer_dir"
  native_account_observer "$SEEALPHA_ACCOUNT_PRODUCT" "$SEEALPHA_ACCOUNT_ID" "$observer_dir" &
  observer_pid=$!
elif [[ "${SEEALPHA_NATIVE_ACCOUNT_OBSERVER:-0}" != "0" ]]; then
  echo "SEEALPHA_NATIVE_ACCOUNT_OBSERVER must be 0 or 1" >&2
  exit 1
fi

"$@" &
engine_pid=$!
set +e
if [[ -n "$observer_pid" ]]; then
  wait -n -p finished_pid "$engine_pid" "$postgres_pid" "$observer_pid"
else
  wait -n -p finished_pid "$engine_pid" "$postgres_pid"
fi
status=$?
set -e
if [[ "${finished_pid:-}" != "$engine_pid" && "$status" -eq 0 ]]; then
  echo "PostgreSQL or account observer unexpectedly stopped" >&2
  status=1
fi
exit "$status"
