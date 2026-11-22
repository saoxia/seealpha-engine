#!/usr/bin/env bash
# Single-container PostgreSQL + SeeAlpha Engine. No exposed database port.
set -Eeuo pipefail

PG_VERSION=15
PG_BIN_DIR=/usr/libexec/postgresql15
PG_CLUSTER=seealpha
PG_DATA="${SEEALPHA_POSTGRES_DATA_DIR:-/app/.local/state/postgresql}"
PG_RUNTIME=/run/postgresql
PG_CREDENTIAL_FILE="${SEEALPHA_POSTGRES_CREDENTIAL_FILE:-/app/.local/state/.postgres-password}"
# There is no SQLite importer. Refuse any old SQLite files rather than
# silently starting with fresh state when order evidence might exist.
legacy_state_dir="${ENGINE_STATE:-/app/.local/state/engine}"
if [[ -f "$legacy_state_dir/book.sqlite" || -f "$legacy_state_dir/control.sqlite" ]]; then
  echo "Legacy SQLite state is unsupported; refusing to replace potential order evidence." >&2
  exit 1
fi
new_database=0

postgres_pid=""
engine_pid=""
observer_pid=""
pipeline_spot_pid=""
pipeline_um_pid=""
pipeline_options_pid=""

shutdown() {
  trap - TERM INT EXIT
  for worker in "$pipeline_spot_pid" "$pipeline_um_pid" "$pipeline_options_pid"; do
    if [[ -n "$worker" ]]; then
      kill -TERM "$worker" 2>/dev/null || true
      wait "$worker" 2>/dev/null || true
    fi
  done
  if [[ -n "$observer_pid" ]]; then
    kill -TERM "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  if [[ -n "$engine_pid" ]]; then
    kill -TERM "$engine_pid" 2>/dev/null || true
    wait "$engine_pid" 2>/dev/null || true
  fi
  if [[ -n "$postgres_pid" ]]; then
    su-exec postgres "$PG_BIN_DIR/pg_ctl" \
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
  su-exec postgres "$PG_BIN_DIR/initdb" \
    -D "$PG_DATA" --auth-local=peer --auth-host=scram-sha-256 --encoding=UTF8 \
    --no-instructions >/dev/null
fi
if [[ "$(cat "$PG_DATA/PG_VERSION")" != "$PG_VERSION" ]]; then
  echo "PostgreSQL data version mismatch; upgrade must be explicit" >&2
  exit 1
fi
# Refuse direct reuse of an unverified glibc-backed PostgreSQL data volume.
# A matching major version alone does not prove locale/index compatibility.
PG_PLATFORM_MARKER="$PG_DATA/.seealpha-alpine-musl"
if [[ "$new_database" == 1 ]]; then
  su-exec postgres touch "$PG_PLATFORM_MARKER"
elif [[ ! -f "$PG_PLATFORM_MARKER" ]]; then
  echo "Existing PostgreSQL cluster lacks Alpine/musl provenance; back up via the original Debian image and restore into a fresh Alpine volume." >&2
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
su-exec postgres /usr/bin/postgres15 \
  -D "$PG_DATA" -c listen_addresses=127.0.0.1 -c port=5432 \
  -c unix_socket_directories="$PG_RUNTIME" \
  -c shared_buffers=16MB -c max_connections=30 \
  -c work_mem=1MB -c maintenance_work_mem=16MB \
  -c autovacuum_work_mem=8MB &
postgres_pid=$!

ready=0
for _ in {1..60}; do
  if ! kill -0 "$postgres_pid" 2>/dev/null; then
    echo "PostgreSQL exited during startup" >&2
    exit 1
  fi
  if su-exec postgres pg_isready -q -h "$PG_RUNTIME" -p 5432; then
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
if ! su-exec postgres psql -h "$PG_RUNTIME" -d postgres -Atqc \
  "SELECT 1 FROM pg_roles WHERE rolname='seealpha_cache'" | grep -qx 1; then
  su-exec postgres psql -h "$PG_RUNTIME" -d postgres -v ON_ERROR_STOP=1 -q <<SQL
CREATE ROLE seealpha_cache LOGIN PASSWORD '$PG_PASSWORD';
SQL
fi
if ! su-exec postgres psql -h "$PG_RUNTIME" -d postgres -Atqc \
  "SELECT 1 FROM pg_database WHERE datname='seealpha_cache'" | grep -qx 1; then
  su-exec postgres createdb -h "$PG_RUNTIME" -O seealpha_cache seealpha_cache
fi

# Initialize the exact pinned NautilusTrader rc9 schema once, as the database
# owner. The native Postgres cache refuses missing/incompatible schemas.
SCHEMA_DIR=/usr/local/share/nautilus-postgres-schema
schema_ready="$(su-exec postgres psql -h "$PG_RUNTIME" -d seealpha_cache -Atqc \
  "SELECT CASE WHEN to_regclass('public.account_event') IS NOT NULL AND to_regclass('public.order_event') IS NOT NULL AND to_regclass('public.position_event') IS NOT NULL THEN 'yes' ELSE 'no' END")"
if [[ "$schema_ready" != "yes" ]]; then
  # Existing partial schema should never be overwritten silently.
  object_count="$(su-exec postgres psql -h "$PG_RUNTIME" -d seealpha_cache -Atqc \
    "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m')")"
  if [[ "$object_count" != "0" ]]; then
    echo "Incomplete Nautilus PostgreSQL schema; refusing automatic migration" >&2
    exit 1
  fi
  for sql in types.sql functions.sql partitions.sql tables.sql; do
    su-exec postgres psql -X -h "$PG_RUNTIME" -U postgres -d seealpha_cache \
      -v ON_ERROR_STOP=1 -f "$SCHEMA_DIR/$sql" >/dev/null
  done
  # Schema tables created by postgres need permissions for the account cache.
  su-exec postgres psql -X -h "$PG_RUNTIME" -d seealpha_cache \
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
# The Engine uses the existing local PostgreSQL instance for SeeAlpha-owned state.
# Do not print the connection string: it includes the persistent database password.
export SEEALPHA_DATABASE_URL="postgresql://seealpha_cache:${PG_PASSWORD}@127.0.0.1:5432/seealpha_cache"
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

# Product-local autonomous market data loops (no Engine data-job dispatcher).
# They are inactive until a tenant defines a subscription via the config API.
if [[ "${SEEALPHA_PIPELINE_ENABLED:-1}" == "1" ]]; then
  # Pipeline writes into the same Catalog root that Runtime reads.
  catalog_root="${ENGINE_CATALOG_DIR:-/app/data/catalog}"
  if [[ "$catalog_root" != /* ]]; then catalog_root="/app/$catalog_root"; fi
  mkdir -p "$catalog_root"
  if [[ -L "$catalog_root" ]]; then
    echo "Pipeline Catalog root cannot be a symlink" >&2
    exit 1
  fi
  # Ensure a mounted/persistent Catalog root is provisioned by the operator.
  # Missing Catalog only causes a retry; the process never reports READY.
  seealpha-pipeline spot &
  pipeline_spot_pid=$!
  seealpha-pipeline futures_um &
  pipeline_um_pid=$!
  seealpha-pipeline options &
  pipeline_options_pid=$!
elif [[ "${SEEALPHA_PIPELINE_ENABLED:-1}" != "0" ]]; then
  echo "SEEALPHA_PIPELINE_ENABLED must be 0 or 1" >&2
  exit 1
fi

"$@" &
engine_pid=$!
set +e
processes=("$engine_pid" "$postgres_pid")
if [[ -n "$observer_pid" ]]; then processes+=("$observer_pid"); fi
if [[ -n "$pipeline_spot_pid" ]]; then processes+=("$pipeline_spot_pid"); fi
if [[ -n "$pipeline_um_pid" ]]; then processes+=("$pipeline_um_pid"); fi
if [[ -n "$pipeline_options_pid" ]]; then processes+=("$pipeline_options_pid"); fi
wait -n -p finished_pid "${processes[@]}"
status=$?
set -e
if [[ "${finished_pid:-}" != "$engine_pid" && "$status" -eq 0 ]]; then
  echo "PostgreSQL or a managed service unexpectedly stopped" >&2
  status=1
fi
exit "$status"
