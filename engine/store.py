"""Engine-local SQLite (WAL). Secrets are never written.

Runtime rows are keyed by tenant_id and, where an account is involved, account_id.
Existing books are copied onto tenant id "1", the migrated default production tenant.
"""

import base64
import json
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS connections (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment)
);
CREATE TABLE IF NOT EXISTS order_requests (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
CREATE TABLE IF NOT EXISTS orders (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    order_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, client_order_id)
);
CREATE TABLE IF NOT EXISTS fills (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    fill_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, fill_id)
);
CREATE TABLE IF NOT EXISTS positions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    position_id TEXT NOT NULL,
    position_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, position_id)
);
CREATE TABLE IF NOT EXISTS gaps (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    gap_key TEXT NOT NULL,
    gap_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, scope, gap_key)
);
CREATE TABLE IF NOT EXISTS subscriptions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    product TEXT NOT NULL,
    symbol TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, product, symbol, role)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_loads (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    issuance_id TEXT NOT NULL,
    code_key TEXT NOT NULL,
    version TEXT NOT NULL,
    grant_purpose TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    entry_point TEXT NOT NULL,
    package_hash TEXT NOT NULL,
    archive BLOB NOT NULL,
    loaded_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, issuance_id)
);
"""

RUNTIME_TABLES = (
    "connections",
    "order_requests",
    "orders",
    "fills",
    "positions",
    "gaps",
    "subscriptions",
    "events",
    "strategy_loads",
)
LEGACY_TENANT_ID = "1"


def _strategy_load(row, *, include_archive=False):
    if row is None:
        return None
    item = dict(row)
    item["parameters"] = json.loads(item.pop("parameters_json"))
    archive = item.pop("archive", b"")
    if include_archive:
        item["archive"] = bytes(archive) if archive is not None else b""
    return item


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = None
        self.lock = threading.Lock()
        self._open()

    def _open(self):
        self.db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self._migrate_legacy()
        self.db.executescript(SCHEMA)
        self._ensure_strategy_load_columns()
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def _migrate_legacy(self):
        tables = {
            row[0]
            for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        pending = []
        for name in RUNTIME_TABLES:
            if name not in tables:
                continue
            columns = [row[1] for row in self.db.execute(f"PRAGMA table_info({name})")]
            if "tenant_id" not in columns:
                pending.append(name)
        if not pending:
            return
        for name in pending:
            self.db.execute(f"ALTER TABLE {name} RENAME TO {name}_legacy")
        self.db.executescript(SCHEMA)
        copies = {
            "connections": """INSERT INTO connections
                (tenant_id, account_id, environment, status, updated_at)
                SELECT ?, account_id, environment, status, updated_at FROM connections_legacy""",
            "order_requests": """INSERT INTO order_requests
                (tenant_id, account_id, request_id, body_json, updated_at)
                SELECT ?, account_id, request_id, body_json, updated_at FROM order_requests_legacy""",
            "orders": """INSERT INTO orders
                (tenant_id, account_id, environment, client_order_id, order_json, updated_at)
                SELECT ?, account_id, environment, client_order_id, order_json, updated_at
                FROM orders_legacy""",
            "fills": """INSERT INTO fills
                (tenant_id, account_id, environment, fill_id, fill_json, updated_at)
                SELECT ?, account_id, environment, fill_id, fill_json, updated_at FROM fills_legacy""",
            "positions": """INSERT INTO positions
                (tenant_id, account_id, environment, position_id, position_json, updated_at)
                SELECT ?, account_id, environment, position_id, position_json, updated_at
                FROM positions_legacy""",
            "gaps": """INSERT INTO gaps
                (tenant_id, account_id, scope, gap_key, gap_json, updated_at)
                SELECT ?, account_id, scope, gap_key, gap_json, updated_at FROM gaps_legacy""",
            "subscriptions": """INSERT INTO subscriptions
                (tenant_id, account_id, product, symbol, role, state, progress_json, updated_at)
                SELECT ?, account_id, product, symbol, role, state, progress_json, updated_at
                FROM subscriptions_legacy""",
            "events": """INSERT INTO events
                (id, tenant_id, account_id, kind, body_json, created_at)
                SELECT id, ?, account_id, kind, body_json, created_at FROM events_legacy""",
        }
        for name, sql in copies.items():
            if f"{name}_legacy" in {
                row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }:
                self.db.execute(sql, (LEGACY_TENANT_ID,))
                self.db.execute(f"DROP TABLE {name}_legacy")
        self.db.execute(
            """INSERT INTO tenants (tenant_id, purpose, status) VALUES (?, 'production', 'active')
               ON CONFLICT(tenant_id) DO NOTHING""",
            (LEGACY_TENANT_ID,),
        )

    def _ensure_strategy_load_columns(self):
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(strategy_loads)")}
        if "entry_point" not in columns:
            self.db.execute(
                "ALTER TABLE strategy_loads ADD COLUMN entry_point TEXT NOT NULL DEFAULT ''"
            )
        if "package_hash" not in columns:
            self.db.execute(
                "ALTER TABLE strategy_loads ADD COLUMN package_hash TEXT NOT NULL DEFAULT ''"
            )
        if "archive" not in columns:
            self.db.execute(
                "ALTER TABLE strategy_loads ADD COLUMN archive BLOB NOT NULL DEFAULT X''"
            )

    def _ensure(self):
        if self.db is None:
            self._open()

    def close(self):
        with self.lock:
            if self.db is not None:
                self.db.close()
                self.db = None

    def _write(self, sql, params=()):
        with self.lock:
            self._ensure()
            self.db.execute(sql, params)

    def ensure_tenant(self, tenant_id, purpose, status):
        self._write(
            """INSERT INTO tenants (tenant_id, purpose, status) VALUES (?, ?, ?)
               ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status""",
            (str(tenant_id), purpose, status),
        )
        return self.tenant(tenant_id)

    def tenant(self, tenant_id):
        self._ensure()
        row = self.db.execute(
            "SELECT * FROM tenants WHERE tenant_id=?", (str(tenant_id),)
        ).fetchone()
        return dict(row) if row else None

    def upsert_connection(self, tenant_id, account_id, environment, status):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO connections (tenant_id, account_id, environment, status, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, environment) DO UPDATE SET
                 status=excluded.status, updated_at=excluded.updated_at""",
            (str(tenant_id), str(account_id), environment, status, now),
        )
        return self.connection(tenant_id, account_id, environment)

    def connection(self, tenant_id, account_id, environment):
        self._ensure()
        row = self.db.execute(
            """SELECT * FROM connections
               WHERE tenant_id=? AND account_id=? AND environment=?""",
            (str(tenant_id), str(account_id), environment),
        ).fetchone()
        return dict(row) if row else None

    def get_request(self, tenant_id, account_id, request_id):
        self._ensure()
        row = self.db.execute(
            """SELECT body_json FROM order_requests
               WHERE tenant_id=? AND account_id=? AND request_id=?""",
            (str(tenant_id), str(account_id), str(request_id)),
        ).fetchone()
        return json.loads(row["body_json"]) if row else None

    def save_request(self, tenant_id, account_id, request_id, body):
        stored = {key: value for key, value in body.items() if key != "replay"}
        self._write(
            """INSERT INTO order_requests
               (tenant_id, account_id, request_id, body_json, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, request_id) DO UPDATE SET
                 body_json=excluded.body_json, updated_at=excluded.updated_at""",
            (
                str(tenant_id),
                str(account_id),
                str(request_id),
                json.dumps(stored, ensure_ascii=False),
                int(time.time() * 1000),
            ),
        )
        return stored

    def mark_request_ledger_synced(self, tenant_id, account_id, request_id):
        with self.lock:
            self._ensure()
            row = self.db.execute(
                """SELECT body_json FROM order_requests
                   WHERE tenant_id=? AND account_id=? AND request_id=?""",
                (str(tenant_id), str(account_id), str(request_id)),
            ).fetchone()
            if row is None:
                return None
            body = json.loads(row["body_json"])
            if not body.get("ledger_synced"):
                body["ledger_synced"] = True
                self.db.execute(
                    """UPDATE order_requests SET body_json=?, updated_at=?
                       WHERE tenant_id=? AND account_id=? AND request_id=?""",
                    (json.dumps(body, ensure_ascii=False), int(time.time() * 1000),
                     str(tenant_id), str(account_id), str(request_id)),
                )
            return body

    def upsert_order(self, tenant_id, account_id, environment, order):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO orders
               (tenant_id, account_id, environment, client_order_id, order_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, environment, client_order_id) DO UPDATE SET
                 order_json=excluded.order_json, updated_at=excluded.updated_at""",
            (
                str(tenant_id),
                str(account_id),
                environment,
                order["client_order_id"],
                json.dumps(order, ensure_ascii=False),
                now,
            ),
        )

    def orders(self, tenant_id, account_id, environment):
        self._ensure()
        rows = self.db.execute(
            """SELECT order_json FROM orders
               WHERE tenant_id=? AND account_id=? AND environment=? ORDER BY updated_at""",
            (str(tenant_id), str(account_id), environment),
        ).fetchall()
        return [json.loads(row["order_json"]) for row in rows]

    def upsert_fill(self, tenant_id, account_id, environment, fill_id, fill):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO fills
               (tenant_id, account_id, environment, fill_id, fill_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, environment, fill_id) DO UPDATE SET
                 fill_json=excluded.fill_json, updated_at=excluded.updated_at""",
            (
                str(tenant_id),
                str(account_id),
                environment,
                str(fill_id),
                json.dumps(fill, ensure_ascii=False),
                now,
            ),
        )

    def fills(self, tenant_id, account_id, environment):
        self._ensure()
        rows = self.db.execute(
            """SELECT fill_json FROM fills
               WHERE tenant_id=? AND account_id=? AND environment=? ORDER BY updated_at""",
            (str(tenant_id), str(account_id), environment),
        ).fetchall()
        return [json.loads(row["fill_json"]) for row in rows]

    def upsert_position(self, tenant_id, account_id, environment, position):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO positions
               (tenant_id, account_id, environment, position_id, position_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, environment, position_id) DO UPDATE SET
                 position_json=excluded.position_json, updated_at=excluded.updated_at""",
            (
                str(tenant_id),
                str(account_id),
                environment,
                str(position["id"]),
                json.dumps(position, ensure_ascii=False),
                now,
            ),
        )

    def positions(self, tenant_id, account_id, environment):
        self._ensure()
        rows = self.db.execute(
            """SELECT position_json FROM positions
               WHERE tenant_id=? AND account_id=? AND environment=? ORDER BY position_id""",
            (str(tenant_id), str(account_id), environment),
        ).fetchall()
        return [json.loads(row["position_json"]) for row in rows]

    def upsert_gap(self, tenant_id, account_id, scope, gap_key, gap):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO gaps (tenant_id, account_id, scope, gap_key, gap_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, scope, gap_key) DO UPDATE SET
                 gap_json=excluded.gap_json, updated_at=excluded.updated_at""",
            (str(tenant_id), str(account_id), scope, gap_key, json.dumps(gap, ensure_ascii=False), now),
        )

    def gaps(self, tenant_id, account_id, scope=None):
        self._ensure()
        if scope is None:
            rows = self.db.execute(
                """SELECT gap_json FROM gaps WHERE tenant_id=? AND account_id=?
                   ORDER BY scope, gap_key""",
                (str(tenant_id), str(account_id)),
            ).fetchall()
        else:
            rows = self.db.execute(
                """SELECT gap_json FROM gaps
                   WHERE tenant_id=? AND account_id=? AND scope=? ORDER BY gap_key""",
                (str(tenant_id), str(account_id), scope),
            ).fetchall()
        return [json.loads(row["gap_json"]) for row in rows]

    def upsert_subscription(self, tenant_id, account_id, product, symbol, role, state, progress):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO subscriptions
               (tenant_id, account_id, product, symbol, role, state, progress_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, product, symbol, role) DO UPDATE SET
                 state=excluded.state, progress_json=excluded.progress_json,
                 updated_at=excluded.updated_at""",
            (
                str(tenant_id),
                str(account_id),
                product,
                symbol,
                role,
                state,
                json.dumps(progress, ensure_ascii=False),
                now,
            ),
        )

    def subscriptions(self, tenant_id, account_id):
        self._ensure()
        rows = self.db.execute(
            """SELECT tenant_id, account_id, product, symbol, role, state, progress_json, updated_at
               FROM subscriptions WHERE tenant_id=? AND account_id=?
               ORDER BY product, symbol, role""",
            (str(tenant_id), str(account_id)),
        ).fetchall()
        return [{**dict(row), "progress": json.loads(row["progress_json"])} for row in rows]

    def tenant_subscriptions(self, tenant_id):
        self._ensure()
        rows = self.db.execute(
            """SELECT tenant_id, account_id, product, symbol, role, state, progress_json, updated_at
               FROM subscriptions WHERE tenant_id=?
               ORDER BY product, symbol, role""",
            (str(tenant_id),),
        ).fetchall()
        return [{**dict(row), "progress": json.loads(row["progress_json"])} for row in rows]

    def latest_event(self, tenant_id, account_id, kind):
        self._ensure()
        row = self.db.execute(
            """SELECT id, kind, body_json, created_at FROM events
               WHERE tenant_id=? AND account_id=? AND kind=?
               ORDER BY id DESC LIMIT 1""",
            (str(tenant_id), str(account_id), kind),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "kind": row["kind"],
            "body": json.loads(row["body_json"]),
            "created_at": row["created_at"],
        }

    def add_event(self, tenant_id, account_id, kind, body):
        now = int(time.time() * 1000)
        with self.lock:
            self._ensure()
            cursor = self.db.execute(
                """INSERT INTO events (tenant_id, account_id, kind, body_json, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (str(tenant_id), str(account_id), kind, json.dumps(body, ensure_ascii=False), now),
            )
            return cursor.lastrowid

    def events(self, tenant_id, account_id, after=0, limit=200):
        self._ensure()
        rows = self.db.execute(
            """SELECT id, kind, body_json, created_at FROM events
               WHERE tenant_id=? AND account_id=? AND id>? ORDER BY id LIMIT ?""",
            (str(tenant_id), str(account_id), int(after), int(limit)),
        ).fetchall()
        return [
            {
                "id": row["id"],
                "kind": row["kind"],
                "body": json.loads(row["body_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def upsert_strategy_load(
        self, tenant_id, account_id, issuance_id, code_key, version, grant_purpose, parameters,
        entry_point, package_hash, archive,
    ):
        now = int(time.time() * 1000)
        self._write(
            """INSERT INTO strategy_loads (
                   tenant_id, account_id, issuance_id, code_key, version, grant_purpose,
                   parameters_json, entry_point, package_hash, archive, loaded_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, account_id, issuance_id) DO UPDATE SET
                 code_key=excluded.code_key, version=excluded.version,
                 grant_purpose=excluded.grant_purpose,
                 parameters_json=excluded.parameters_json,
                 entry_point=excluded.entry_point, package_hash=excluded.package_hash,
                 archive=excluded.archive, loaded_at=excluded.loaded_at""",
            (
                str(tenant_id), str(account_id), str(issuance_id), code_key, version, grant_purpose,
                json.dumps(parameters, ensure_ascii=False, sort_keys=True),
                entry_point, package_hash, archive, now,
            ),
        )
        return self.strategy_load(tenant_id, account_id, issuance_id)

    def strategy_load(self, tenant_id, account_id, issuance_id):
        self._ensure()
        row = self.db.execute(
            """SELECT tenant_id, account_id, issuance_id, code_key, version, grant_purpose,
                      parameters_json, entry_point, package_hash, loaded_at
               FROM strategy_loads
               WHERE tenant_id=? AND account_id=? AND issuance_id=?""",
            (str(tenant_id), str(account_id), str(issuance_id)),
        ).fetchone()
        return _strategy_load(row)

    def strategy_loads(self, tenant_id, account_id=None, *, include_archive=False):
        self._ensure()
        columns = (
            "tenant_id, account_id, issuance_id, code_key, version, grant_purpose, "
            "parameters_json, entry_point, package_hash, loaded_at"
        )
        if include_archive:
            columns += ", archive"
        if account_id is None:
            rows = self.db.execute(
                f"""SELECT {columns}
                   FROM strategy_loads WHERE tenant_id=?
                   ORDER BY account_id, issuance_id""",
                (str(tenant_id),),
            ).fetchall()
        else:
            rows = self.db.execute(
                f"""SELECT {columns}
                   FROM strategy_loads WHERE tenant_id=? AND account_id=?
                   ORDER BY issuance_id""",
                (str(tenant_id), str(account_id)),
            ).fetchall()
        return [_strategy_load(row, include_archive=include_archive) for row in rows]

    def export_tenant(self, tenant_id):
        self._ensure()
        tenant_id = str(tenant_id)
        payload = {"tenant": self.tenant(tenant_id), "tables": {}}
        for name in RUNTIME_TABLES:
            rows = self.db.execute(
                f"SELECT * FROM {name} WHERE tenant_id=? ORDER BY rowid", (tenant_id,)
            ).fetchall()
            exported = []
            for row in rows:
                item = dict(row)
                if name == "strategy_loads" and isinstance(item.get("archive"), (bytes, memoryview)):
                    item["archive"] = base64.b64encode(bytes(item["archive"])).decode()
                exported.append(item)
            payload["tables"][name] = exported
        return payload

    def delete_runtime(self, tenant_id):
        tenant_id = str(tenant_id)
        with self.lock:
            self._ensure()
            for name in RUNTIME_TABLES:
                self.db.execute(f"DELETE FROM {name} WHERE tenant_id=?", (tenant_id,))

    def delete_tenant(self, tenant_id):
        self.delete_runtime(tenant_id)
        self._write("DELETE FROM tenants WHERE tenant_id=?", (str(tenant_id),))

    def import_tenant(self, tenant_id, purpose, payload):
        tenant_id = str(tenant_id)
        self.delete_runtime(tenant_id)
        tables = payload.get("tables") or {}
        with self.lock:
            self._ensure()
            for name in RUNTIME_TABLES:
                for row in tables.get(name) or []:
                    item = {**row, "tenant_id": tenant_id}
                    if name == "strategy_loads" and isinstance(item.get("archive"), str):
                        item["archive"] = base64.b64decode(item["archive"])
                    columns = list(item)
                    sql = (
                        f"INSERT INTO {name} ({', '.join(columns)}) "
                        f"VALUES ({', '.join('?' for _ in columns)})"
                    )
                    self.db.execute(sql, [item[column] for column in columns])
        return self.ensure_tenant(tenant_id, purpose, "active")
