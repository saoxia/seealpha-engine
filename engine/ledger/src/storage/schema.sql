
CREATE TABLE IF NOT EXISTS portfolio_coordinator_states (
 tenant_id TEXT NOT NULL,
 account_id TEXT NOT NULL,
 instrument TEXT NOT NULL,
 revision TEXT NOT NULL,
 body_json TEXT NOT NULL,
 PRIMARY KEY (tenant_id,account_id,instrument)
);
CREATE TABLE IF NOT EXISTS basket_journals (
 tenant_id TEXT NOT NULL,
 account_id TEXT NOT NULL,
 basket_id TEXT NOT NULL,
 revision TEXT NOT NULL,
 body_json TEXT NOT NULL,
 PRIMARY KEY (tenant_id, account_id, basket_id)
);
CREATE TABLE IF NOT EXISTS order_intents (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
CREATE INDEX IF NOT EXISTS order_intents_client
    ON order_intents(tenant_id, account_id, environment, client_order_id);
CREATE TABLE IF NOT EXISTS order_reconciliation_tasks (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    request_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','leased','retry','manual')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_due_ms INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_until_ms INTEGER,
    last_error_code TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,account_id,request_id),
    FOREIGN KEY(tenant_id,account_id,request_id)
        REFERENCES order_intents(tenant_id,account_id,request_id)
);
CREATE INDEX IF NOT EXISTS order_reconcile_due
    ON order_reconciliation_tasks(tenant_id,state,next_due_ms);
CREATE TABLE IF NOT EXISTS order_observations (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    observed_at_ns INTEGER NOT NULL,
    status TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,client_order_id,observation_id)
);
CREATE INDEX IF NOT EXISTS order_observations_by_time
    ON order_observations(tenant_id,account_id,environment,client_order_id,observed_at_ns);
CREATE TABLE IF NOT EXISTS account_evidence_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    kind TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    UNIQUE(tenant_id,account_id,environment,kind,evidence_id)
);
CREATE INDEX IF NOT EXISTS account_evidence_events_page
    ON account_evidence_events(tenant_id,account_id,environment,id);
CREATE TABLE IF NOT EXISTS account_snapshots (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    revision INTEGER NOT NULL,
    evidence_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    source TEXT NOT NULL,
    balances_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment)
);
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
-- Immutable request intent stays in order_requests. Confirmed results,
-- retries and reconciliation are appended as separate scoped projections.
CREATE TABLE IF NOT EXISTS order_request_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    FOREIGN KEY(tenant_id,account_id,request_id)
        REFERENCES order_requests(tenant_id,account_id,request_id)
);
CREATE INDEX IF NOT EXISTS order_request_views_by_request
    ON order_request_views(tenant_id,account_id,request_id,id);
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
CREATE TABLE IF NOT EXISTS quarantined_unverified_fills (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    fill_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,fill_id)
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
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_positions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, symbol)
);
CREATE TABLE IF NOT EXISTS account_settlements (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    product TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,product,event_kind,event_id)
);
CREATE INDEX IF NOT EXISTS account_settlement_symbol
    ON account_settlements(tenant_id,account_id,environment,product,symbol);
CREATE TABLE IF NOT EXISTS backtest_results (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    report_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, kind)
);
CREATE TABLE IF NOT EXISTS research_snapshots (
    tenant_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, snapshot_id)
);
CREATE TABLE IF NOT EXISTS paper_sessions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    report_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
