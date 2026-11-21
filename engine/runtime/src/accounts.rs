//! Account registration keeps API secrets in process memory.
//! SQLite stores only the connection status. Snapshot calls use the supplied
//! loopback gateway and never write the secret.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_venue::reconciliation::{reconcile_assets, reconcile_positions};
use seealpha_venue::{Credentials, Product, VenueClient, VenueError};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

type SecretMap = HashMap<(String, String, String), Credentials>;

fn secrets() -> &'static Mutex<SecretMap> {
    static SECRETS: OnceLock<Mutex<SecretMap>> = OnceLock::new();
    SECRETS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key(tenant_id: &str, account_id: &str, environment: &str) -> (String, String, String) {
    (
        tenant_id.to_owned(),
        account_id.to_owned(),
        environment.to_owned(),
    )
}

fn scrub(text: &str, api_key: &str, secret: &str) -> String {
    let mut out = text.to_owned();
    if !api_key.is_empty() {
        out = out.replace(api_key, "[redacted]");
    }
    if !secret.is_empty() {
        out = out.replace(secret, "[redacted]");
    }
    out
}

fn map_venue(error: VenueError, api_key: &str, secret: &str) -> RuntimeError {
    RuntimeError::Conflict(scrub(&error.to_string(), api_key, secret))
}

impl Runtime {
    pub fn register_connection(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        api_key: &str,
        secret: &str,
    ) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Conflict("开发租户不能向交易所下单。".into()));
        }
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        validate_secret(api_key, secret)?;
        secrets()
            .lock()
            .map_err(|_| RuntimeError::Conflict("账户连接暂不可用。".into()))?
            .insert(
                key(tenant_id, account_id, environment),
                Credentials {
                    api_key: api_key.to_owned(),
                    secret: secret.to_owned(),
                },
            );
        self.ledger
            .upsert_connection(tenant_id, account_id, environment, "registered")
            .map_err(map_ledger)?;
        let body = json!({"environment": environment, "status": "registered"}).to_string();
        self.ledger
            .add_event(tenant_id, account_id, "connection", &body)
            .map_err(map_ledger)?;
        Ok(json!({
            "tenant_id": tenant_id,
            "account_id": account_id,
            "environment": environment,
            "status": "registered",
        }))
    }

    pub fn forget_connection(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        if let Ok(mut guard) = secrets().lock() {
            guard.remove(&key(tenant_id, account_id, environment));
        }
        self.ledger
            .upsert_connection(tenant_id, account_id, environment, "closed")
            .map_err(map_ledger)?;
        let body = json!({"environment": environment, "status": "closed"}).to_string();
        self.ledger
            .add_event(tenant_id, account_id, "connection", &body)
            .map_err(map_ledger)?;
        Ok(json!({
            "tenant_id": tenant_id,
            "account_id": account_id,
            "environment": environment,
            "status": "closed",
        }))
    }

    /// Run one leased reconciliation through signed exchange READS only.
    /// Never sends an order, marks settlement, or infers a fill from an ACK.
    /// Missing credentials, missing orders and incomplete trades remain
    /// unresolved with persistent retry/backoff or manual review.
    pub fn run_one_registered_order_reconciliation(
        &self,
        tenant: &str,
        worker: &str,
        venue_base: &str,
    ) -> Result<Option<Value>, RuntimeError> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = || -> Result<i64, RuntimeError> {
            let ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| RuntimeError::Conflict("系统时钟无效。".into()))?
                .as_millis();
            i64::try_from(ms).map_err(|_| RuntimeError::Conflict("系统时钟溢出。".into()))
        };
        self.require_active(tenant)?;
        let begun = now()?;
        // Network queries can exceed the lease; a late worker cannot
        // overwrite a later generation's result.
        let Some(task) = self.claim_order_reconciliation(tenant, worker, begun, 300_000)? else {
            return Ok(None);
        };
        let (result, outcome, code) = match self.read_reconciliation_evidence(&task, venue_base) {
            Ok(report) => (Ok(report), "evidence_recorded", None),
            Err(err) => {
                // Never persist remote error strings (they might include
                // credentials or unreliable exchange payload details).
                let invalid = matches!(
                    err,
                    RuntimeError::BadRequest(_) | RuntimeError::Unprocessable(_)
                );
                let reason = if invalid {
                    "INVALID_EVIDENCE"
                } else {
                    "READ_UNCONFIRMED"
                };
                (
                    Err(err),
                    if invalid { "manual" } else { "retry" },
                    Some(reason),
                )
            }
        };
        self.finish_order_reconciliation(
            tenant,
            &task.account_id,
            &task.request_id,
            worker,
            task.attempts,
            now()?,
            outcome,
            code,
        )?;
        result.map(Some)
    }

    /// Only signed GET routes are reachable here. This method deliberately
    /// does not call legacy order-request updates or post account balances.
    fn read_reconciliation_evidence(
        &self,
        task: &seealpha_ledger::ReconciliationTask,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        use rust_decimal::Decimal;
        if !matches!(task.environment.as_str(), "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("无效的对账账户环境。".into()));
        }
        let tenant = self.require_active(&task.tenant_id)?;
        if tenant.purpose != "production" {
            return Err(RuntimeError::Unprocessable(
                "非正式租户不能查询真实订单。".into(),
            ));
        }
        let intent = self
            .ledger
            .registered_order_intent(&task.tenant_id, &task.account_id, &task.request_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::BadRequest("订单意图不存在。".into()))?;
        if intent["environment"].as_str() != Some(task.environment.as_str())
            || intent["client_order_id"].as_str() != Some(task.client_order_id.as_str())
        {
            return Err(RuntimeError::BadRequest("订单身份或环境不一致。".into()));
        }
        // Native execution orders cannot be reconciled by the legacy
        // signed-REST adapter. Native execution reports must carry verified
        // account/order ownership and be admitted separately to Ledger.
        // Conflict means retry/manual review, never a legacy GET fallback.
        if intent["execution_backend"] == "nautilus_binance_native" {
            return Err(RuntimeError::Conflict(
                "原生执行订单的认证对账尚未接线，禁止回退旧连接器。".into(),
            ));
        }
        let symbol = intent["symbol"]
            .as_str()
            .ok_or_else(|| RuntimeError::BadRequest("订单交易对无效。".into()))?;
        let product = match intent["product"].as_str() {
            Some("spot") => Product::Spot,
            Some("futures_um") => Product::FuturesUm,
            Some("options") => Product::Option,
            _ => return Err(RuntimeError::BadRequest("订单产品不可核验。".into())),
        };
        let creds = secrets()
            .lock()
            .map_err(|_| RuntimeError::Conflict("账户凭据状态不可用。".into()))?
            .get(&key(&task.tenant_id, &task.account_id, &task.environment))
            .cloned()
            .ok_or_else(|| RuntimeError::Conflict("没有已登记的账户凭据。".into()))?;
        validate_secret(&creds.api_key, &creds.secret)?;
        let venue = VenueClient::new(venue_base)
            .map_err(|_| RuntimeError::BadRequest("对账网关配置无效。".into()))?;
        let order = venue
            .query_order(&creds, product, symbol, &task.client_order_id)
            .map_err(|_| RuntimeError::Conflict("签名订单查询未确认。".into()))?
            .ok_or_else(|| RuntimeError::Conflict("交易所未找到订单，保持待核实。".into()))?;
        if order["client_order_id"].as_str() != Some(task.client_order_id.as_str())
            || order["symbol"].as_str() != Some(symbol)
            || order["side"].as_str() != intent["side"].as_str()
        {
            return Err(RuntimeError::BadRequest(
                "交易所订单身份或方向冲突。".into(),
            ));
        }
        let quantity = order["quantity"]
            .as_str()
            .and_then(|v| v.parse::<Decimal>().ok())
            .ok_or_else(|| RuntimeError::BadRequest("订单数量缺失。".into()))?;
        let expected = intent["quantity"]
            .as_str()
            .and_then(|v| v.parse::<Decimal>().ok())
            .ok_or_else(|| RuntimeError::BadRequest("预登记订单数量无效。".into()))?;
        if quantity != expected {
            return Err(RuntimeError::BadRequest("订单数量与授权意图冲突。".into()));
        }
        let id = order["order_id"]
            .as_str()
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .ok_or_else(|| RuntimeError::BadRequest("交易所订单编号无效。".into()))?;
        let filled = order["filled"]
            .as_str()
            .and_then(|v| v.parse::<Decimal>().ok())
            .ok_or_else(|| RuntimeError::BadRequest("订单累计成交量无效。".into()))?;
        if filled < Decimal::ZERO || filled > expected {
            return Err(RuntimeError::BadRequest("累计成交量超出订单数量。".into()));
        }
        let status = order["status"]
            .as_str()
            .ok_or_else(|| RuntimeError::BadRequest("缺少订单状态。".into()))?;
        if !matches!(
            status,
            "NEW" | "PARTIALLY_FILLED" | "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED"
        ) {
            return Err(RuntimeError::BadRequest(
                "交易所订单状态暂不能核实。".into(),
            ));
        }
        let trades = venue
            .query_order_trades(&creds, product, symbol, id)
            .map_err(|_| RuntimeError::Conflict("签名成交明细查询失败。".into()))?;
        let mut ids = Vec::with_capacity(trades.len());
        let mut seen = std::collections::HashSet::new();
        let mut total = Decimal::ZERO;
        for fill in &trades {
            if fill["source"] != "signed_exchange_user_trades"
                || fill["venue_order_id"].as_str() != Some(id)
            {
                return Err(RuntimeError::BadRequest(
                    "成交明细证据来源或订单不一致。".into(),
                ));
            }
            let trade_id = fill["trade_id"]
                .as_str()
                .filter(|v| seen.insert(*v))
                .ok_or_else(|| RuntimeError::BadRequest("交易所成交编号重复。".into()))?;
            let qty = fill["quantity"]
                .as_str()
                .and_then(|v| v.parse::<Decimal>().ok())
                .filter(|v| *v > Decimal::ZERO)
                .ok_or_else(|| RuntimeError::BadRequest("成交数量无效。".into()))?;
            total = total
                .checked_add(qty)
                .ok_or_else(|| RuntimeError::BadRequest("成交数量溢出。".into()))?;
            ids.push(trade_id.to_owned());
        }
        // A completed GET with missing historical trades is not a fill.
        if total != filled
            || (status == "FILLED" && total != expected)
            || (status == "PARTIALLY_FILLED" && (total <= Decimal::ZERO || total >= expected))
            || (status == "NEW" && total != Decimal::ZERO)
            || (status == "REJECTED" && total != Decimal::ZERO)
        {
            return Err(RuntimeError::Conflict(
                "交易所订单状态与成交记录尚未一致。".into(),
            ));
        }
        // Independently query the account; a response is only read evidence,
        // not authorization or an account snapshot in Ledger.
        if product == Product::FuturesUm {
            venue
                .account_snapshot(&creds)
                .map_err(|_| RuntimeError::Conflict("账户签名快照查询未确认。".into()))?;
        }
        // Durable verified trade IDs precede the order observation.
        self.ledger
            .import_verified_fills(
                &task.tenant_id,
                &task.account_id,
                &task.environment,
                id,
                &trades,
            )
            .map_err(map_ledger)?;
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| RuntimeError::Conflict("系统时钟无效。".into()))?
            .as_nanos();
        let at_ns =
            i64::try_from(at).map_err(|_| RuntimeError::Conflict("系统时钟溢出。".into()))?;
        let observation = json!({
            "environment":task.environment,
            "client_order_id":task.client_order_id,
            "venue_order_id":id,
            "status":status,
            "source":"signed_exchange_order_query",
            "observed_at_ns":at_ns,
            "trade_ids":ids,
        });
        let observation_id = format!("read_{}_{}", task.attempts, at_ns);
        self.ledger
            .append_order_observation(
                &task.tenant_id,
                &task.account_id,
                &task.environment,
                &task.client_order_id,
                &observation_id,
                &observation,
            )
            .map_err(map_ledger)?;
        Ok(json!({
            "request_id":task.request_id,
            "client_order_id":task.client_order_id,
            "status":status,
            "verified_trade_count":trades.len(),
            "verified_quantity":total.to_string(),
            "account_settled":false,
            "resend_order":false,
            "reconciliation_only":true
        }))
    }

    /// Reconcile an order using the account credentials already registered
    /// in memory. The HTTP reconciliation request never needs API secrets.
    pub fn sync_registered_fills(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        let credentials = secrets()
            .lock()
            .map_err(|_| RuntimeError::Conflict("账户连接状态锁不可用。".into()))?
            .get(&key(tenant, account, environment))
            .cloned()
            .ok_or_else(|| {
                RuntimeError::Conflict("尚未登记账户凭据，不能核对交易所真实成交。".into())
            })?;
        self.sync_verified_order_fills(
            tenant,
            account,
            environment,
            request_id,
            &credentials.api_key,
            &credentials.secret,
            venue_base,
        )
    }

    pub fn account_snapshot(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        api_key: &str,
        secret: &str,
        venue_base: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        let (api_key, secret) = if api_key.is_empty() || secret.is_empty() {
            let guard = secrets()
                .lock()
                .map_err(|_| RuntimeError::Conflict("账户连接暂不可用。".into()))?;
            let stored = guard
                .get(&key(tenant_id, account_id, environment))
                .ok_or_else(|| {
                    RuntimeError::Conflict("账户连接未登记，业务端需先下发凭据。".into())
                })?;
            (stored.api_key.clone(), stored.secret.clone())
        } else {
            (api_key.to_owned(), secret.to_owned())
        };
        validate_secret(&api_key, &secret)?;
        let Some(base) = venue_base.filter(|value| !value.is_empty()) else {
            return Err(RuntimeError::Conflict(
                "交易网关未配置，拒绝向默认交易所发单。".into(),
            ));
        };
        if base.starts_with("http://")
            && !(base.starts_with("http://127.0.0.1:") || base.starts_with("http://localhost:"))
        {
            return Err(RuntimeError::Conflict("交易网关地址无效。".into()));
        }
        let client = VenueClient::new(base).map_err(|error| map_venue(error, &api_key, &secret))?;
        let credentials = Credentials {
            api_key: api_key.clone(),
            secret: secret.clone(),
        };
        let snapshot = client
            .account_snapshot(&credentials)
            .map_err(|error| map_venue(error, &api_key, &secret))?;
        let text = scrub(&snapshot.to_string(), &api_key, &secret);
        let snapshot: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
        let positions = snapshot["positions"].as_array().map(Vec::len).unwrap_or(0);
        let body = json!({"environment": environment, "positions": positions}).to_string();
        self.ledger
            .add_event(tenant_id, account_id, "account_snapshot", &body)
            .map_err(map_ledger)?;
        drop(credentials);
        Ok(snapshot)
    }
    /// Account evidence and discrepancy report for a single product. This is a
    /// read-and-audit operation, not a balance mutation or trading approval.
    /// Expected snapshots are supplied by an authenticated caller and must
    /// correspond to the same tenant, account, environment and product.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_account_product(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        product: Product,
        expected: &Value,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        let credentials = secrets()
            .lock()
            .map_err(|_| RuntimeError::Conflict("账户凭据锁不可用。".into()))?
            .get(&key(tenant, account, environment))
            .cloned()
            .ok_or_else(|| RuntimeError::Conflict("账户凭据未登记。".into()))?;
        validate_secret(&credentials.api_key, &credentials.secret)?;
        let client = VenueClient::new(venue_base)
            .map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?;
        let observed = client
            .account_snapshot_product(&credentials, product)
            .map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?;
        let assets = reconcile_assets(product, expected, &observed)
            .map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?;
        let positions = if product == Product::Spot {
            Vec::new()
        } else {
            reconcile_positions(product, expected, &observed)
                .map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?
        };
        let to_json = |diffs: Vec<seealpha_venue::reconciliation::AccountDifference>| {
            diffs
                .into_iter()
                .map(|d| {
                    json!({"key":d.key,
                "expected":d.expected.to_string(),"observed":d.observed.to_string()})
                })
                .collect::<Vec<_>>()
        };
        let product_name = match product {
            Product::Spot => "spot",
            Product::FuturesUm => "futures_um",
            Product::Option => "options",
        };
        let report = json!({"tenant_id":tenant,"account_id":account,
            "environment":environment,"product":product_name,
            "asset_differences":to_json(assets),"position_differences":to_json(positions),
            "source":"signed_exchange_snapshot",
            "status":"comparison_only"});
        self.ledger
            .add_event(
                tenant,
                account,
                "account_reconciliation",
                &report.to_string(),
            )
            .map_err(map_ledger)?;
        Ok(report)
    }

    /// Import only exchange-authored settlement records. Public rate observations
    /// and theoretical exercise payoffs are never posted as account cash.
    #[allow(clippy::too_many_arguments)]
    pub fn sync_registered_settlements(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        product: Product,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        let active = self.require_active(tenant)?;
        if active.purpose != "production" || !matches!(environment, "production" | "sandbox") {
            return Err(RuntimeError::Conflict(
                "结算核对要求正式租户与有效账户环境。".into(),
            ));
        }
        let credentials = secrets()
            .lock()
            .map_err(|_| RuntimeError::Conflict("凭据状态锁不可用。".into()))?
            .get(&key(tenant, account, environment))
            .cloned()
            .ok_or_else(|| RuntimeError::Conflict("尚未登记账户凭据。".into()))?;
        validate_secret(&credentials.api_key, &credentials.secret)?;
        let client = VenueClient::new(venue_base)
            .map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?;
        let (product_name, rows) = match product {
            Product::FuturesUm => (
                "futures_um",
                client.um_funding_income(&credentials, symbol, start_ms, end_ms),
            ),
            Product::Option => (
                "options",
                client.option_exercise_records(&credentials, symbol, start_ms, end_ms),
            ),
            Product::Spot => {
                return Err(RuntimeError::Unprocessable(
                    "现货不使用资金费或期权行权结算入口。".into(),
                ));
            }
        };
        let rows = rows.map_err(|e| map_venue(e, &credentials.api_key, &credentials.secret))?;
        self.ledger
            .import_account_settlements(tenant, account, environment, product_name, &rows)
            .map_err(map_ledger)?;
        Ok(json!({"tenant_id":tenant,"account_id":account,
            "environment":environment,"product":product_name,"symbol":symbol,
            "records":rows,"verified_source":true,"automatic_cash_posting":false}))
    }
}

fn validate_secret(api_key: &str, secret: &str) -> Result<(), RuntimeError> {
    if api_key.is_empty() || secret.is_empty() || api_key == secret {
        return Err(RuntimeError::Conflict(
            "已保存的 API Key 与 Secret Key 相同，无法生成有效签名。请编辑账户，填写同一组 API 对应的 Key 和 Secret Key。".into(),
        ));
    }
    if api_key.chars().any(|char| char == '\n' || char == '\r')
        || secret.chars().any(|char| char == '\n' || char == '\r')
    {
        return Err(RuntimeError::Conflict("交易账户凭据无效。".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthMode, Config, Runtime};
    use std::fs;

    fn runtime(venue_base: Option<String>) -> (Runtime, std::path::PathBuf) {
        // Distinct SQLite state per parallel test: clock timestamps alone are
        // not a safe uniqueness guarantee at high concurrency.
        static NEXT_TEST_ROOT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "seealpha-accounts-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST_ROOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("state")).unwrap();
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: AuthMode::None,
            token: String::new(),
            state_dir: root.join("state"),
            project: root.clone(),
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base,
            python: std::path::PathBuf::from("python3"),
        };
        let runtime = Runtime::open(&config).unwrap();
        (runtime, root)
    }

    fn sqlite_blob(root: &std::path::Path) -> Vec<u8> {
        let mut blob = Vec::new();
        for entry in fs::read_dir(root.join("state")).unwrap() {
            let path = entry.unwrap().path();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            if name.starts_with("book.sqlite") {
                blob.extend(fs::read(&path).unwrap());
            }
        }
        blob
    }

    #[test]
    fn test_connection_secrets_stay_in_memory_and_out_of_sqlite() {
        let (runtime, root) = runtime(None);
        runtime
            .ledger
            .ensure_tenant("7", "production", "active")
            .unwrap();
        let body = runtime
            .register_connection("7", "11", "production", "key-one", "secret-one")
            .unwrap();
        assert_eq!(body["status"], "registered");
        assert!(!body.to_string().contains("secret-one"));
        assert!(!body.to_string().contains("key-one"));
        let blob = sqlite_blob(&root);
        assert!(!blob.windows(10).any(|window| window == b"secret-one"));
        assert!(!blob.windows(7).any(|window| window == b"key-one"));
        let closed = runtime.forget_connection("7", "11", "production").unwrap();
        assert_eq!(closed["status"], "closed");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_development_tenant_cannot_register_exchange_credentials() {
        let (runtime, root) = runtime(None);
        runtime
            .ledger
            .ensure_tenant("8", "development", "active")
            .unwrap();
        let error = runtime
            .register_connection("8", "11", "production", "key-one", "secret-one")
            .unwrap_err();
        assert!(error.to_string().contains("开发租户"));
        let blob = sqlite_blob(&root);
        assert!(!blob.windows(10).any(|window| window == b"secret-one"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_account_snapshot_uses_loopback_and_does_not_store_secrets() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let local = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            local.block_on(async move {
                let app =
                    axum::Router::new().route("/fapi/v2/account", axum::routing::get(mock_account));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let port = receiver.recv().unwrap();
        let (runtime, root) = runtime(Some(format!("http://127.0.0.1:{port}")));
        runtime
            .ledger
            .ensure_tenant("9", "development", "active")
            .unwrap();
        let snapshot = runtime
            .account_snapshot(
                "9",
                "11",
                "production",
                "key-one",
                "secret-one",
                Some(&format!("http://127.0.0.1:{port}")),
            )
            .unwrap();
        assert_eq!(snapshot["balances"][0]["walletBalance"], "1.5");
        assert_eq!(snapshot["positions"][0]["symbol"], "ETHUSDT");
        let rendered = snapshot.to_string();
        assert!(!rendered.contains("secret-one"));
        assert!(!rendered.contains("key-one"));
        let blob = sqlite_blob(&root);
        assert!(!blob.windows(10).any(|window| window == b"secret-one"));
        assert!(!blob.windows(7).any(|window| window == b"key-one"));
        let missing =
            runtime.account_snapshot("9", "11", "production", "key-one", "secret-one", None);
        assert!(missing.is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_credentials_keep_reconciliation_retryable_without_order_send() {
        let (runtime, root) = runtime(None);
        runtime
            .ledger
            .ensure_tenant("restore", "production", "active")
            .unwrap();
        let intent = json!({"environment":"production","action":"NEW",
            "client_order_id":"client1","symbol":"ETHUSDT","product":"spot",
            "side":"BUY","quantity":"0.1"});
        runtime
            .ledger
            .preregister_order_intent(
                "restore",
                "account1",
                "production",
                "req1",
                "client1",
                &"a".repeat(64),
                &intent,
            )
            .unwrap();
        assert_eq!(
            runtime.enqueue_order_reconciliations("restore", 1).unwrap(),
            1
        );
        assert!(
            runtime
                .run_one_registered_order_reconciliation(
                    "restore",
                    "worker1",
                    "http://127.0.0.1:12345",
                )
                .is_err()
        );
        let task = runtime
            .ledger
            .order_reconciliation_task("restore", "account1", "req1")
            .unwrap()
            .unwrap();
        assert_eq!(task.state, "retry");
        assert_eq!(task.attempts, 1);
        assert_eq!(task.last_error_code.as_deref(), Some("READ_UNCONFIRMED"));
        assert!(
            runtime
                .ledger
                .latest_order_observation("restore", "account1", "production", "client1")
                .unwrap()
                .is_none()
        );
        assert!(
            runtime
                .ledger
                .fills("restore", "account1", "production")
                .unwrap()
                .is_empty()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn signed_get_worker_records_new_without_fill_or_post() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let local = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            local.block_on(async move {
                let app = axum::Router::new()
                    .route("/api/v3/order", axum::routing::get(mock_reconcile_order))
                    .route(
                        "/api/v3/myTrades",
                        axum::routing::get(mock_reconcile_trades),
                    );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                sender.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        });
        let port = receiver.recv().unwrap();
        let base = format!("http://127.0.0.1:{port}");
        let (runtime, root) = runtime(Some(base.clone()));
        runtime
            .ledger
            .ensure_tenant("sync", "production", "active")
            .unwrap();
        runtime
            .register_connection("sync", "account1", "production", "key-one", "secret-one")
            .unwrap();
        let intent = json!({"environment":"production","action":"NEW",
            "client_order_id":"client1","symbol":"ETHUSDT","product":"spot",
            "side":"BUY","quantity":"0.1"});
        runtime
            .ledger
            .preregister_order_intent(
                "sync",
                "account1",
                "production",
                "req1",
                "client1",
                &"b".repeat(64),
                &intent,
            )
            .unwrap();
        runtime.enqueue_order_reconciliations("sync", 1).unwrap();
        let result = runtime
            .run_one_registered_order_reconciliation("sync", "worker1", &base)
            .unwrap()
            .unwrap();
        assert_eq!(result["resend_order"], false);
        assert_eq!(result["status"], "NEW");
        assert_eq!(result["verified_trade_count"], 0);
        let observed = runtime
            .ledger
            .latest_order_observation("sync", "account1", "production", "client1")
            .unwrap()
            .unwrap();
        assert_eq!(observed["status"], "NEW");
        assert!(
            runtime
                .ledger
                .fills("sync", "account1", "production")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            runtime
                .ledger
                .order_reconciliation_task("sync", "account1", "req1")
                .unwrap()
                .unwrap()
                .state,
            "retry"
        );
        let _ = fs::remove_dir_all(root);
    }

    async fn mock_reconcile_order(
        request: axum::extract::Request,
    ) -> axum::Json<serde_json::Value> {
        assert_eq!(request.method(), axum::http::Method::GET);
        assert!(request.uri().query().unwrap_or("").contains("signature="));
        axum::Json(json!({
            "clientOrderId":"client1","orderId":42,"symbol":"ETHUSDT",
            "side":"BUY","type":"LIMIT","origQty":"0.1","executedQty":"0",
            "price":"2000","status":"NEW",
        }))
    }
    async fn mock_reconcile_trades(
        request: axum::extract::Request,
    ) -> axum::Json<serde_json::Value> {
        assert_eq!(request.method(), axum::http::Method::GET);
        assert!(request.uri().query().unwrap_or("").contains("signature="));
        axum::Json(json!([]))
    }

    async fn mock_account(request: axum::extract::Request) -> axum::Json<serde_json::Value> {
        let query = request.uri().query().unwrap_or("");
        assert!(query.contains("signature="));
        assert!(!query.contains("secret-one"));
        assert!(!query.contains("key-one"));
        let header = request
            .headers()
            .get("x-mbx-apikey")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert_eq!(header, "key-one");
        axum::Json(json!({
            "assets": [{"asset": "USDT", "walletBalance": "1.5", "note": "key-one"}],
            "positions": [{"symbol": "ETHUSDT", "positionAmt": "0"}],
            "canTrade": true,
            "updateTime": 10,
            "echo": "secret-one"
        }))
    }
}
