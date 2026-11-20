//! Account registration keeps API secrets in process memory.
//! SQLite stores only the connection status. Snapshot calls use the supplied
//! loopback gateway and never write the secret.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_venue::{Credentials, VenueClient, VenueError, Product};
use seealpha_venue::reconciliation::{reconcile_assets, reconcile_positions};
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

    /// Reconcile an order using the account credentials already registered
    /// in memory. The HTTP reconciliation request never needs API secrets.
    pub fn sync_registered_fills(
        &self,tenant:&str,account:&str,environment:&str,request_id:&str,
        venue_base:&str,
    )->Result<Value,RuntimeError>{
        self.require_active(tenant)?;
        if !matches!(environment,"production"|"sandbox"){
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        let credentials=secrets().lock()
            .map_err(|_|RuntimeError::Conflict("账户连接状态锁不可用。".into()))?
            .get(&key(tenant,account,environment)).cloned()
            .ok_or_else(||RuntimeError::Conflict(
                "尚未登记账户凭据，不能核对交易所真实成交。".into()))?;
        self.sync_verified_order_fills(
            tenant,account,environment,request_id,
            &credentials.api_key,&credentials.secret,venue_base
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
        &self, tenant:&str, account:&str, environment:&str,
        product:Product, expected:&Value, venue_base:&str,
    )->Result<Value,RuntimeError>{
        self.require_active(tenant)?;
        if !matches!(environment,"production"|"sandbox") {
            return Err(RuntimeError::Unprocessable("账户环境无效。".into()));
        }
        let credentials=secrets().lock()
            .map_err(|_|RuntimeError::Conflict("账户凭据锁不可用。".into()))?
            .get(&key(tenant,account,environment)).cloned()
            .ok_or_else(||RuntimeError::Conflict("账户凭据未登记。".into()))?;
        validate_secret(&credentials.api_key,&credentials.secret)?;
        let client=VenueClient::new(venue_base)
            .map_err(|e|map_venue(e,&credentials.api_key,&credentials.secret))?;
        let observed=client.account_snapshot_product(&credentials,product)
            .map_err(|e|map_venue(e,&credentials.api_key,&credentials.secret))?;
        let assets=reconcile_assets(product,expected,&observed)
            .map_err(|e|map_venue(e,&credentials.api_key,&credentials.secret))?;
        let positions=if product==Product::Spot {Vec::new()} else {
            reconcile_positions(product,expected,&observed)
                .map_err(|e|map_venue(e,&credentials.api_key,&credentials.secret))?
        };
        let to_json=|diffs:Vec<seealpha_venue::reconciliation::AccountDifference>| {
            diffs.into_iter().map(|d|json!({"key":d.key,
                "expected":d.expected.to_string(),"observed":d.observed.to_string()}))
                .collect::<Vec<_>>()
        };
        let product_name=match product {
            Product::Spot=>"spot",
            Product::FuturesUm=>"futures_um",
            Product::Option=>"options",
        };
        let report=json!({"tenant_id":tenant,"account_id":account,
            "environment":environment,"product":product_name,
            "asset_differences":to_json(assets),"position_differences":to_json(positions),
            "source":"signed_exchange_snapshot",
            "status":"comparison_only"});
        self.ledger.add_event(tenant,account,"account_reconciliation",
            &report.to_string()).map_err(map_ledger)?;
        Ok(report)
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
        let root = std::env::temp_dir().join(format!(
            "seealpha-accounts-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
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
