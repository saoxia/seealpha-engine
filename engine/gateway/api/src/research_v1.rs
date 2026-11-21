//! Research v1 is a separate capability boundary from the Engine administrator API.
//! The only executable research job here is an issued-strategy, task-scoped,
//! durable simulation. Snapshot export and candidate execution fail closed
//! until Runtime exposes their actual evidence and worker contracts.
use crate::{AppError, AppState, CONTRACT_HEADER, CONTRACT_VERSION, parse_time_ms};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use rust_decimal::Decimal;
use seealpha_research::{Grant, require_op, verify_research_grant};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

// MCP protocol lives in its own gateway directory; HTTP keeps the shared research context.
#[path = "../../mcp/src/research.rs"]
mod mcp;

const RESEARCH_HEADER: &str = "X-SeeAlpha-Research-Contract-Version";
const RESEARCH_VERSION: &str = "1";

pub(super) fn routes(mcp_enabled: bool) -> Router<AppState> {
    let mut routes = Router::new()
        .route("/research/v1/datasets", get(datasets))
        .route("/research/v1/snapshots", post(snapshot_unavailable))
        .route(
            "/research/v1/snapshots/{snapshot_id}",
            get(snapshot_read_unavailable),
        )
        .route(
            "/research/v1/snapshots/{snapshot_id}/content",
            get(snapshot_read_unavailable),
        )
        .route("/research/v1/backtests", post(submit_backtest))
        .route("/research/v1/backtests/{backtest_id}", get(get_backtest))
        .route(
            "/research/v1/backtests/{backtest_id}/cancel",
            post(cancel_backtest),
        )
        .route(
            "/research/v1/candidate-backtests",
            post(candidate_unavailable),
        );
    if mcp_enabled {
        routes = routes.route("/research/mcp", post(mcp::handle));
    }
    routes
}

fn authorize(state: &AppState, headers: &HeaderMap, http_v1: bool) -> Result<Grant, AppError> {
    if http_v1
        && (headers.get(CONTRACT_HEADER).and_then(|h| h.to_str().ok()) != Some(CONTRACT_VERSION)
            || headers.get(RESEARCH_HEADER).and_then(|h| h.to_str().ok()) != Some(RESEARCH_VERSION))
    {
        return Err(AppError::upgrade("Research contract version mismatch"));
    }
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let (scheme, token) = presented
        .split_once(' ')
        .ok_or_else(|| AppError::unauthorized("Research authorization required"))?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim().is_empty() {
        return Err(AppError::unauthorized("Research authorization required"));
    }
    let signing_key = state
        .config
        .research_signing_key
        .as_ref()
        .ok_or_else(|| AppError::not_found("路径不存在。"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs() as i64)
        .unwrap_or(0);
    let grant = verify_research_grant(signing_key, token.trim(), now)
        .map_err(|_| AppError::unauthorized("Research authorization rejected"))?;
    if state.config.tenant_purpose.as_deref() != Some("development")
        || state.config.tenant_id.as_deref() != Some(grant.tenant_id.as_str())
    {
        return Err(AppError::forbidden("Research tenant scope denied"));
    }
    let tenant = state
        .runtime
        .require_active(&grant.tenant_id)
        .map_err(|_| AppError::forbidden("Research requires an active development tenant"))?;
    if tenant.purpose != "development" {
        return Err(AppError::forbidden(
            "Research requires an active development tenant",
        ));
    }
    Ok(grant)
}

fn scope(
    grant: &Grant,
    operation: &str,
    dataset: Option<&str>,
    strategy: Option<&str>,
) -> Result<(), AppError> {
    require_op(grant, operation, dataset, strategy)
        .map_err(|_| AppError::forbidden("Research operation outside granted scope"))
}

fn available_datasets(state: &AppState, grant: &Grant) -> Result<Value, AppError> {
    scope(grant, "datasets:read", None, None)?;
    let inventory = state
        .runtime
        .native_dataset_inventory(&grant.tenant_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::conflict("Research Catalog 未挂载，无法确认数据集库存。"))?;
    let datasets: Vec<Value> = inventory
        .into_iter()
        .filter_map(|mut record| {
            let product = record["product"].as_str()?;
            let symbol = record["symbol"].as_str()?;
            let level = record["level"].as_str()?;
            let id = format!("{product}:{symbol}:{level}");
            if !grant.datasets.iter().any(|allowed| allowed == &id) {
                return None;
            }
            record["id"] = json!(id);
            Some(record)
        })
        .collect();
    Ok(json!({"datasets": datasets}))
}

async fn datasets(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let grant = authorize(&state, &headers, true)?;
    Ok(Json(available_datasets(&state, &grant)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotRequest {
    dataset_id: String,
    start: String,
    end: String,
    format: String,
}

async fn snapshot_unavailable(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let grant = authorize(&state, &headers, true)?;
    let body: SnapshotRequest = decode_body(&body)?;
    scope(&grant, "snapshots:create", Some(&body.dataset_id), None)?;
    if body.format != "parquet"
        || parse_time_ms(&body.start).is_none()
        || parse_time_ms(&body.end).is_none()
    {
        return Err(AppError::unprocessable("Research snapshot request invalid"));
    }
    Err(AppError::not_implemented(
        "Runtime 尚无不可变 Parquet 快照导出与持久任务状态接口。",
    ))
}

async fn snapshot_read_unavailable(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(_snapshot_id): Path<String>,
) -> Result<Response, AppError> {
    let grant = authorize(&state, &headers, true)?;
    scope(&grant, "snapshots:read", None, None)?;
    Err(AppError::not_implemented(
        "Runtime 尚无任务归属校验、文件哈希与 Parquet 快照读取接口。",
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchBacktestRequest {
    purpose: String,
    execution: String,
    strategy_ref: String,
    dataset_refs: Vec<String>,
    start: String,
    end: String,
    starting_balance: String,
    quote_currency: String,
    #[serde(default = "empty_parameters")]
    parameters: Value,
    idempotency_key: String,
}

fn empty_parameters() -> Value {
    json!({})
}

fn decode_body<T: DeserializeOwned>(body: &Bytes) -> Result<T, AppError> {
    serde_json::from_slice(body)
        .map_err(|_| AppError::unprocessable("Invalid Research request body"))
}

fn valid_idempotency(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b':' | b'-'))
}

fn short_hash(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn validate_backtest(
    state: &AppState,
    grant: &Grant,
    body: &ResearchBacktestRequest,
) -> Result<(Value, String), AppError> {
    if body.purpose != "development"
        || body.execution != "simulation"
        || body.dataset_refs.len() != 1
        || !body
            .parameters
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
        || !valid_idempotency(&body.idempotency_key)
        || !(10..=40).contains(&body.start.len())
        || !(10..=40).contains(&body.end.len())
        || !(1..=40).contains(&body.starting_balance.len())
        || !(3..=8).contains(&body.quote_currency.len())
        || !body.quote_currency.bytes().all(|c| c.is_ascii_uppercase())
        || body.strategy_ref.len() > 128
    {
        return Err(AppError::unprocessable("Invalid research backtest request"));
    }
    let account = body
        .strategy_ref
        .strip_prefix("account:")
        .filter(|account| {
            (1..=40).contains(&account.len())
                && account
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
        })
        .ok_or_else(|| {
            AppError::unprocessable("strategy_ref must identify an issued development account")
        })?;
    let dataset = &body.dataset_refs[0];
    scope(
        grant,
        "backtests:submit",
        Some(dataset),
        Some(&body.strategy_ref),
    )?;
    let mut parts = dataset.split(':');
    let product = parts.next().unwrap_or("");
    let symbol = parts.next().unwrap_or("");
    let interval = parts.next().unwrap_or("");
    if parts.next().is_some()
        || !matches!(product, "spot" | "futures_um")
        || !matches!(interval, "1m" | "5m")
        || seealpha_market::check_symbol(symbol).is_err()
    {
        return Err(AppError::unprocessable(
            "Unsupported research dataset reference",
        ));
    }
    let start = parse_time_ms(&body.start)
        .ok_or_else(|| AppError::unprocessable("Invalid development backtest window"))?;
    let end = parse_time_ms(&body.end)
        .ok_or_else(|| AppError::unprocessable("Invalid development backtest window"))?;
    let step: i64 = if interval == "1m" { 60_000 } else { 300_000 };
    let count = end
        .checked_sub(start)
        .ok_or_else(|| AppError::unprocessable("Invalid development backtest window"))?;
    if start < 0
        || count <= 0
        || count % step != 0
        || count / step > 5000
        || start % step != 0
        || end % step != 0
    {
        return Err(AppError::unprocessable(
            "Invalid development backtest window",
        ));
    }
    if body
        .starting_balance
        .parse::<Decimal>()
        .ok()
        .is_none_or(|v| v <= Decimal::ZERO)
    {
        return Err(AppError::unprocessable("Invalid starting balance"));
    }
    let inventory = state
        .runtime
        .native_dataset_inventory(&grant.tenant_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::not_found("Research dataset not found"))?;
    if !inventory.iter().any(|item| {
        item["product"] == product && item["symbol"] == symbol && item["level"] == interval
    }) {
        return Err(AppError::not_found("Research dataset not found"));
    }
    let loads = state
        .runtime
        .strategy_loads(&grant.tenant_id, Some(account))
        .map_err(AppError::from)?;
    // The public load listing deliberately omits strategy parameters. Read
    // issued Runtime records too, without exposing archives to the caller.
    let strategies = state
        .runtime
        .stored_strategies(&grant.tenant_id, account)
        .map_err(AppError::from)?;
    let issued = loads["items"].as_array().is_some_and(|items| {
        !items.is_empty()
            && items.iter().all(|item| {
                item["grant_purpose"] == "development"
                    && strategies.iter().any(|strategy| {
                        item["issuance_id"] == strategy.member_id
                            && strategy.parameters["symbol"] == symbol
                    })
            })
    });
    if !issued {
        return Err(AppError::conflict(
            "No matching issued development strategy",
        ));
    }
    let params = json!({
        "account_id": account, "product": product, "symbol": symbol,
        "interval": interval, "start_ms": start, "end_ms": end,
        "starting_balance": body.starting_balance,
        "quote_currency": body.quote_currency,
        "_research": {
            "task_id": grant.task_id,
            "dataset_id": dataset,
            "strategy_ref": body.strategy_ref
        }
    });
    let idempotency = format!(
        "research.{}.{}",
        short_hash(&grant.task_id),
        short_hash(&body.idempotency_key)
    );
    Ok((params, idempotency))
}

fn public_job(job: &Value, with_result: bool) -> Value {
    let mut reply = json!({
        "backtest_id": job["id"],
        "status": job["status"],
    });
    if with_result && job["status"] == "completed" {
        reply["result"] = job["result"].clone();
    }
    reply
}

fn submit_job(
    state: &AppState,
    grant: &Grant,
    body: &ResearchBacktestRequest,
) -> Result<Value, AppError> {
    let (params, idempotency) = validate_backtest(state, grant, body)?;
    let job = state
        .runtime
        .schedule_backtest_job(&grant.tenant_id, &params, Some(&idempotency))
        .map_err(AppError::from)?;
    if job["status"] == "queued" {
        let id = job["id"]
            .as_str()
            .ok_or_else(|| AppError::conflict("Research task journal returned no identity"))?;
        crate::parity::spawn_backtest(
            state.runtime.clone(),
            grant.tenant_id.clone(),
            id.to_owned(),
        );
    }
    Ok(public_job(&job, false))
}

async fn submit_backtest(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let grant = authorize(&state, &headers, true)?;
    let body: ResearchBacktestRequest = decode_body(&body)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(submit_job(&state, &grant, &body)?),
    ))
}

fn scoped_job(
    state: &AppState,
    grant: &Grant,
    backtest_id: &str,
    operation: &str,
) -> Result<Value, AppError> {
    scope(grant, operation, None, None)?;
    if backtest_id.len() != 32
        || !backtest_id
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(AppError::not_found("Research backtest not found"));
    }
    let job = state
        .runtime
        .data_job(&grant.tenant_id, backtest_id)
        .map_err(|_| AppError::not_found("Research backtest not found"))?;
    let marker = &job["payload"]["_research"];
    if job["action"] != "backtest" || marker["task_id"] != grant.task_id {
        return Err(AppError::not_found("Research backtest not found"));
    }
    let dataset = marker["dataset_id"]
        .as_str()
        .ok_or_else(|| AppError::not_found("Research backtest not found"))?;
    let strategy = marker["strategy_ref"]
        .as_str()
        .ok_or_else(|| AppError::not_found("Research backtest not found"))?;
    scope(grant, operation, Some(dataset), Some(strategy))?;
    Ok(job)
}

fn read_job(state: &AppState, grant: &Grant, id: &str) -> Result<Value, AppError> {
    let job = scoped_job(state, grant, id, "backtests:read")?;
    Ok(public_job(&job, true))
}

fn cancel_job(state: &AppState, grant: &Grant, id: &str) -> Result<Value, AppError> {
    let _ = scoped_job(state, grant, id, "backtests:cancel")?;
    let cancelled = state
        .runtime
        .cancel_data_job(&grant.tenant_id, id)
        .map_err(AppError::from)?;
    Ok(public_job(&cancelled, false))
}

async fn get_backtest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(backtest_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let grant = authorize(&state, &headers, true)?;
    Ok(Json(read_job(&state, &grant, &backtest_id)?))
}

async fn cancel_backtest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(backtest_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let grant = authorize(&state, &headers, true)?;
    Ok(Json(cancel_job(&state, &grant, &backtest_id)?))
}

async fn candidate_unavailable(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let grant = authorize(&state, &headers, true)?;
    let body: Value = decode_body(&body)?;
    let strategy = body["strategy_ref"]
        .as_str()
        .ok_or_else(|| AppError::unprocessable("Missing candidate strategy_ref"))?;
    let dataset = body["dataset_refs"]
        .as_array()
        .filter(|refs| refs.len() == 1)
        .and_then(|refs| refs[0].as_str())
        .ok_or_else(|| AppError::unprocessable("Missing candidate dataset_ref"))?;
    scope(&grant, "candidates:submit", Some(dataset), Some(strategy))?;
    Err(AppError::not_implemented(
        "Runtime 尚未提供候选包静态校验、不可变持久任务和候选回测执行接口。",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthMode, CONTRACT_HEADER, Config, router};
    use axum::body::Body;
    use http_body_util::BodyExt;
    use seealpha_market::BinanceSource;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn state() -> AppState {
        let root = std::env::temp_dir().join(format!(
            "research-http-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: AuthMode::Token,
            token: "ci-engine-token-0123456789abcdef0123456789abcdef".into(),
            state_dir: root.join("state"),
            project: root,
            tenant_id: Some("42".into()),
            tenant_purpose: Some("development".into()),
            research_signing_key: Some("research-signing-key-0123456789abcdef".into()),
            research_mcp_enabled: true,
            venue_base: None,
            python: "python3".into(),
        };
        let runtime = Arc::new(crate::Runtime::open(&config).unwrap());
        runtime
            .ensure_tenant("42", "development", "active")
            .unwrap();
        AppState {
            config: Arc::new(config),
            runtime,
            market: Arc::new(BinanceSource::new().unwrap()),
        }
    }

    fn signed_grant(ops: &[&str], datasets: &[&str], task_id: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = json!({
            "audience":"seealpha-engine-research","version":1,
            "tenant_id":"42","task_id":task_id,"not_before":now-10,"expires_at":now+600,
            "ops":ops,"datasets":datasets,"strategies":["account:a"]
        });
        seealpha_research::sign_research_grant("research-signing-key-0123456789abcdef", &claims)
            .unwrap()
    }
    async fn request(
        app: &Router,
        path: &str,
        method: &str,
        bearer: Option<&str>,
        version: Option<&str>,
        research_version: Option<&str>,
        body: &str,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = bearer {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(v) = version {
            builder = builder.header(CONTRACT_HEADER, v);
        }
        if let Some(v) = research_version {
            builder = builder.header(RESEARCH_HEADER, v);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, headers, value)
    }

    #[tokio::test]
    async fn research_headers_grant_tenant_and_scope_fail_closed() {
        let app = router(state());
        let granted = signed_grant(&["datasets:read"], &["spot:ETHUSDT:1m"], "task-one");
        let path = "/research/v1/datasets";
        let (status, headers, _) = request(&app, path, "GET", None, Some("2"), Some("1"), "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[CONTRACT_HEADER], "2");
        assert_eq!(headers[RESEARCH_HEADER], "1");
        let (status, _, _) = request(&app, path, "GET", Some(&granted), None, Some("1"), "").await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        let (status, _, _) =
            request(&app, path, "GET", Some(&granted), Some("2"), Some("9"), "").await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        let (status,_,_) = request(&app,"/research/v1/snapshots","POST",
            Some(&granted),Some("2"),Some("1"),
            r#"{"dataset_id":"spot:ETHUSDT:1m","start":"2026-11-20T00:00:00Z","end":"2026-11-20T01:00:00Z","format":"parquet"}"#).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _, _) =
            request(&app, path, "GET", Some(&granted), Some("2"), Some("1"), "").await;
        assert_eq!(status, StatusCode::CONFLICT); // No actual Parquet catalog.
        let other = signed_grant(&["snapshots:create"], &["spot:ETHUSDT:1m"], "task-two");
        let (status,_,body) = request(&app,"/research/v1/snapshots","POST",
            Some(&other),Some("2"),Some("1"),
            r#"{"dataset_id":"spot:ETHUSDT:1m","start":"2026-11-20T00:00:00Z","end":"2026-11-20T01:00:00Z","format":"parquet"}"#).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body["detail"].as_str().unwrap().contains("Parquet"));
    }

    #[tokio::test]
    async fn only_own_research_task_can_read_or_cancel_persisted_backtest() {
        let st = state();
        let grant1 = signed_grant(
            &["backtests:read", "backtests:cancel"],
            &["spot:ETHUSDT:1m"],
            "task-one",
        );
        let grant2 = signed_grant(
            &["backtests:read", "backtests:cancel"],
            &["spot:ETHUSDT:1m"],
            "task-two",
        );
        let params = json!({
            "account_id":"a","product":"spot","symbol":"ETHUSDT","interval":"1m",
            "start_ms":0,"end_ms":60000,"starting_balance":"1000","quote_currency":"USDT",
            "_research":{"task_id":"task-one","dataset_id":"spot:ETHUSDT:1m","strategy_ref":"account:a"}
        });
        let job = st
            .runtime
            .schedule_backtest_job("42", &params, Some("research.demo.123"))
            .unwrap();
        let id = job["id"].as_str().unwrap();
        let app = router(st);
        let path = format!("/research/v1/backtests/{id}");
        let (status, _, _) =
            request(&app, &path, "GET", Some(&grant2), Some("2"), Some("1"), "").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _, data) =
            request(&app, &path, "GET", Some(&grant1), Some("2"), Some("1"), "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(data["status"], "queued");
        let (status, _, cancelled) = request(
            &app,
            &format!("{path}/cancel"),
            "POST",
            Some(&grant1),
            Some("2"),
            Some("1"),
            "{}",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cancelled["status"], "cancelled");
        let (status, _, _) =
            request(&app, &path, "GET", Some(&grant2), Some("2"), Some("1"), "").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mcp_does_not_require_http_headers_or_advertise_fake_workers() {
        let app = router(state());
        let grant = signed_grant(&["backtests:submit"], &["spot:ETHUSDT:1m"], "task-mcp");
        let (status, _, response) = request(
            &app,
            "/research/mcp",
            "POST",
            Some(&grant),
            None,
            None,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tools = response["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "submit_backtest");
        let (status,_,unknown)=request(&app,"/research/mcp","POST",Some(&grant),None,None,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_snapshot","arguments":{}}}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(unknown["error"]["code"], -32602);
        let (status, _, _) = request(
            &app,
            "/research/mcp",
            "POST",
            Some("bad-token"),
            None,
            None,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_research_request_is_422_before_any_job_is_queued() {
        let st = state();
        let grant = signed_grant(&["backtests:submit"], &["spot:ETHUSDT:1m"], "task-validate");
        let app = router(st);
        let (status, _, _) = request(
            &app,
            "/research/v1/backtests",
            "POST",
            Some(&grant),
            Some("2"),
            Some("1"),
            r#"{"purpose":"production","execution":"simulation","strategy_ref":"account:a",
                "dataset_refs":["spot:ETHUSDT:1m"],"start":"2026-11-20T00:00:00Z",
                "end":"2026-11-20T00:01:00Z","starting_balance":"1000","quote_currency":"USDT",
                "idempotency_key":"id-1"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
    #[tokio::test]
    async fn research_grant_cannot_access_admin_and_admin_token_cannot_access_research() {
        let st = state();
        let admin = st.config.token.clone();
        let grant = signed_grant(&["datasets:read"], &["spot:ETHUSDT:1m"], "task-isolation");
        let app = router(st);

        let (status, headers, _) =
            request(&app, "/health", "GET", Some(&grant), None, None, "").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[CONTRACT_HEADER], "2");

        let (status, _, _) = request(
            &app,
            "/research/v1/datasets",
            "GET",
            Some(&admin),
            Some("2"),
            Some("1"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, _, _) = request(
            &app,
            "/tenants/other/jobs",
            "GET",
            Some(&admin),
            None,
            None,
            "",
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn research_body_errors_never_override_authentication_or_contract_version() {
        let app = router(state());
        let grant = signed_grant(
            &["backtests:submit"],
            &["spot:ETHUSDT:1m"],
            "task-malformed",
        );
        let path = "/research/v1/backtests";
        let (status, _, _) = request(
            &app,
            path,
            "POST",
            None,
            Some("2"),
            Some("1"),
            "{invalid-json",
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, _, _) = request(
            &app,
            path,
            "POST",
            Some(&grant),
            Some("3"),
            Some("1"),
            "{invalid-json",
        )
        .await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);

        let (status, headers, _) = request(
            &app,
            path,
            "POST",
            Some(&grant),
            Some("2"),
            Some("1"),
            "{invalid-json",
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(headers[CONTRACT_HEADER], "2");
        assert_eq!(headers[RESEARCH_HEADER], "1");
    }

    #[tokio::test]
    async fn running_research_job_cannot_be_falsely_cancelled() {
        let st = state();
        let grant = signed_grant(
            &["backtests:read", "backtests:cancel"],
            &["spot:ETHUSDT:1m"],
            "task-running",
        );
        let params = json!({
            "account_id":"a", "product":"spot", "symbol":"ETHUSDT", "interval":"1m",
            "start_ms":0, "end_ms":60000, "starting_balance":"1000",
            "quote_currency":"USDT",
            "_research":{
                "task_id":"task-running", "dataset_id":"spot:ETHUSDT:1m",
                "strategy_ref":"account:a"
            }
        });
        let queued = st
            .runtime
            .schedule_backtest_job("42", &params, Some("research.running.id"))
            .unwrap();
        let id = queued["id"].as_str().unwrap();
        assert!(
            st.runtime
                .ledger
                .transition_data_job("42", id, "queued", "running", None,)
                .unwrap()
        );
        let app = router(st);
        let path = format!("/research/v1/backtests/{id}/cancel");
        let (status, _, body) = request(
            &app,
            &path,
            "POST",
            Some(&grant),
            Some("2"),
            Some("1"),
            "{}",
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_ne!(body["status"], "cancelled");
    }

    #[tokio::test]
    async fn mcp_rejects_browser_origin_and_bad_protocol() {
        let app = router(state());
        let grant = signed_grant(&["datasets:read"], &[], "task-mcp-protocol");
        let path = "/research/mcp";
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let mk = |origin: bool, version: &str| {
            let mut builder = axum::http::Request::builder()
                .method("POST")
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {grant}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("mcp-protocol-version", version);
            if origin {
                builder = builder.header("origin", "null");
            }
            builder.body(Body::from(body)).unwrap()
        };
        let app_forbidden = app.clone().oneshot(mk(true, mcp::MCP_VERSION)).await.unwrap();
        assert_eq!(app_forbidden.status(), StatusCode::FORBIDDEN);
        let app_bad_protocol = app.oneshot(mk(false, "2099-01-01")).await.unwrap();
        assert_eq!(app_bad_protocol.status(), StatusCode::BAD_REQUEST);
    }
}
