//! Research v1 is a separate capability boundary from the Engine administrator API.
//! Dataset discovery is read-only. Snapshot export remains unimplemented.
use crate::{AppError, AppState, CONTRACT_HEADER, CONTRACT_VERSION, parse_time_ms};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use crate::research::{Grant, require_op, verify_research_grant};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
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
;
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

fn decode_body<T: DeserializeOwned>(body: &Bytes) -> Result<T, AppError> {
    serde_json::from_slice(body)
        .map_err(|_| AppError::unprocessable("Invalid Research request body"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthMode, CONTRACT_HEADER, Config, router};
    use axum::http::StatusCode;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use seealpha_market::BinanceSource;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn state() -> AppState {
        // Isolate parallel HTTP tests even when the wall clock has coarse resolution.
        static NEXT_TEST_ROOT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "research-http-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST_ROOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
        crate::research::sign_research_grant("research-signing-key-0123456789abcdef", &claims)
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
    async fn mcp_does_not_require_http_headers_or_advertise_fake_workers() {
        let app = router(state());
        let grant = signed_grant(&["datasets:read"], &["spot:ETHUSDT:1m"], "task-mcp");
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
        assert_eq!(tools[0]["name"], "list_datasets");
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
            &["snapshots:create"],
            &["spot:ETHUSDT:1m"],
            "task-malformed",
        );
        let path = "/research/v1/snapshots";
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
