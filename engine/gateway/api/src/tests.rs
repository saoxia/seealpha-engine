use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use seealpha_market::BinanceSource;
    use seealpha_runtime::Config;
    use tower::ServiceExt;

    fn state() -> AppState {
        let root = std::env::temp_dir().join(format!(
            "seealpha-http-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
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
            project: root.clone(),
            tenant_id: Some("42".into()),
            tenant_purpose: Some("development".into()),
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: std::path::PathBuf::from("python3"),
        };
        let runtime = Arc::new(Runtime::open(&config).unwrap());
        let market = Arc::new(BinanceSource::new().unwrap_or_else(|_| panic!("client")));
        AppState {
            config: Arc::new(config),
            runtime,
            market,
        }
    }

    async fn call(app: &Router, request: Request) -> (StatusCode, String, Value) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let header = response
            .headers()
            .get(CONTRACT_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(json!({}));
        (status, header, value)
    }

    #[tokio::test]
    async fn handshake_statuses_match_platform_v2() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let (status, header, _) = call(
            &app,
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(header, "2");
        let (status, header, body) = call(
            &app,
            Request::builder()
                .uri("/health")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header, "2");
        assert_eq!(body["contract_version"], "2");
        let (status, header, _) = call(
            &app,
            Request::builder()
                .uri("/health")
                .header("authorization", format!("Bearer {token}"))
                .header(CONTRACT_HEADER, "incompatible")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(header, "2");
        let (status, _, _) = call(
            &app,
            Request::builder()
                .uri("/tenants/99/jobs")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _, _) = call(
            &app,
            Request::builder()
                .uri("/tenants/42/accounts/a/orders")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .method("POST")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn worker_settings_require_an_active_tenant_even_when_unpinned() {
        let mut setup = state();
        Arc::get_mut(&mut setup.config).unwrap().tenant_id = None;
        setup
            .runtime
            .ensure_tenant("42", "development", "active")
            .unwrap();
        let app = router(setup);
        let request = |tenant: &str| {
            Request::builder()
                .uri(format!("/tenants/{tenant}/jobs/worker-settings"))
                .header(
                    "authorization",
                    "Bearer ci-engine-token-0123456789abcdef0123456789abcdef",
                )
                .body(Body::empty())
                .unwrap()
        };
        let (status, _, _) = call(&app, request("42")).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, _) = call(&app, request("99")).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn account_health_reads_only_its_own_connection_metadata() {
        let mut setup = state();
        Arc::get_mut(&mut setup.config).unwrap().tenant_id = None;
        setup
            .runtime
            .ensure_tenant("42", "development", "active")
            .unwrap();
        setup
            .runtime
            .ensure_tenant("43", "development", "active")
            .unwrap();
        setup
            .runtime
            .ledger
            .upsert_connection("42", "a", "production", "registered")
            .unwrap();
        setup
            .runtime
            .ledger
            .upsert_connection("42", "other", "sandbox", "registered")
            .unwrap();
        setup
            .runtime
            .ledger
            .upsert_connection("43", "a", "sandbox", "registered")
            .unwrap();
        let app = router(setup);
        let get_health = |tenant: &str, account: &str| {
            Request::builder()
                .uri(format!("/tenants/{tenant}/accounts/{account}/health"))
                .header(
                    "authorization",
                    "Bearer ci-engine-token-0123456789abcdef0123456789abcdef",
                )
                .body(Body::empty())
                .unwrap()
        };
        let (status, header, response) = call(&app, get_health("42", "a")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header, "2");
        assert_eq!(response["status"], "ok");
        let connections = response["connections"].as_array().unwrap();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0]["tenant_id"], "42");
        assert_eq!(connections[0]["account_id"], "a");
        assert_eq!(connections[0]["environment"], "production");
        assert_eq!(connections[0]["status"], "registered");
        assert!(response.to_string().find("api_secret").is_none());

        let (status, _, isolated) = call(&app, get_health("42", "missing")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(isolated["connections"].as_array().unwrap().is_empty());

        let (status, _, other) = call(&app, get_health("43", "a")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(other["connections"][0]["environment"], "sandbox");

        let (status, _, _) = call(&app, get_health("44", "a")).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn connection_and_snapshot_routes_do_not_echo_secrets() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let (status, _, _) = call(
            &app,
            Request::builder()
                .method("PUT")
                .uri("/tenants/42")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"development","status":"active"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, body) = call(
            &app,
            Request::builder()
                .method("POST")
                .uri("/tenants/42/accounts/11/connections")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"environment":"production","api_key":"key-one","api_secret":"secret-one"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let rendered = body.to_string();
        assert!(!rendered.contains("secret-one"));
        assert!(!rendered.contains("key-one"));
        let (status, _, body) = call(
            &app,
            Request::builder()
                .method("POST")
                .uri("/tenants/42/accounts/11/snapshots")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"environment":"production","api_key":"key-one","api_secret":"secret-one"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        let rendered = body.to_string();
        assert!(!rendered.contains("secret-one"));
        assert!(rendered.contains("Nautilus 原生账户及成交证据读取尚未接入"));
    }

    #[tokio::test]
    async fn historical_carry_and_option_requests_fail_closed_without_required_evidence() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let headers = format!("Bearer {token}");
        let (status, _, _) = call(
            &app,
            Request::builder()
                .method("PUT")
                .uri("/tenants/42")
                .header("authorization", &headers)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"development","status":"active"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let body = r#"{"start":"0","end":"1080000","interval":"1m","starting_spot_balance":"1000","starting_perpetual_balance":"1000","max_gross_notional":"1000"}"#;
        let (status, _, carry) = call(
            &app,
            Request::builder()
                .method("POST")
                .uri("/tenants/42/accounts/desk/carry-backtests")
                .header("authorization", &headers)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(carry["detail"].as_str().unwrap_or("").contains("手续费"));
        let (status, _, latest) = call(
            &app,
            Request::builder()
                .uri("/tenants/42/accounts/desk/carry-backtests/latest")
                .header("authorization", &headers)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(latest.get("detail").is_some());
        let protected = r#"{"start":"0","end":"240000","interval":"1m","starting_options_balance":"1000","starting_perpetual_balance":"1000","max_gross_notional":"1000"}"#;
        let (status, _, option) = call(
            &app,
            Request::builder()
                .method("POST")
                .uri("/tenants/42/accounts/desk/protected-option-backtests")
                .header("authorization", &headers)
                .header("content-type", "application/json")
                .body(Body::from(protected))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(option["detail"].as_str().unwrap_or("").contains("期权合约"));
        let (status, _, latest_option) = call(
            &app,
            Request::builder()
                .uri("/tenants/42/accounts/desk/protected-option-backtests/latest")
                .header("authorization", &headers)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(latest_option.get("detail").is_some());
    }

    #[tokio::test]
    async fn native_control_and_reader_routes_do_not_start_collectors() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let make = |path: &str, method: &str, body: &str| {
            Request::builder()
                .uri(path)
                .method(method)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap()
        };
        let (status, _, _) = call(
            &app,
            make(
                "/tenants/42",
                "PUT",
                r#"{"purpose":"development","status":"active"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status,_,policy)=call(&app,make(
            "/tenants/42/data/control/sync","POST",
            r#"{"policies":[{"product":"spot","symbol":"ETHUSDT","realtime_enabled":true,"base_level":"1m"}]}"#
        )).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(policy["synced"], 1);
        let (status, _, subscription) = call(
            &app,
            make("/tenants/42/data/control/subscriptions", "POST", "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(subscription["items"].as_array().unwrap().len(), 1);
        let (status, _, service) = call(
            &app,
            make("/tenants/42/data/control/services", "POST", "{}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(service["active_collectors"], 0);
        let (status,_,_)=call(&app,make(
            "/tenants/42/accounts/a/strategy-market/subscriptions","POST",
            r#"{"issuance_id":"unissued","streams":[{"product":"spot","symbol":"ETHUSDT","interval":"1m"}]}"#
        )).await;
        assert_eq!(status, StatusCode::CONFLICT);
        let (status, _, rows) = call(
            &app,
            make(
                "/tenants/42/accounts/a/positions?environment=paper",
                "GET",
                "",
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(rows["positions"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn native_backtest_denies_missing_history_without_fetching_or_synthesizing() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let make = |path: &str, method: &str, body: &str| {
            Request::builder()
                .uri(path)
                .method(method)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap()
        };
        let _ = call(
            &app,
            make(
                "/tenants/42",
                "PUT",
                r#"{"purpose":"development","status":"active"}"#,
            ),
        )
        .await;
        let (status, _, response) = call(
            &app,
            make(
                "/tenants/42/accounts/a/backtests",
                "POST",
                r#"{"product":"spot","symbol":"ETHUSDT","interval":"1m",
                "start":"2026-11-20T00:00:00Z","end":"2026-11-20T00:02:00Z",
                "starting_balance":"1000","quote_currency":"USDT"}"#,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            response["detail"]
                .as_str()
                .unwrap_or("")
                .contains("历史 K 线")
        );
    }

    #[tokio::test]
    async fn native_catalog_page_and_coverage_are_read_only_and_contiguous() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let make = |path: &str, method: &str, body: &str| {
            Request::builder()
                .uri(path)
                .method(method)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap()
        };
        let _ = call(
            &app,
            make(
                "/tenants/42",
                "PUT",
                r#"{"purpose":"development","status":"active"}"#,
            ),
        )
        .await;
        let (status,_,imported)=call(&app,make(
            "/tenants/42/data/catalog/bars","POST",
            r#"{"product":"spot","symbol":"ETHUSDT","interval":"1m",
                "bars":[
                    {"open_time_ms":0,"open":"100","high":"101","low":"99","close":"100","volume":"5"},
                    {"open_time_ms":60000,"open":"100","high":"102","low":"100","close":"101","volume":"4"}
                ]}"#
        )).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(imported["imported"], 2);
        let query = r#"{"product":"spot","symbol":"ETHUSDT","interval":"1m",
            "start_ms":0,"end_ms":120000}"#;
        let (status, _, coverage) =
            call(&app, make("/tenants/42/data/coverage", "POST", query)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(coverage["complete"], true);
        let (status, _, page) = call(&app, make("/tenants/42/data/page", "POST", query)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["records"].as_array().unwrap().len(), 2);
        let (status, _, _) = call(
            &app,
            make(
                "/tenants/42/data/catalog/bars",
                "POST",
                r#"{"product":"spot","symbol":"ETHUSDT","interval":"1m",
                "bars":[{"open_time_ms":0,"open":"100","high":"101","low":"99",
                        "close":"99","volume":"5"}]}"#,
            ),
        )
        .await;
        // Conflicting historical OHLCV is immutable; never silently overwrite it.
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn native_funding_is_read_only_settled_exchange_evidence() {
        let setup = state();
        setup
            .runtime
            .ensure_tenant("42", "development", "active")
            .unwrap();
        setup
            .runtime
            .ledger
            .import_funding_events(
                "42",
                "ETHUSDT",
                &[json!({
                    "symbol":"ETHUSDT","funding_time_ms":100,
                    "rate_type":"Regular","funding_rate":"0.0001","mark_price":"2000"
                })],
            )
            .unwrap();
        let app = router(setup);
        let request = Request::builder()
            .uri("/tenants/42/data/funding")
            .method("POST")
            .header(
                "authorization",
                "Bearer ci-engine-token-0123456789abcdef0123456789abcdef",
            )
            .header("content-type", "application/json")
            .body(Body::from(r#"{"symbol":"ETHUSDT","limit":10}"#))
            .unwrap();
        let (status, _, data) = call(&app, request).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(data["rows"].as_array().unwrap().len(), 1);
        assert_eq!(data["no_synthetic_funding"], true);
        assert_eq!(data["completeness"], "not_certified");
    }
