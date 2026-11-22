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
        };
        let runtime = Arc::new(Runtime::open(&config).unwrap());
        runtime.ensure_tenant("42", "development", "active").unwrap();
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
    async fn retired_registry_and_account_routes_are_absent() {
        let app = router(state());
        let token = "Bearer ci-engine-token-0123456789abcdef0123456789abcdef";
        for (method, path) in [
            ("PUT", "/tenants/42"),
            ("POST", "/tenants/42/accounts/a/connections"),
            ("GET", "/tenants/42/accounts/a/health"),
            ("POST", "/tenants/42/accounts/a/snapshots"),
        ] {
            let request = Request::builder()
                .method(method).uri(path).header("authorization", token)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"api_secret":"never-echo"}"#)).unwrap();
            let (status, _, body) = call(&app, request).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert!(!body.to_string().contains("never-echo"));
        }
    }

    #[tokio::test]
    async fn historical_carry_and_option_requests_fail_closed_without_required_evidence() {
        let app = router(state());
        let token = "ci-engine-token-0123456789abcdef0123456789abcdef";
        let headers = format!("Bearer {token}");
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
        // No mounted Catalog is a distinct missing-evidence error, never a
        // fabricated backtest result.
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(
            response["detail"]
                .as_str()
                .unwrap_or("")
                .contains("Catalog")
        );
    }

    #[tokio::test]
    async fn retired_catalog_bar_routes_are_absent() {
        let app = router(state());
        let token = "Bearer ci-engine-token-0123456789abcdef0123456789abcdef";
        for (method, path) in [
            ("POST", "/tenants/42/data/catalog/bars"),
            ("GET", "/tenants/42/data/catalog/bars"),
            ("POST", "/tenants/42/data/coverage"),
            ("POST", "/tenants/42/data/page"),
            ("POST", "/tenants/42/data/chart"),
            ("POST", "/tenants/42/data/latest"),
        ] {
            let request = Request::builder()
                .method(method).uri(path).header("authorization", token)
                .header("content-type", "application/json")
                .body(Body::from("{}")).unwrap();
            let (status, _, _) = call(&app, request).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn native_funding_is_read_only_settled_exchange_evidence() {
        let setup = state();
        setup
            .runtime
            .ensure_tenant("42", "development", "active")
            .unwrap();
        // Missing Pipeline Catalog must fail closed rather than read Ledger rates.
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
        assert_ne!(status, StatusCode::OK);
        assert!(data.get("rows").is_none());
    }

