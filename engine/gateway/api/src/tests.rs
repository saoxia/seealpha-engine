use super::*;
use axum::body::Body;
use http_body_util::BodyExt;
use seealpha_market::BinanceSource;
use tower::ServiceExt;

fn state() -> AppState {
    let root=std::env::temp_dir().join(format!("seealpha-http-{}-{}",
        std::process::id(),std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&root).unwrap();
    let config=Config{
        host:"127.0.0.1".into(),port:0,state_dir:root.join("state"),
        project:root,research_mcp_enabled:false,
    };
    config.ensure_state_dir().unwrap();
    let book=config.book_path();
    let db=seealpha_database::Connection::open(&book).unwrap();
    seealpha_database::bootstrap_engine_schema(&db).unwrap();
    drop(db);
    AppState{
        config:Arc::new(config),
        ledger:Arc::new(Ledger::open(&book).unwrap()),
        strategy_store:Arc::new(StrategyStore::open(&book.with_file_name("strategies")).unwrap()),
        control:Arc::new(ControlStore::open(&book.with_file_name("control-state")).unwrap()),
        market:Arc::new(BinanceSource::new().unwrap()),
    }
}
async fn call(app:&Router,request:Request)->(StatusCode,Value){
    let response=app.clone().oneshot(request).await.unwrap();
    let status=response.status();
    let body=response.into_body().collect().await.unwrap().to_bytes();
    (status,serde_json::from_slice(&body).unwrap_or_else(|_|json!({})))
}
fn request(method:&str,path:&str,body:&str)->Request{
    Request::builder().method(method).uri(path).header("content-type","application/json")
        .body(Body::from(body.to_string())).unwrap()
}
#[tokio::test]
async fn health_is_local_and_requires_no_bearer(){
    let app=router(state());
    let (status,body)=call(&app,request("GET","/health","")).await;
    assert_eq!(status,StatusCode::OK);
    assert_eq!(body["contract_version"],"2");
    assert_eq!(body["workers"],0);
    let response=app.clone().oneshot(Request::builder().uri("/health")
        .header(CONTRACT_HEADER,"unsupported").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(),StatusCode::UPGRADE_REQUIRED);
}
#[tokio::test]
async fn current_paths_are_routed(){
    let app=router(state());
    let (status,_)=call(&app,request("GET","/pipeline/subscriptions","")).await;
    assert_eq!(status,StatusCode::OK);
    let (status,_)=call(&app,request("POST","/accounts/a/orders","{}")).await;
    assert_eq!(status,StatusCode::METHOD_NOT_ALLOWED);
    let (status,body)=call(&app,request("GET","/accounts/a/positions?environment=paper","")).await;
    assert_eq!(status,StatusCode::CONFLICT);
    assert!(body["detail"].as_str().unwrap_or("").contains("Nautilus"));
}
#[tokio::test]
async fn read_only_research_and_catalog_require_real_inventory_not_a_grant(){
    let app=router(state());
    let (status,body)=call(&app,request("GET","/research/v1/datasets","")).await;
    assert_eq!(status,StatusCode::CONFLICT);
    assert!(body["detail"].as_str().unwrap_or("").contains("Catalog"));
    let (status,body)=call(&app,request("POST","/data/catalog/inventory",
        r#"{"data_type":"trades","identifier":"ETHUSDT.BINANCE"}"#)).await;
    assert_eq!(status,StatusCode::NOT_FOUND);
    assert!(body["detail"].as_str().unwrap_or("").contains("Catalog"));
}
#[tokio::test]
async fn strategy_preview_does_not_create_orders_or_synthesize_state(){
    let app=router(state());
    let (status,body)=call(&app,request("POST","/accounts/a/strategy-preview",
        r#"{"symbol":"ETHUSDT","interval":"1m","bars":[]}"#)).await;
    assert_eq!(status,StatusCode::CONFLICT);
    assert!(body["detail"].as_str().is_some());
    let (status,_)=call(&app,request("POST","/accounts/a/paper/sessions","{}")).await;
    assert_eq!(status,StatusCode::NOT_FOUND);
}
