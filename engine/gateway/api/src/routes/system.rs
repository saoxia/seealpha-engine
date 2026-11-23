use super::*;
pub(super) async fn health()->Json<Value>{
    Json(json!({"status":"ok","workers":0,"contract_version":CONTRACT_VERSION}))
}
