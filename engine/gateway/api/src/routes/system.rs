//! Health and data worker configuration.
use super::*;

pub(super) async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "workers": state.runtime.inflight(),
        "contract_version": CONTRACT_VERSION,
    }))
}

pub(super) async fn runnable(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<RunnableBody>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .runnable(&tenant_id, &body.action)
            .map_err(AppError::from)?,
    ))
}
