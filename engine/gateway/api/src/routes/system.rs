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

pub(super) async fn jobs(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .data_job_list(&tenant_id)
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn worker_settings(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    Ok(Json(
        state
            .runtime
            .control
            .worker_settings()
            .map_err(|e| AppError::bad_request(e.to_string()))?,
    ))
}

pub(super) async fn update_worker_settings(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let workers = body["max_workers"]
        .as_i64()
        .ok_or_else(|| AppError::bad_request("Worker 数量无效。"))?;
    let timeout = body["timeout_seconds"]
        .as_i64()
        .ok_or_else(|| AppError::bad_request("Worker 超时无效。"))?;
    Ok(Json(
        state
            .runtime
            .control
            .update_worker_settings(workers, timeout)
            .map_err(|e| AppError::bad_request(e.to_string()))?,
    ))
}

