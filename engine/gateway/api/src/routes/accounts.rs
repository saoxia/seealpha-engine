//! Account connections and snapshots.
use super::*;

pub(super) async fn register_connection(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let environment = text(&body, "environment");
    let api_key = text(&body, "api_key");
    let secret = text(&body, "api_secret");
    Ok(Json(
        state
            .runtime
            .register_connection(&tenant_id, &account_id, &environment, &api_key, &secret)
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn forget_connection(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query
        .environment
        .ok_or_else(|| AppError::Unprocessable("账户环境无效。".into()))?;
    Ok(Json(
        state
            .runtime
            .forget_connection(&tenant_id, &account_id, &environment)
            .map_err(AppError::from)?,
    ))
}

/// Legacy account health parity: persisted registrations only, scoped to
/// the authenticated tenant/account. Neither an exchange connectivity check
/// nor authorization to trade.
pub(super) async fn account_health(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let connections = state
        .runtime
        .ledger
        .account_connections(&tenant_id, &account_id)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(json!({
        "status": "ok",
        "workers": state.runtime.inflight(),
        "connections": connections
    })))
}

pub(super) async fn account_snapshot(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let environment = text(&body, "environment");
    let api_key = text(&body, "api_key");
    let secret = text(&body, "api_secret");
    Ok(Json(
        state
            .runtime
            .account_snapshot(
                &tenant_id,
                &account_id,
                &environment,
                &api_key,
                &secret,
                state.config.venue_base.as_deref(),
            )
            .map_err(AppError::from)?,
    ))
}
