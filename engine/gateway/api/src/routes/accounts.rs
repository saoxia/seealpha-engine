//! Account connections and snapshots.
use super::*;

pub(super) async fn register_connection(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let environment = text(&body, "environment");
    Ok(Json(
        state
            .runtime
            .register_connection(&tenant_id, &account_id, &environment)
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
    let registry = state
        .runtime
        .ledger
        .account_registry_view(&tenant_id, &account_id)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(json!({
        "status": "ok",
        "workers": state.runtime.inflight(),
        "connections": registry["connections"],
        "live_account_verified": registry["live_account_verified"],
        "execution_authorized": registry["execution_authorized"]
    })))
}

pub(super) async fn account_snapshot(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let environment = text(&body, "environment");
    // The legacy HTTP contract is accepted for compatibility, but credentials
    // must never enter the native read-only account observation boundary.
    if !text(&body, "api_key").is_empty() || !text(&body, "api_secret").is_empty() {
        return Err(AppError::conflict("原生账户观察不接受请求内凭据。"));
    }
    Ok(Json(
        state
            .runtime
            .account_snapshot(
                &tenant_id,
                &account_id,
                &environment,
            )
            .map_err(AppError::from)?,
    ))
}
