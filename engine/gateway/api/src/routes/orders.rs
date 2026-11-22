//! Order commands, reconciliation and ledger reads.
use super::*;

pub(super) async fn list_orders(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "paper".into());
    let orders = state
        .runtime
        .stored_orders(&tenant_id, &account_id, &environment)
        .map_err(|error| match error {
            seealpha_ledger::LedgerError::Conflict(message) => AppError::conflict(message),
            seealpha_ledger::LedgerError::Database(detail) => {
                AppError::bad_request(format!("状态库不可用。{detail}"))
            }
        })?;
    let items = orders
        .into_iter()
        .map(|order| {
            serde_json::from_str::<Value>(&order.order_json).unwrap_or(json!({ "venue": "paper" }))
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({ "orders": items })))
}

pub(super) async fn order_command(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let venue_base = state
        .config
        .venue_base
        .clone()
        .ok_or_else(|| AppError::conflict("交易网关未配置，拒绝向默认交易所发单。"))?;
    let environment = body
        .get("environment")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let api_key = body
        .get("api_key")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let secret = body
        .get("api_secret")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let action = body
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let values = body.get("values").cloned().unwrap_or(Value::Null);
    let result = state.runtime.execute_order(OrderCommand {
        tenant_id,
        account_id,
        environment,
        api_key,
        secret,
        action,
        values,
        venue_base,
    });
    Ok(Json(result.map_err(AppError::from)?))
}

pub(super) async fn ledger_sync(
    State(state): State<AppState>,
    Path((tenant_id, account_id, request_id)): Path<(String, String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .mark_ledger_synced(&tenant_id, &account_id, &request_id)
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn reconcile_order(
    State(state): State<AppState>,
    Path((tenant_id, account_id, request_id)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let venue_base = state.config.venue_base.clone().unwrap_or_default();
    Ok(Json(
        state
            .runtime
            .reconcile_order(
                &tenant_id,
                &account_id,
                &request_id,
                body.get("environment")
                    .and_then(Value::as_str)
                    .unwrap_or("production"),
                body.get("api_key").and_then(Value::as_str).unwrap_or(""),
                body.get("api_secret").and_then(Value::as_str).unwrap_or(""),
                &venue_base,
                body.get("account_revision")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn list_fills(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "production".into());
    let fills = state
        .runtime
        .ledger
        .fills(&tenant_id, &account_id, &environment)
        .map_err(|error| match error {
            seealpha_ledger::LedgerError::Conflict(message) => AppError::conflict(message),
            seealpha_ledger::LedgerError::Database(detail) => AppError::bad_request(detail),
        })?;
    let items = fills
        .into_iter()
        .filter_map(|row| serde_json::from_str::<Value>(&row).ok())
        .collect::<Vec<_>>();
    Ok(Json(json!({ "fills": items })))
}

pub(super) async fn list_gaps(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let gaps = state
        .runtime
        .ledger
        .gaps_for_scope(
            &tenant_id,
            &account_id,
            query.get("scope").map(String::as_str),
        )
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(json!({ "gaps": gaps })))
}

pub(super) async fn put_gap(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let scope = body.get("scope").and_then(Value::as_str).unwrap_or("");
    let gap_key = body.get("gap_key").and_then(Value::as_str).unwrap_or("");
    let gap = body.get("gap").cloned().unwrap_or(json!({}));
    state
        .runtime
        .ledger
        .put_gap(&tenant_id, &account_id, scope, gap_key, &gap.to_string())
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    state
        .runtime
        .ledger
        .add_event(&tenant_id, &account_id, "gap", &gap.to_string())
        .ok();
    Ok(Json(gap))
}

pub(super) async fn list_events(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let after = query
        .get("after")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(200);
    Ok(Json(
        state
            .runtime
            .ledger
            .event_page(&tenant_id, &account_id, after, limit)
            .map_err(|error| AppError::bad_request(error.to_string()))?,
    ))
}
