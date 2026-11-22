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
        .map_err(AppError::from)?;
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

