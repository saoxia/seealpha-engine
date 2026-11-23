//! Read-only historical order rows and verified Ledger fills.
use super::*;

pub(super) async fn list_orders(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "paper".into());
    // Historical order rows only; not an authoritative Nautilus order view.
    let connection = seealpha_database::Connection::open(&state.config.book_path())
        .map_err(|error| AppError::bad_request(format!("状态库不可用。{error}")))?;
    let mut statement = connection
        .prepare(
            "SELECT order_json FROM orders
             WHERE account_id=$1 AND environment=$2
             ORDER BY updated_at, client_order_id",
        )
        .map_err(|error| AppError::bad_request(format!("订单查询失败。{error}")))?;
    let rows = statement
        .query_map(
            seealpha_database::params![account_id, environment],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| AppError::bad_request(format!("订单查询失败。{error}")))?;
    let raw_orders = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| AppError::bad_request(format!("订单查询失败。{error}")))?;
    let items = raw_orders
        .into_iter()
        .map(|raw| {
            serde_json::from_str::<Value>(&raw)
                .map_err(|_| AppError::conflict("持久化订单记录损坏。"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({ "orders": items })))
}

pub(super) async fn list_fills(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(query): Query<EnvironmentQuery>,
) -> Result<Json<Value>, AppError> {
    let environment = query.environment.unwrap_or_else(|| "production".into());
    let fills = state
        .ledger
        .fills(&account_id, &environment)
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

