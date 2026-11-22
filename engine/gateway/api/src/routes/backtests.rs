//! History, carry and option backtest HTTP handlers.
use super::*;

pub(super) async fn run_backtest(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let symbol = body
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("ETHUSDT");
    let quote = body
        .get("quote_currency")
        .and_then(Value::as_str)
        .unwrap_or("USDT");
    let starting: rust_decimal::Decimal = body
        .get("starting_balance")
        .and_then(Value::as_str)
        .unwrap_or("10000")
        .parse()
        .map_err(|_| AppError::bad_request("虚拟账户资金无效。"))?;
    let risk: rust_decimal::Decimal = body
        .get("risk_limit")
        .and_then(Value::as_str)
        .unwrap_or("10")
        .parse()
        .map_err(|_| AppError::bad_request("风险额度无效。"))?;
    // Legacy API contract: a date-scoped request uses stored, issued strategy
    // packages and contiguous local catalog evidence, never injected targets.
    if body.get("bars").is_none() {
        let start = body
            .get("start")
            .and_then(Value::as_str)
            .and_then(parse_time_ms)
            .ok_or_else(|| AppError::bad_request("开始时间无效。"))?;
        let end = body
            .get("end")
            .and_then(Value::as_str)
            .and_then(parse_time_ms)
            .ok_or_else(|| AppError::bad_request("结束时间无效。"))?;
        let interval = body.get("interval").and_then(Value::as_str).unwrap_or("1m");
        let product = body
            .get("product")
            .and_then(Value::as_str)
            .unwrap_or("futures_um");
        return Ok(Json(
            state
                .runtime
                .run_issued_catalog_backtest(
                    &tenant_id,
                    &account_id,
                    product,
                    symbol,
                    interval,
                    start,
                    end,
                    starting,
                    quote,
                )
                .map_err(AppError::from)?,
        ));
    }
    let raw_bars = body
        .get("bars")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let bars = raw_bars
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let close = row.get("close")?.as_str()?.parse().ok()?;
            let open_time_ms = row
                .get("open_time_ms")
                .and_then(Value::as_i64)
                .unwrap_or(index as i64 * 60_000);
            Some(seealpha_backtest::HistoryBar {
                open_time_ms,
                close,
            })
        })
        .collect::<Vec<_>>();
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|row| {
            Some(seealpha_backtest::PublishedTarget {
                member_id: row
                    .get("member_id")
                    .and_then(Value::as_str)
                    .unwrap_or("strategy")
                    .to_string(),
                bar_index: row.get("bar_index")?.as_u64()? as usize,
                quantity: row.get("quantity")?.as_str()?.parse().ok()?,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        state
            .runtime
            .run_account_backtest(
                &tenant_id,
                &account_id,
                symbol,
                quote,
                starting,
                risk,
                &bars,
                &targets,
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn latest_backtest(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .latest_history_backtest(&tenant_id, &account_id)
            .map_err(AppError::from)?,
    ))
}


pub(super) async fn run_carry(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let symbol = body
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("ETHUSDT");
    if body.get("start_ms").is_some() || body.get("start").is_some() {
        return Err(AppError::conflict("真实历史证据回测暂未提供；不会使用模拟窗口替代指定历史区间。"));
    }
    Ok(Json(
        state
            .runtime
            .run_carry_window(
                &tenant_id,
                &account_id,
                symbol,
                window_count(&body)?,
                decimal_field(&body, "starting_spot_balance", "1000")?,
                decimal_field(&body, "starting_perpetual_balance", "1000")?,
                decimal_field(&body, "max_gross_notional", "1000")?,
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn latest_carry(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .latest_kind(
                &tenant_id,
                &account_id,
                "carry",
                "还没有资金费组合回测结果。",
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn run_protected(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    if body.get("start_ms").is_some() || body.get("start").is_some() {
        return Err(AppError::conflict("真实历史证据回测暂未提供；不会使用模拟窗口替代指定历史区间。"));
    }
    Ok(Json(
        state
            .runtime
            .run_protected_window(
                &tenant_id,
                &account_id,
                window_count(&body)?,
                decimal_field(&body, "starting_options_balance", "1000")?,
                decimal_field(&body, "starting_perpetual_balance", "1000")?,
                decimal_field(&body, "max_gross_notional", "1000")?,
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn latest_protected(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .latest_kind(
                &tenant_id,
                &account_id,
                "protected_option",
                "还没有期权保护组合回测结果。",
            )
            .map_err(AppError::from)?,
    ))
}

