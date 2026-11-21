//! History, carry, option and candidate backtest HTTP handlers.
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
        let from = body["start_ms"]
            .as_i64()
            .or_else(|| body["start"].as_str().and_then(parse_time_ms))
            .ok_or_else(|| AppError::bad_request("开始时间无效。"))?;
        let until = body["end_ms"]
            .as_i64()
            .or_else(|| body["end"].as_str().and_then(parse_time_ms))
            .ok_or_else(|| AppError::bad_request("结束时间无效。"))?;
        let fee = body["fee_rate"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("必须明确设置模拟手续费费率。"))?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("手续费费率无效。"))?;
        let spot = body["spot_qty"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("现货目标数量无效。"))?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("现货目标数量无效。"))?;
        let perp = body["perp_qty"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("永续目标数量无效。"))?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("永续目标数量无效。"))?;
        return Ok(Json(
            state
                .runtime
                .run_evidence_carry_backtest(
                    &tenant_id,
                    &account_id,
                    symbol,
                    body["interval"].as_str().unwrap_or("1m"),
                    from,
                    until,
                    spot,
                    perp,
                    decimal_field(&body, "starting_balance", "10000")?,
                    decimal_field(&body, "max_gross_notional", "10000")?,
                    fee,
                )
                .map_err(AppError::from)?,
        ));
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
        let start = body["start_ms"]
            .as_i64()
            .or_else(|| body["start"].as_str().and_then(parse_time_ms))
            .ok_or_else(|| AppError::bad_request("开始时间无效。"))?;
        let end = body["end_ms"]
            .as_i64()
            .or_else(|| body["end"].as_str().and_then(parse_time_ms))
            .ok_or_else(|| AppError::bad_request("结束时间无效。"))?;
        let option_symbol = body["option_symbol"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("期权合约无效。"))?;
        let perp_symbol = body["perp_symbol"]
            .as_str()
            .ok_or_else(|| AppError::bad_request("永续合约无效。"))?;
        if body["fee_rate"].as_str().is_none() || body["option_multiplier"].as_str().is_none() {
            return Err(AppError::bad_request("必须指定模拟手续费及期权合约乘数。"));
        }
        return Ok(Json(
            state
                .runtime
                .run_evidence_protected_backtest(
                    &tenant_id,
                    &account_id,
                    option_symbol,
                    perp_symbol,
                    body["interval"].as_str().unwrap_or("1m"),
                    start,
                    end,
                    decimal_field(&body, "option_qty", "0")?,
                    decimal_field(&body, "option_multiplier", "0")?,
                    decimal_field(&body, "perp_qty", "0")?,
                    decimal_field(&body, "starting_balance", "10000")?,
                    decimal_field(&body, "max_gross_notional", "10000")?,
                    decimal_field(&body, "fee_rate", "0")?,
                )
                .map_err(AppError::from)?,
        ));
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

pub(super) async fn run_candidate(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("candidate");
    Ok(Json(
        state
            .runtime
            .run_candidate_backtest(
                &tenant_id,
                &account_id,
                name,
                decimal_field(&body, "ending_balance", "0")?,
            )
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn cancel_candidate(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, AppError> {
    let name = query.get("name").map(String::as_str).unwrap_or("candidate");
    Ok(Json(
        state
            .runtime
            .cancel_candidate(&tenant_id, &account_id, name)
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn latest_candidate(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .latest_kind(&tenant_id, &account_id, "candidate", "还没有候选回测")
            .map_err(AppError::from)?,
    ))
}
