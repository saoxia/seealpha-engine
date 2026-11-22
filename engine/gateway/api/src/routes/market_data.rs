//! Catalog bar ingress.
use super::*;

pub(super) async fn import_bars(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    let product = body["product"].as_str().unwrap_or("spot");
    let symbol = body["symbol"].as_str().unwrap_or("");
    let interval = body["interval"].as_str().unwrap_or("1m");
    let _ = seealpha_market::Product::parse(product)
        .map_err(|e| AppError::bad_request(e.to_string()))?;
    seealpha_market::check_symbol(symbol).map_err(|e| AppError::bad_request(e.to_string()))?;
    let step =
        seealpha_market::interval_ms(interval).map_err(|e| AppError::bad_request(e.to_string()))?;
    let rows = body["bars"]
        .as_array()
        .ok_or_else(|| AppError::bad_request("历史 K 线数组无效。"))?;
    if rows.is_empty() || rows.len() > 5000 {
        return Err(AppError::bad_request("历史 K 线数量超出范围。"));
    }
    let mut evidence = Vec::with_capacity(rows.len());
    let mut canonical = Vec::with_capacity(rows.len());
    for row in rows {
        let text = |field: &str| -> Result<&str, AppError> {
            row[field]
                .as_str()
                .ok_or_else(|| AppError::bad_request("K 线 Decimal 字段无效。"))
        };
        let start = row["open_time_ms"]
            .as_i64()
            .ok_or_else(|| AppError::bad_request("K 线开始时间无效。"))?;
        let open = text("open")?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("K 线价格无效。"))?;
        let high = text("high")?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("K 线价格无效。"))?;
        let low = text("low")?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("K 线价格无效。"))?;
        let close = text("close")?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("K 线价格无效。"))?;
        let volume = text("volume")?
            .parse::<rust_decimal::Decimal>()
            .map_err(|_| AppError::bad_request("K 线成交量无效。"))?;
        if open <= rust_decimal::Decimal::ZERO
            || high <= rust_decimal::Decimal::ZERO
            || low <= rust_decimal::Decimal::ZERO
            || close <= rust_decimal::Decimal::ZERO
        {
            return Err(AppError::bad_request("K 线价格必须大于零。"));
        }
        let close_ms = start
            .checked_add(step)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| AppError::bad_request("K 线时间溢出。"))?;
        canonical.push(seealpha_market::Bar {
            open_time_ms: start,
            close_time_ms: close_ms,
            open,
            high,
            low,
            close,
            volume,
        });
        evidence.push(seealpha_runtime::CatalogBar {
            open_time_ms: start,
            open: open.to_string(),
            high: high.to_string(),
            low: low.to_string(),
            close: close.to_string(),
            volume: volume.to_string(),
        });
    }
    seealpha_market::validate_bars(&canonical, step)
        .map_err(|e| AppError::bad_request(e.to_string()))?;
    // Parquet is the established Python Pipeline storage contract.
    // A mounted Catalog is authoritative; never silently write a separate
    // SQLite copy that backtests cannot see.
    let native = state
        .runtime
        .market_data_backend()
        .map_err(AppError::from)?
        == "nautilus_v2_parquet";
    // Python derives the precision from authoritative Instrument metadata.
    // The Rust importer refuses to invent it for a native Parquet publication.
    if native && (body.get("price_precision").is_none() || body.get("size_precision").is_none()) {
        return Err(AppError::bad_request(
            "写入 Nautilus Parquet 必须传入经 Instrument 验证的精度。",
        ));
    }
    let price_precision = body["price_precision"]
        .as_u64()
        .and_then(|x| u32::try_from(x).ok())
        .unwrap_or(8);
    let size_precision = body["size_precision"]
        .as_u64()
        .and_then(|x| u32::try_from(x).ok())
        .unwrap_or(8);
    let imported = state
        .runtime
        .commit_market_bars(
            &tenant_id,
            product,
            symbol,
            interval,
            &evidence,
            price_precision,
            size_precision,
        )
        .map_err(AppError::from)?;
    Ok(Json(imported))
}
