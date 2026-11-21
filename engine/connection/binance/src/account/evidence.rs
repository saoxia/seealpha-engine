//! Read-only Binance evidence replacing the legacy Python connector helpers.
//! No order, transfer, leverage, or margin mutation is exposed here.
use crate::{Credentials, VenueClient, VenueError, check_credentials, route_gateway, venue_symbol};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::Read;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

const DAY_MS: u64 = 86_400_000;
const PAGE_SIZE: usize = 1000;

fn invalid() -> VenueError { VenueError::Unconfirmed }
fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|v| v.as_millis() as u64).unwrap_or(0)
}
fn finite(value: &Value) -> Result<String, VenueError> {
    let s = value.as_str().ok_or_else(invalid)?;
    Decimal::from_str(s).map_err(|_| invalid())?;
    Ok(s.to_owned())
}
fn identity(value: &Value, max: usize) -> Result<&str, VenueError> {
    let s = value.as_str().ok_or_else(invalid)?;
    if s.is_empty() || s.len() > max || !s.is_ascii() { return Err(invalid()); }
    Ok(s)
}
fn nonnegative(value: &Value) -> Result<String, VenueError> {
    let s = finite(value)?;
    if Decimal::from_str(&s).map_err(|_| invalid())?.is_sign_negative() {
        return Err(invalid());
    }
    Ok(s)
}

impl VenueClient {
    /// Raw signed USD-M wallet response, limited to the fields consumed by valuation.
    pub fn asset_account_evidence(&self, credentials: &Credentials) -> Result<Value, VenueError> {
        check_credentials(credentials)?;
        let raw = self.signed(credentials, "GET", "/fapi/v2/account", &[])?;
        let multi = raw.get("multiAssetsMargin").and_then(Value::as_bool).ok_or_else(invalid)?;
        let rows = raw.get("assets").and_then(Value::as_array).ok_or_else(invalid)?;
        let mut assets = serde_json::Map::new();
        for item in rows {
            let currency = identity(&item["asset"], 30)?;
            if !currency.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                || assets.contains_key(currency) { return Err(invalid()); }
            assets.insert(currency.to_owned(), json!({
                "walletBalance": finite(&item["walletBalance"])?,
                "unrealizedProfit": finite(&item["unrealizedProfit"])?,
                "maintMargin": nonnegative(&item["maintMargin"])?,
            }));
        }
        let mut positions = Vec::new();
        let mut seen = BTreeSet::new();
        for item in raw.get("positions").and_then(Value::as_array).ok_or_else(invalid)? {
            let amount = finite(&item["positionAmt"])?;
            if Decimal::from_str(&amount).map_err(|_| invalid())?.is_zero() { continue; }
            let symbol = identity(&item["symbol"], 40)?;
            let side = identity(&item["positionSide"], 8)?;
            if !matches!(side, "BOTH" | "LONG" | "SHORT")
                || !seen.insert(format!("{symbol}:{side}")) { return Err(invalid()); }
            positions.push(json!({
                "symbol": symbol, "position_side": side, "quantity": amount,
                "entry_price": finite(&item["entryPrice"])?,
            }));
        }
        Ok(json!({
            "multi_assets": multi,
            "assets": assets,
            "positions": positions,
            "totals": {
                "total_wallet_balance": finite(&raw["totalWalletBalance"])?,
                "total_unrealized_profit": finite(&raw["totalUnrealizedProfit"])?,
                "total_margin_balance": finite(&raw["totalMarginBalance"])?,
                "total_maint_margin": nonnegative(&raw["totalMaintMargin"])?,
            },
            "observed_at_ms": now_ms(),
        }))
    }

    /// Read the account's current hedge/one-way mode, never change it.
    pub fn position_mode(&self, credentials: &Credentials) -> Result<&'static str, VenueError> {
        check_credentials(credentials)?;
        let mode = self.signed(credentials, "GET", "/fapi/v1/positionSide/dual", &[])?;
        match mode["dualSidePosition"].as_bool() {
            Some(true) => Ok("hedge"),
            Some(false) => Ok("oneway"),
            None => Err(invalid()),
        }
    }

    /// Display-only position risk evidence; never inferred from account balances.
    pub fn position_risk(
        &self, credentials: &Credentials, symbol: &str,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        let symbol = venue_symbol(symbol)?;
        let raw = self.signed(credentials, "GET", "/fapi/v3/positionRisk",
            &[("symbol".into(), symbol.clone())])?;
        let rows = raw.as_array().ok_or_else(invalid)?;
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for row in rows {
            if row["symbol"].as_str() != Some(&symbol) { return Err(invalid()); }
            let side = identity(&row["positionSide"], 8)?;
            if !matches!(side, "BOTH" | "LONG" | "SHORT") || !seen.insert(side) {
                return Err(invalid());
            }
            out.push(json!({
                "symbol": symbol, "position_side": side,
                "quantity": finite(&row["positionAmt"])?,
                "entry_price": finite(&row["entryPrice"])?,
                "break_even_price": finite(&row["breakEvenPrice"])?,
                "liquidation_price": finite(&row["liquidationPrice"])?,
            }));
        }
        Ok(out)
    }

    /// Inclusive USD-M income history. Saturated pages are split by millisecond.
    /// A saturated single millisecond fails closed rather than pretending coverage.
    pub fn income_history(
        &self, credentials: &Credentials, start_ms: u64, end_ms: u64,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        if start_ms == 0 || start_ms > end_ms
            || end_ms - start_ms > 87 * DAY_MS
            || end_ms > now_ms().saturating_add(60_000) {
            return Err(VenueError::Rejected("Invalid income history window".into()));
        }
        let mut output = Vec::new();
        let mut cursor = start_ms;
        while cursor <= end_ms {
            let end = end_ms.min(cursor.saturating_add(7 * DAY_MS - 1));
            let mut pending = vec![(cursor, end)];
            while let Some((first, last)) = pending.pop() {
                let raw = self.signed(credentials, "GET", "/fapi/v1/income", &[
                    ("startTime".into(), first.to_string()),
                    ("endTime".into(), last.to_string()),
                    ("limit".into(), PAGE_SIZE.to_string()),
                ])?;
                let rows = raw.as_array().ok_or_else(invalid)?;
                if rows.len() > PAGE_SIZE { return Err(invalid()); }
                let mut page = Vec::new();
                let mut ids = BTreeSet::new();
                for row in rows {
                    let timestamp = row["time"].as_u64().ok_or_else(invalid)?;
                    if !(first..=last).contains(&timestamp) { return Err(invalid()); }
                    let kind = identity(&row["incomeType"], 80)?;
                    let asset = identity(&row["asset"], 30)?;
                    if !asset.bytes().all(|b| b.is_ascii_alphanumeric()) { return Err(invalid()); }
                    let transaction = row["tranId"].as_str().map(str::to_owned)
                        .or_else(|| row["tranId"].as_u64().map(|n| n.to_string()))
                        .ok_or_else(invalid)?;
                    if transaction.is_empty() || transaction.len() > 80
                        || !transaction.bytes().all(|b| b.is_ascii_digit())
                        || !ids.insert((kind.to_owned(), transaction.clone())) { return Err(invalid()); }
                    page.push(json!({
                        "event_id": format!("binance-income:{kind}:{transaction}"),
                        "timestamp": timestamp, "income_type": kind,
                        "asset": asset, "amount": finite(&row["income"])?,
                        "transaction_id": transaction,
                        "symbol": row["symbol"].as_str().unwrap_or(""),
                        "trade_id": row["tradeId"], "info": row["info"],
                        "evidence": "venue", "status": "confirmed",
                    }));
                }
                if rows.len() == PAGE_SIZE {
                    if first == last { return Err(invalid()); }
                    let middle = first + (last - first) / 2;
                    pending.push((middle + 1, last));
                    pending.push((first, middle));
                } else {
                    output.push(json!({"start_ms": first, "end_ms": last, "records": page}));
                }
            }
            if end == end_ms { break; }
            cursor = end + 1;
        }
        Ok(output)
    }

    /// Public USD index price, distinct from a futures market quote.
    pub fn asset_index(&self, currency: &str) -> Result<Value, VenueError> {
        if currency.is_empty() || currency.len() > 30
            || !currency.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()) {
            return Err(VenueError::Symbol);
        }
        let symbol = format!("{currency}USD");
        let base = route_gateway(&self.base, "/fapi/v1/assetIndex")?;
        let response = self.http.get(format!("{base}/fapi/v1/assetIndex"))
            .query(&[("symbol", symbol.as_str())]).send().map_err(|_| invalid())?;
        if !response.status().is_success() { return Err(invalid()); }
        let mut buf = Vec::new();
        response.take(131_073).read_to_end(&mut buf).map_err(|_| invalid())?;
        if buf.len() > 131_072 { return Err(invalid()); }
        let raw: Value = serde_json::from_slice(&buf).map_err(|_| invalid())?;
        if raw["symbol"].as_str() != Some(symbol.as_str()) { return Err(invalid()); }
        let stamp = raw["time"].as_u64().ok_or_else(invalid)?;
        if now_ms().abs_diff(stamp) > 120_000 { return Err(invalid()); }
        let price = finite(&raw["index"])?;
        if Decimal::from_str(&price).map_err(|_| invalid())?.is_sign_negative()
            || price.parse::<Decimal>().map_err(|_| invalid())?.is_zero() { return Err(invalid()); }
        Ok(json!({"price": price, "priced_at": stamp / 1000}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_currency_is_rejected_without_network() {
        let venue = VenueClient::new("http://127.0.0.1:19000").unwrap();
        assert!(matches!(venue.asset_index("../wallet"), Err(VenueError::Symbol)));
    }
    #[test]
    fn decimal_evidence_rejects_non_numeric_values() {
        assert!(finite(&json!("oops")).is_err());
        assert!(nonnegative(&json!("-1")).is_err());
        assert_eq!(finite(&json!("0.000001")).unwrap(), "0.000001");
    }
}
