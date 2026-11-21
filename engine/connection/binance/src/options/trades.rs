//! Signed, bounded Binance Options trade pagination; seealpha-binance-options
//! owns the Options-only trade normalization and evidence checks.
use crate::{Credentials, VenueClient, VenueError, check_credentials, venue_symbol};
use seealpha_binance_options::parse_trade_id;
use serde_json::Value;
use std::collections::BTreeSet;

fn digits(value: &Value) -> Result<u64, VenueError> {
    parse_trade_id(value).map_err(|_| VenueError::Unconfirmed)
}

/// Temporary compatibility surface while Runtime and external callers still
/// depend on the existing seealpha_venue::option_trades public API.
/// The actual Options-only parsing and validation lives in binance-options.
pub fn normalize_option_trades(
    symbol: &str,
    order_id: &str,
    rows: &[Value],
) -> Result<Vec<Value>, VenueError> {
    seealpha_binance_options::normalize_option_trades(symbol, order_id, rows)
        .map_err(|_| VenueError::Unconfirmed)
}

impl VenueClient {
    /// Read oldest-to-newest with bounded pagination, never silently treat a
    /// saturated or truncated page as a complete trade history.
    pub fn option_order_trades(
        &self,
        credentials: &Credentials,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<Value>, VenueError> {
        check_credentials(credentials)?;
        let canonical = venue_symbol(symbol)?;
        let _ = order_id
            .parse::<u64>()
            .map_err(|_| VenueError::Unconfirmed)?;
        let mut from_id = 0u64;
        let mut all = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..32 {
            let reply = self.signed(
                credentials,
                "GET",
                "/eapi/v1/userTrades",
                &[
                    ("symbol".into(), canonical.clone()),
                    ("fromId".into(), from_id.to_string()),
                    ("limit".into(), "1000".into()),
                ],
            )?;
            let rows = reply.as_array().ok_or(VenueError::Unconfirmed)?;
            let mut greatest = None;
            for row in rows {
                let id = digits(row.get("id").ok_or(VenueError::Unconfirmed)?)?;
                if id < from_id || !seen.insert(id) {
                    return Err(VenueError::Unconfirmed);
                }
                greatest = Some(greatest.map_or(id, |old: u64| old.max(id)));
                all.push(row.clone());
            }
            if rows.len() < 1000 {
                return normalize_option_trades(&canonical, order_id, &all);
            }
            from_id = greatest
                .ok_or(VenueError::Unconfirmed)?
                .checked_add(1)
                .ok_or(VenueError::Unconfirmed)?;
        }
        Err(VenueError::Unconfirmed)
    }
}
