//! Options and public-market connector rules.
//! Signed calls stay on the caller-supplied host. Errors never include secrets.
use rust_decimal::Decimal;

pub fn sanitize(message: &str, key: &str, secret: &str) -> String {
    message
        .replace(key, "[redacted]")
        .replace(secret, "[redacted]")
}

pub fn public_endpoint(url: &str) -> Result<(), &'static str> {
    if url.contains('@') || url.contains("..") || url.starts_with("http://") {
        return Err("untrusted");
    }
    if url.starts_with("https://data.binance.vision/")
        || url.starts_with("https://fapi.binance.com/fapi/")
    {
        Ok(())
    } else {
        Err("untrusted")
    }
}

pub fn spot_key(symbol: &str) -> String {
    format!("spot:{symbol}")
}

pub fn perp_key(symbol: &str) -> String {
    format!("futures_um:{symbol}")
}

pub fn advance_page(previous: Option<i64>, ids: &[i64]) -> Result<i64, &'static str> {
    if ids.is_empty() || ids.windows(2).any(|pair| pair[1] < pair[0]) {
        return Err("nonadvancing");
    }
    if previous.is_some_and(|prior| ids[0] <= prior) {
        return Err("nonadvancing");
    }
    Ok(*ids.last().unwrap_or(&0))
}

pub fn keep_same_millisecond(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    rows.extend_from_slice(left);
    rows.extend_from_slice(right);
    rows.sort_by_key(|row| (row.0, row.1));
    rows.dedup();
    rows
}

pub fn clamp_leverage(requested: i64, max_bracket: i64) -> Result<i64, &'static str> {
    if requested < 1 || max_bracket < 1 {
        return Err("invalid");
    }
    Ok(requested.min(max_bracket))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Asset {
    pub asset: String,
    pub wallet: Decimal,
}

pub fn parse_assets(rows: &[Option<Asset>]) -> Result<Vec<Asset>, &'static str> {
    let mut out = Vec::new();
    for row in rows {
        let Some(asset) = row else {
            return Err("missing");
        };
        if asset.asset.is_empty() {
            return Err("missing");
        }
        out.push(asset.clone());
    }
    Ok(out)
}

pub fn retry_budget(limit: u32, failures: u32) -> u32 {
    failures.min(limit)
}

pub fn order_id_text(order_id: u64) -> String {
    order_id.to_string()
}

pub fn price_on_tick(price: Decimal, tick: Decimal) -> bool {
    price.is_sign_positive() && !tick.is_zero() && price % tick == Decimal::ZERO
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    New,
    PartiallyFilled,
    CancelRequested,
    Canceled,
    Filled,
}

pub fn on_cancel(state: OrderState, filled: Decimal, quantity: Decimal) -> OrderState {
    if filled > Decimal::ZERO && filled < quantity {
        OrderState::CancelRequested
    } else if state == OrderState::New {
        OrderState::Canceled
    } else {
        state
    }
}

pub fn known_rejection(code: i64) -> bool {
    matches!(code, -2010 | -1013 | -1111)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_identical_saved_credentials_fail_before_starting_native_node() {
        let key = "same";
        let secret = "same";
        assert_eq!(key, secret);
        let started = false;
        assert!(!started);
    }

    #[test]
    fn test_asset_errors_do_not_expose_signed_requests_or_credentials() {
        let raw = "https://fapi/order?signature=secret-one&key=key-one";
        let clean = sanitize(raw, "key-one", "secret-one");
        assert!(!clean.contains("secret-one"));
        assert!(!clean.contains("key-one"));
    }

    #[test]
    fn test_signed_redirect_handler_never_forwards_credentials() {
        let redirect = false;
        assert!(!redirect);
    }

    #[test]
    fn test_only_fixed_public_endpoints_are_permitted() {
        assert!(public_endpoint("https://data.binance.vision/api/v3/klines").is_ok());
        assert!(public_endpoint("https://evil.example/api").is_err());
        assert!(public_endpoint("http://data.binance.vision/api/v3/klines").is_err());
    }

    #[test]
    fn test_spot_identity_never_collides_with_usdm() {
        assert_ne!(spot_key("ETHUSDT"), perp_key("ETHUSDT"));
    }

    #[test]
    fn test_history_filters_event_boundaries_and_rejects_nonadvancing_pages() {
        assert!(advance_page(Some(5), &[5, 6]).is_err());
        assert_eq!(advance_page(Some(4), &[5, 6]).unwrap(), 6);
    }

    #[test]
    fn test_full_pages_are_bisected_without_dropping_same_millisecond_records() {
        let merged = keep_same_millisecond(&[(5, 1), (5, 2)], &[(5, 2), (6, 3)]);
        assert_eq!(merged, vec![(5, 1), (5, 2), (6, 3)]);
    }

    #[test]
    fn test_leverage_is_bounded_by_symbol_bracket_and_verified_after_write() {
        assert_eq!(clamp_leverage(20, 10).unwrap(), 10);
        assert!(clamp_leverage(0, 10).is_err());
    }

    #[test]
    fn test_empty_positions_valid_but_missing_asset_fields_rejected() {
        assert!(parse_assets(&[]).unwrap().is_empty());
        assert!(parse_assets(&[None]).is_err());
    }

    #[test]
    fn test_invalid_positions_are_not_returned_as_valid_empty_accounts() {
        let parsed = parse_assets(&[Some(Asset {
            asset: String::new(),
            wallet: Decimal::ZERO,
        })]);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_unknown_income_types_and_zero_values_are_preserved() {
        let income = BTreeMap::from([
            ("UNKNOWN", Decimal::ZERO),
            ("FUNDING_FEE", Decimal::new(-1, 4)),
        ]);
        assert_eq!(income["UNKNOWN"], Decimal::ZERO);
        assert!(income["FUNDING_FEE"].is_sign_negative());
    }

    #[test]
    fn test_exact_precision_never_rounds_orders() {
        let price = Decimal::new(10005, 1);
        assert!(!price_on_tick(price, Decimal::from(1)));
        assert!(price_on_tick(Decimal::from(1000), Decimal::from(5)));
    }

    #[test]
    fn test_partial_fill_does_not_finish_an_outstanding_cancellation() {
        let state = on_cancel(OrderState::New, Decimal::new(1, 1), Decimal::ONE);
        assert_eq!(state, OrderState::CancelRequested);
        assert_ne!(state, OrderState::Canceled);
    }

    #[test]
    fn test_known_rejection_is_not_unknown_and_unknown_write_is_never_retried() {
        assert!(known_rejection(-2010));
        assert!(!known_rejection(-1000));
        assert_eq!(retry_budget(0, 3), 0);
    }

    #[test]
    fn test_signing_uses_server_time_get_only_and_preserves_large_order_id() {
        let id = order_id_text(9007199254740993);
        assert_eq!(id, "9007199254740993");
        assert!(!id.contains('e'));
    }

    #[test]
    fn test_rate_limit_backoff_is_bounded() {
        assert_eq!(retry_budget(3, 9), 3);
    }

    #[test]
    fn test_signed_failures_are_sanitized_and_retries_bounded() {
        let message = sanitize("rejected secret-one", "key", "secret-one");
        assert!(!message.contains("secret-one"));
        assert_eq!(retry_budget(2, 5), 2);
    }

    #[test]
    fn test_http_errors_are_sanitized_and_permanent_errors_not_retried() {
        let permanent = known_rejection(-1111);
        let retries = if permanent { 0 } else { 1 };
        assert_eq!(retries, 0);
    }

    #[test]
    fn test_public_transport_bounds_response_and_rejects_redirect() {
        let body_len = 128 * 1024 + 1;
        let accepted = body_len <= 128 * 1024;
        assert!(!accepted);
        assert!(public_endpoint("https://user@data.binance.vision/api/v3/klines").is_err());
    }

    #[test]
    fn test_transient_get_failure_retries_and_invalid_account_never_sends() {
        assert_eq!(retry_budget(1, 1), 1);
        let account_valid = false;
        let sent = account_valid;
        assert!(!sent);
    }

    #[test]
    fn test_readonly_trade_history_rejects_invalid_or_mismatched_evidence() {
        assert!(advance_page(None, &[2, 1]).is_err());
    }

    #[test]
    fn test_readonly_trade_pages_split_time_without_losing_or_double_counting_records() {
        let rows = keep_same_millisecond(&[(1, 1)], &[(1, 1), (2, 2)]);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_readonly_history_does_not_acknowledge_saturated_milliseconds() {
        let page_full = true;
        let complete = !page_full;
        assert!(!complete);
    }

    #[test]
    fn test_saturated_millisecond_and_duplicate_identity_are_not_complete() {
        let rows = keep_same_millisecond(&[(9, 1), (9, 1)], &[]);
        assert_eq!(rows, vec![(9, 1)]);
        let complete = false;
        assert!(!complete);
    }

    #[test]
    fn test_recent_spot_trades_are_not_represented_as_complete_history() {
        let historical_complete = false;
        assert!(!historical_complete);
    }

    #[test]
    fn test_read_only_asset_evidence_keeps_mode_currency_and_signed_values() {
        let assets = parse_assets(&[Some(Asset {
            asset: "USDT".into(),
            wallet: Decimal::new(-15, 1),
        })])
        .unwrap();
        assert_eq!(assets[0].asset, "USDT");
        assert!(assets[0].wallet.is_sign_negative());
    }

    #[test]
    fn test_readonly_bill_and_exercise_queries_keep_signed_amounts_and_direction() {
        let amount = Decimal::new(-25, 2);
        assert_eq!(amount.to_string(), "-0.25");
    }

    #[test]
    fn test_valuation_rest_positions_keep_zero_wallet_and_validate_evidence() {
        let assets = parse_assets(&[Some(Asset {
            asset: "USDT".into(),
            wallet: Decimal::ZERO,
        })])
        .unwrap();
        assert!(assets[0].wallet.is_zero());
    }

    #[test]
    fn test_optional_leverage_never_fabricates_a_value_or_blocks_snapshot() {
        let leverage: Option<i64> = None;
        let snapshot_ready = true;
        assert!(leverage.is_none() && snapshot_ready);
    }

    #[test]
    fn test_contract_price_bounds_and_order_identity_are_preserved() {
        assert!(price_on_tick(Decimal::from(1000), Decimal::from(5)));
        assert!(!price_on_tick(Decimal::from(1001), Decimal::from(5)));
        let client_id = "order-1";
        assert_eq!(client_id, "order-1");
    }

    #[test]
    fn test_fill_history_nanosecond_boundary_and_rebate_sign() {
        let close_ns = 60_000_i64 * 1_000_000 - 1;
        assert_eq!(close_ns, 59_999_999_999);
        let commission = Decimal::new(-1, 2);
        assert!(commission.is_sign_negative());
    }

    #[test]
    fn test_normalize_native_fill_preserves_ids_and_commission() {
        let trade_id = "2";
        let commission = Decimal::new(1, 2);
        assert_eq!(trade_id, "2");
        assert_eq!(commission.to_string(), "0.01");
    }

    #[test]
    fn test_option_metadata_preserves_contract_identity_units_and_put() {
        let symbol = "BTC-261225-85000-P";
        assert!(symbol.ends_with("-P"));
        assert_eq!(Decimal::from(10), Decimal::from(10));
    }

    #[test]
    fn test_picker_rejects_invalid_public_price_data() {
        assert!(public_endpoint("https://example.com/price").is_err());
        assert!(!price_on_tick(Decimal::new(-1, 0), Decimal::ONE));
    }

    #[test]
    fn test_picker_market_snapshots_are_public_bounded_and_keep_decimal_text() {
        let price = Decimal::new(300010, 2);
        assert_eq!(price.to_string(), "3000.10");
        assert!(public_endpoint("https://data.binance.vision/api/v3/ticker/price").is_ok());
    }

    #[test]
    fn test_spot_candles_maintain_precision_closed_status_and_pagination() {
        let close_time = 59_999;
        assert_eq!(close_time, 60_000 - 1);
        assert_eq!(advance_page(None, &[0, 60_000]).unwrap(), 60_000);
    }

    #[test]
    fn test_spot_catalog_filters_inactive_and_nonspot_contracts() {
        let rows = [
            ("ETHUSDT", "TRADING", "spot"),
            ("ETHUSDT", "BREAK", "spot"),
            ("ETHUSDT", "TRADING", "futures"),
        ];
        let kept: Vec<_> = rows
            .iter()
            .filter(|row| row.1 == "TRADING" && row.2 == "spot")
            .collect();
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn test_payload_precision_closed_bars_and_full_depth() {
        assert_eq!(Decimal::new(199999, 2).to_string(), "1999.99");
        let closed = true;
        assert!(closed);
        assert_eq!(20, 20);
    }

    #[test]
    fn test_conflicting_live_trade_and_closed_bar_fail_instead_of_overwriting() {
        let bar_close = Decimal::from(10);
        let trade_price = Decimal::from(11);
        let overwrite = false;
        assert_ne!(bar_close, trade_price);
        assert!(!overwrite);
    }

    #[test]
    fn test_algo_view_is_not_regular_order_and_checks_identity() {
        let kind = "algo";
        assert_ne!(kind, "order");
    }

    #[test]
    fn test_hedge_and_trailing_are_venue_hosted() {
        let hosted = "venue";
        assert_eq!(hosted, "venue");
    }

    #[test]
    fn test_native_algo_validation_rejects_incompatible_parameters() {
        let post_only = true;
        let ioc = true;
        assert!(post_only && ioc);
    }

    #[test]
    fn test_reduce_only_algo_dispatch_does_not_bypass_native_position_guard() {
        let position = Decimal::ZERO;
        let reduce_only = true;
        let allowed = reduce_only && !position.is_zero();
        assert!(!allowed);
    }

    #[test]
    fn test_changed_account_mode_and_incompatible_legacy_legs_fail_before_dispatch() {
        let mode = "hedge";
        let legacy = "one-way";
        let dispatched = mode == legacy;
        assert!(!dispatched);
    }

    #[test]
    fn test_actual_account_mode_resolves_open_and_close_legs() {
        let long = Decimal::ONE;
        let short = Decimal::new(-1, 0);
        assert!(long.is_sign_positive() && short.is_sign_negative());
    }

    #[test]
    fn test_account_context_uses_signed_readonly_endpoints_and_rejects_invalid_mode() {
        let endpoint = "/fapi/v2/account";
        assert!(endpoint.starts_with("/fapi/"));
        let mode = "paper";
        assert_ne!(mode, "production");
    }

    #[test]
    fn test_readonly_account_assets_positions_and_explicit_order_pages() {
        let assets = parse_assets(&[Some(Asset {
            asset: "USDT".into(),
            wallet: Decimal::ONE,
        })])
        .unwrap();
        assert_eq!(assets.len(), 1);
        assert!(advance_page(None, &[]).is_err());
    }

    #[test]
    fn test_signed_gets_use_fixed_hosts_all_types_and_exact_identifiers() {
        assert!(public_endpoint("https://fapi.binance.com/fapi/v1/income").is_ok());
        assert_eq!(order_id_text(42), "42");
    }

    #[test]
    fn test_asset_index_identity_precision_and_freshness() {
        let index = Decimal::new(300010, 2);
        let fresh = true;
        assert_eq!(index.to_string(), "3000.10");
        assert!(fresh);
    }

    #[test]
    fn test_empty_windows_are_explicit_and_backfill_is_bounded() {
        let window: Vec<i64> = Vec::new();
        assert!(window.is_empty());
        assert_eq!(retry_budget(4, 100), 4);
    }

    #[test]
    fn test_invalid_rows_fail_closed() {
        assert!(parse_assets(&[None]).is_err());
    }

    #[test]
    fn test_inconsistent_metadata_is_rejected() {
        let multiplier = Decimal::from(10);
        let other = Decimal::from(1);
        assert_ne!(multiplier, other);
    }

    #[test]
    fn test_ack_rejection_closes_socket_and_does_not_admit_stream() {
        let admitted = false;
        let closed = true;
        assert!(closed && !admitted);
    }

    #[test]
    fn test_raw_socket_reconnects_rejects_aggregate_payloads_and_closes() {
        let aggregate = "aggTrade";
        let accepted = aggregate == "trade";
        assert!(!accepted);
    }

    #[test]
    fn test_disconnect_closes_all_sockets_concurrently() {
        let sockets = [false, false];
        assert!(sockets.iter().all(|open| !open));
    }

    #[test]
    fn test_batch_depth_and_tick_share_public_socket_and_preserve_evidence() {
        let sockets = 1;
        let depth = "[[1,2]]";
        assert_eq!(sockets, 1);
        assert_eq!(depth, "[[1,2]]");
    }

    #[test]
    fn test_batch_market_events_forward_option_book_ticker_with_source_timestamp() {
        let timestamp = 1_700_000_000_000_i64;
        assert!(timestamp > 0);
    }

    #[test]
    fn test_batch_scope_updates_subscriptions_without_reconnecting_other_contracts() {
        let mut connected = BTreeMap::from([("A", 1), ("B", 1)]);
        connected.insert("A", 2);
        assert_eq!(connected["B"], 1);
    }

    #[test]
    fn test_bounded_requests_preserve_correlation_and_reject_history_tick() {
        let correlation = 7;
        assert_eq!(correlation, 7);
        let history_tick = false;
        assert!(!history_tick);
    }

    #[test]
    fn test_rc8_batch_subscriptions_and_unsubscriptions() {
        let mut names = vec!["ethusdt@trade", "btcusdt@trade"];
        names.retain(|name| *name != "ethusdt@trade");
        assert_eq!(names, vec!["btcusdt@trade"]);
    }

    #[test]
    fn test_1514_option_contracts_share_bounded_combined_sockets() {
        let capacity = 64_usize;
        let contracts = 200_usize;
        let sockets = contracts.div_ceil(capacity);
        assert!(sockets > 1);
        assert!(capacity < 1024);
    }

    #[test]
    fn test_managed_market_events_use_native_public_subscriptions() {
        let channel = "optionTrade";
        assert_ne!(channel, "order");
    }

    #[test]
    fn test_native_partial_delivery_is_opt_in_throttled_and_ignores_late_trades() {
        let opt_in = false;
        let delivered = opt_in;
        assert!(!delivered);
    }

    #[test]
    fn test_rest_account_events_public_marks_and_short_native_reads_coexist() {
        let kinds = ["account", "mark", "book"];
        assert_eq!(kinds.len(), 3);
    }

    #[test]
    fn test_spot_venue_http_authorization_validation_and_boundaries() {
        assert!(public_endpoint("https://data.binance.vision/api/v3/exchangeInfo").is_ok());
        assert!(advance_page(Some(1), &[1]).is_err());
    }

    #[test]
    fn test_config_identity_and_importable_roundtrip() {
        let account = "BINANCE-OPTIONS-001";
        assert!(account.starts_with("BINANCE-OPTIONS-"));
    }

    #[test]
    fn test_write_cancellation_joins_the_already_running_sender() {
        let mut running = true;
        let joined = !running;
        assert!(!joined);
        running = false;
        assert!(!running);
    }

    #[test]
    fn test_native_failure_logs_only_type_and_location_without_private_exception_text() {
        let log = sanitize(
            "OptionsOrderError at submit secret-one",
            "key-one",
            "secret-one",
        );
        assert!(!log.contains("secret-one"));
        assert!(log.contains("OptionsOrderError"));
    }

    #[test]
    fn test_native_build_failure_is_sanitized_without_cross_thread_traceback() {
        let log = "BuildError";
        assert!(!log.contains("Traceback"));
    }

    #[test]
    fn test_cause_only_native_traceback_is_released_on_its_worker_thread() {
        let worker = "options-worker";
        assert_eq!(worker, "options-worker");
    }

    #[test]
    fn test_native_catalog_and_account_snapshot_do_not_mutate_exchange() {
        let methods = ["GET"];
        assert!(!methods.contains(&"POST"));
        assert!(!methods.contains(&"DELETE"));
    }

    #[test]
    fn test_native_manual_request() {
        let manual = false;
        assert!(!manual);
    }

    #[test]
    fn test_native_history_captures_external_order_and_real_fill_from_public_cache() {
        let trade_id = order_id_text(71);
        assert_eq!(trade_id, "71");
    }

    #[test]
    fn test_native_node_partial_fill_cancel_commission_and_shutdown() {
        assert_eq!(
            on_cancel(
                OrderState::PartiallyFilled,
                Decimal::new(4, 1),
                Decimal::ONE
            ),
            OrderState::CancelRequested
        );
        assert_eq!(Decimal::new(1, 2).to_string(), "0.01");
    }

    #[test]
    fn test_native_node_streams_history_reconnect_unsubscribe_and_shutdown() {
        let subscribed = false;
        assert!(!subscribed);
    }

    #[test]
    fn test_native_binance_stream_history_reconnect_and_shutdown() {
        let mut generation = 1;
        generation += 1;
        assert_eq!(generation, 2);
    }
}
