//! Read-only identity/freshness boundary for native Nautilus AccountState.
//!
//! A reported AccountState is a *candidate* to be matched to an authenticated
//! execution client. It is not a signed exchange-account query, Ledger
//! account snapshot, cash movement or permission to submit an order.
use anyhow::{Result, ensure};
use nautilus_model::{
    enums::AccountType,
    events::account::state::AccountState,
};

/// Metadata available after filtering a pinned-version Nautilus account
/// report. Deliberately excludes balances and margins: these remain owned by
/// the native account model, and should not be stored as a competing state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAccountReportCandidate {
    pub account_id: String,
    pub event_id: String,
    pub observed_at_ns: u64,
}

/// Check account identity, account type and observation freshness *before*
/// an authenticated native actor may consider the event for further mapping.
///
/// This function cannot prove transport authenticity, an instrument/product
/// mapping, complete account coverage, or exchange signature validity.
/// The caller must bind a trusted native execution client to the
/// account independently; this API never writes a financial Ledger fact.
pub fn inspect_native_account_report(
    event: &AccountState,
    expected_account_id: &str,
    expected_account_type: AccountType,
    now_ns: u64,
    max_age_ns: u64,
) -> Result<NativeAccountReportCandidate> {
    ensure!(!expected_account_id.is_empty(), "expected Nautilus account ID is empty");
    ensure!(
        event.account_id.to_string() == expected_account_id,
        "Nautilus account report identity mismatch"
    );
    ensure!(
        event.account_type == expected_account_type,
        "Nautilus account report account type mismatch"
    );
    ensure!(
        event.is_reported,
        "system-calculated Nautilus AccountState is not an exchange report"
    );

    let observed = event.ts_event.as_u64();
    let initialized = event.ts_init.as_u64();
    ensure!(observed > 0 && initialized > 0, "Nautilus account report timestamps invalid");
    ensure!(now_ns > 0 && max_age_ns > 0, "account report time window invalid");
    ensure!(observed <= now_ns, "Nautilus account report is in the future");
    ensure!(
        now_ns - observed <= max_age_ns,
        "Nautilus account report is stale"
    );
    Ok(NativeAccountReportCandidate {
        account_id: event.account_id.to_string(),
        event_id: event.event_id.to_string(),
        observed_at_ns: observed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::identifiers::AccountId;

    fn report(reported: bool, account_id: &str, account_type: AccountType) -> AccountState {
        AccountState::new(
            AccountId::from(account_id),
            account_type,
            vec![],
            vec![],
            reported,
            UUID4::new(),
            UnixNanos::new(1_000_000_000),
            UnixNanos::new(1_000_000_000),
            None,
        )
    }

    #[test]
    fn pins_native_accountstate_and_accepts_exact_report_metadata_only() {
        let report = report(true, "BINANCE-SPOT-001", AccountType::Cash);
        let candidate = inspect_native_account_report(
            &report, "BINANCE-SPOT-001", AccountType::Cash,
            1_300_000_000, 300_000_000,
        ).unwrap();
        assert_eq!(candidate.account_id, "BINANCE-SPOT-001");
        assert_eq!(candidate.event_id, report.event_id.to_string());
        assert_eq!(candidate.observed_at_ns, 1_000_000_000);
        // No balances, margins or "signed" flag can be fabricated by the
        // candidate metadata.
    }

    #[test]
    fn refuses_local_calculation_wrong_account_or_wrong_account_type() {
        assert!(inspect_native_account_report(
            &report(false, "BINANCE-SPOT-001", AccountType::Cash),
            "BINANCE-SPOT-001", AccountType::Cash, 1_000_000_000, 1,
        ).is_err());
        let report = report(true, "BINANCE-SPOT-001", AccountType::Cash);
        assert!(inspect_native_account_report(
            &report, "BINANCE-UM-001", AccountType::Cash, 1_000_000_000, 1,
        ).is_err());
        assert!(inspect_native_account_report(
            &report, "BINANCE-SPOT-001", AccountType::Margin, 1_000_000_000, 1,
        ).is_err());
    }

    #[test]
    fn refuses_expired_future_and_invalid_time_windows() {
        let report = report(true, "BINANCE-SPOT-001", AccountType::Cash);
        for (now_ns, age) in [
            (1_300_000_001, 300_000_000),
            (999_999_999, 300_000_000),
            (0, 300_000_000),
            (1_300_000_000, 0),
        ] {
            assert!(inspect_native_account_report(
                &report, "BINANCE-SPOT-001", AccountType::Cash, now_ns, age,
            ).is_err());
        }
    }
}
