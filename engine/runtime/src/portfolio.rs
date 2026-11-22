//! Rust-owned coordinator lifecycle, persisted before handing out any order intent.
//! Commands are never sent to the venue by this adapter.
use crate::{Runtime, RuntimeError, map_ledger};
use rust_decimal::Decimal;
use seealpha_execution::{
    TargetHolding,
    coordinator::{Bar, Command, Coordinator, VirtualFill, VirtualKind, VirtualOrder},
    coordinator_snapshot::{decode_coordinator, encode_coordinator},
};
/// All events are applied to an isolated snapshot and atomically persisted.
pub enum NativePortfolioEvent {
    RegisterMember(String),
    PublishTarget {
        member: String,
        quantity: Decimal,
        risk_exit: bool,
    },
    PublishHolding {
        member: String,
        holding: TargetHolding,
        allocation: Decimal,
        risk_exit: bool,
    },
    AcceptOrder {
        order: VirtualOrder,
        armed_at: Option<u64>,
    },
    ModifyOrder {
        id: String,
        quantity: Option<Decimal>,
        kind: Option<VirtualKind>,
    },
    CancelOrder(String),
    FlattenMember(String),
    ConfirmTerminal {
        order_id: String,
        canceled: bool,
        denied_at: Option<u64>,
    },
    CancelDenied {
        order_id: String,
        at: Option<u64>,
    },
    Freeze(String),
}
pub struct NativePortfolioState {
    pub coordinator: Coordinator,
    pub revision: String,
}
pub struct NativePortfolioResult {
    pub state: NativePortfolioState,
    pub fills: Vec<VirtualFill>,
    pub command: Option<Command>,
}
fn scope(tenant: &str, account: &str, instrument: &str) -> Result<String, RuntimeError> {
    if [tenant, account, instrument].iter().any(|s| {
        s.is_empty()
            || s.len() > 40
            || !s.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || b == b'_'
                    || b == b'-'
                    || b == b'.'
                    || b == b'@'
                    || b == b'/'
                    || b == b':'
            })
    }) {
        return Err(RuntimeError::Conflict(
            "invalid native portfolio scope".into(),
        ));
    }
    Ok(format!("{tenant}:{account}:{instrument}"))
}
impl Runtime {
    /// Reads a durable state; unresolved flight remains pending.
    pub fn recover_native_portfolio(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
    ) -> Result<Option<NativePortfolioState>, RuntimeError> {
        self.require_active(tenant)?;
        let key = scope(tenant, account, instrument)?;
        let Some(row) = self
            .state_store
            .load_coordinator_state(tenant, account, instrument)
            .map_err(map_ledger)?
        else {
            return Ok(None);
        };
        let (coordinator, revision) = decode_coordinator(&key, &row.json)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        if revision != row.revision {
            return Err(RuntimeError::Conflict(
                "native portfolio revision mismatch".into(),
            ));
        }
        Ok(Some(NativePortfolioState {
            coordinator,
            revision,
        }))
    }
    pub fn save_native_portfolio(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
        coordinator: &Coordinator,
        expected: Option<&str>,
    ) -> Result<NativePortfolioState, RuntimeError> {
        self.require_active(tenant)?;
        let key = scope(tenant, account, instrument)?;
        let encoded = encode_coordinator(&key, coordinator)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        self.state_store
            .save_coordinator_state(
                tenant,
                account,
                instrument,
                &encoded.json,
                &encoded.revision,
                expected,
            )
            .map_err(map_ledger)?;
        Ok(NativePortfolioState {
            coordinator: coordinator.clone(),
            revision: encoded.revision,
        })
    }
    pub fn apply_native_portfolio_event(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
        expected_revision: &str,
        event: NativePortfolioEvent,
    ) -> Result<NativePortfolioState, RuntimeError> {
        let state = self
            .recover_native_portfolio(tenant, account, instrument)?
            .ok_or_else(|| RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator = state.coordinator;
        let result = match event {
            NativePortfolioEvent::RegisterMember(id) => coordinator.register(&id),
            NativePortfolioEvent::PublishTarget {
                member,
                quantity,
                risk_exit,
            } => coordinator.report_target(&member, quantity, risk_exit),
            NativePortfolioEvent::PublishHolding {
                member,
                holding,
                allocation,
                risk_exit,
            } => {
                if holding.symbol != instrument {
                    return Err(RuntimeError::Conflict(
                        "portfolio target instrument mismatch".into(),
                    ));
                }
                coordinator.report_holding(&member, &holding, allocation, risk_exit)
            }
            NativePortfolioEvent::AcceptOrder { order, armed_at } => {
                coordinator.accept_order(order, armed_at)
            }
            NativePortfolioEvent::ModifyOrder { id, quantity, kind } => {
                coordinator.modify(&id, quantity, kind)
            }
            NativePortfolioEvent::CancelOrder(id) => coordinator.cancel(&id),
            NativePortfolioEvent::FlattenMember(member) => coordinator.flatten(&member),
            NativePortfolioEvent::ConfirmTerminal {
                order_id,
                canceled,
                denied_at,
            } => coordinator.confirm_terminal(&order_id, canceled, denied_at),
            NativePortfolioEvent::CancelDenied { order_id, at } => {
                coordinator.cancel_denied(&order_id, at)
            }
            NativePortfolioEvent::Freeze(reason) => {
                coordinator.freeze(&reason);
                Ok(())
            }
        };
        if let Err(error) = result {
            if coordinator.frozen {
                self.save_native_portfolio(
                    tenant,
                    account,
                    instrument,
                    &coordinator,
                    Some(expected_revision),
                )?;
            }
            return Err(RuntimeError::Conflict(error.to_string()));
        }
        self.save_native_portfolio(
            tenant,
            account,
            instrument,
            &coordinator,
            Some(expected_revision),
        )
    }
    /// Caller supplies authoritative net position; persisted flight is a prerequisite
    /// for interpreting Submit as a request eligible for subsequent risk checks.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_native_portfolio(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
        expected_revision: &str,
        bar: Option<Bar>,
        account_position: Decimal,
        request_id: &str,
    ) -> Result<NativePortfolioResult, RuntimeError> {
        let state = self
            .recover_native_portfolio(tenant, account, instrument)?
            .ok_or_else(|| RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator = state.coordinator;
        let (fills, command) = coordinator
            .reconcile(bar, account_position, request_id)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let updated = self.save_native_portfolio(
            tenant,
            account,
            instrument,
            &coordinator,
            Some(expected_revision),
        )?;
        Ok(NativePortfolioResult {
            state: updated,
            fills,
            command,
        })
    }
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_native_portfolio_fill(
        &self,
        tenant: &str,
        account: &str,
        instrument: &str,
        expected_revision: &str,
        order_id: &str,
        event_id: &str,
        cumulative: Decimal,
        price: Decimal,
        bar: Option<Bar>,
    ) -> Result<NativePortfolioResult, RuntimeError> {
        let state = self
            .recover_native_portfolio(tenant, account, instrument)?
            .ok_or_else(|| RuntimeError::NotFound("portfolio state missing".into()))?;
        if state.revision != expected_revision {
            return Err(RuntimeError::Conflict("stale portfolio state".into()));
        }
        let mut coordinator = state.coordinator;
        let fills = match coordinator.apply_venue_fill(order_id, event_id, cumulative, price, bar) {
            Ok(fills) => fills,
            Err(error) => {
                if coordinator.frozen {
                    self.save_native_portfolio(
                        tenant,
                        account,
                        instrument,
                        &coordinator,
                        Some(expected_revision),
                    )?;
                }
                return Err(RuntimeError::Conflict(error.to_string()));
            }
        };
        let updated = self.save_native_portfolio(
            tenant,
            account,
            instrument,
            &coordinator,
            Some(expected_revision),
        )?;
        Ok(NativePortfolioResult {
            state: updated,
            fills,
            command: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthMode, Config};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn weighted_portfolio_event_persists_and_rejects_other_instruments() {
        let state = std::env::temp_dir().join(format!(
            "seealpha-native-holding-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: AuthMode::None,
            token: String::new(),
            state_dir: state.clone(),
            project: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: PathBuf::from("python3"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime.ensure_tenant("dev", "development", "active").unwrap();

        let coordinator = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
        let saved = runtime
            .save_native_portfolio("dev", "paper", "ETHUSDT", &coordinator, None)
            .unwrap();
        let registered = runtime
            .apply_native_portfolio_event(
                "dev", "paper", "ETHUSDT", &saved.revision,
                NativePortfolioEvent::RegisterMember("strategy-a".into()),
            ).unwrap();
        let published = runtime
            .apply_native_portfolio_event(
                "dev", "paper", "ETHUSDT", &registered.revision,
                NativePortfolioEvent::PublishHolding {
                    member: "strategy-a".into(),
                    holding: TargetHolding::new(
                        "ETHUSDT", None, Some(Decimal::new(5, 1))
                    ).unwrap(),
                    allocation: Decimal::from(4),
                    risk_exit: false,
                },
            ).unwrap();
        let recovered = runtime
            .recover_native_portfolio("dev", "paper", "ETHUSDT")
            .unwrap().unwrap();
        assert_eq!(published.revision, recovered.revision);
        let reconciled = runtime
            .reconcile_native_portfolio(
                "dev", "paper", "ETHUSDT", &recovered.revision,
                None, Decimal::ZERO, "paper-command-1",
            ).unwrap();
        assert!(matches!(
            reconciled.command,
            Some(Command::Submit { signed_quantity, .. })
                if signed_quantity == Decimal::from(2)
        ));

        let current = reconciled.state.revision;
        let mismatch = runtime.apply_native_portfolio_event(
            "dev", "paper", "ETHUSDT", &current,
            NativePortfolioEvent::PublishHolding {
                member: "strategy-a".into(),
                holding: TargetHolding::new("BTCUSDT", Some(Decimal::ONE), None).unwrap(),
                allocation: Decimal::ONE,
                risk_exit: false,
            },
        );
        assert!(matches!(mismatch, Err(RuntimeError::Conflict(_))));
        assert_eq!(
            runtime.recover_native_portfolio("dev", "paper", "ETHUSDT")
                .unwrap().unwrap().revision,
            current
        );
        let _ = std::fs::remove_dir_all(state);
    }
}
