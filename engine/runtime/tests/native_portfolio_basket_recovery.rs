use rust_decimal::Decimal;
use seealpha_execution::{
    basket::{Checkpoint, LegId, Phase, Plan, Product, State},
    coordinator::{Bar, Command, Coordinator, Side, VirtualKind, VirtualOrder},
};
use seealpha_runtime::{AuthMode, Config, NativePortfolioEvent, Runtime};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
fn d(x: i64) -> Decimal {
    Decimal::from(x)
}
fn config(state: PathBuf) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: 0,
        auth_mode: AuthMode::None,
        token: String::new(),
        state_dir: state,
        project: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
        tenant_id: None,
        tenant_purpose: None,
        research_signing_key: None,
        research_mcp_enabled: false,
        venue_base: None,
        python: PathBuf::from("python3"),
    }
}
fn temp() -> PathBuf {
    std::env::temp_dir().join(format!(
        "seealpha-rust-coordination-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
#[test]
fn basket_journal_survives_restart_and_rejects_stale_writer() {
    let state = temp();
    let config = config(state.clone());
    let leg = LegId {
        tenant: "dev".into(),
        account: "paper".into(),
        product: Product::Spot,
        symbol: "ETHUSDT".into(),
    };
    let plan = Plan {
        targets: [(leg.clone(), d(1))].into(),
        phases: vec![Phase {
            name: "acquire".into(),
            changes: [(leg.clone(), d(1))].into(),
        }],
        gross_notional: d(100),
        executable: false,
    };
    let cp = Checkpoint::new(plan, [(leg, d(0))].into()).unwrap();
    let first;
    {
        let runtime = Runtime::open(&config).unwrap();
        runtime
            .ensure_tenant("dev", "development", "active")
            .unwrap();
        let created = runtime
            .create_basket_checkpoint("dev", "paper", "basket1", &cp)
            .unwrap();
        let pending = runtime
            .register_basket_phase("dev", "paper", "basket1", &created.revision, "req1")
            .unwrap();
        first = pending.revision;
        assert_eq!(pending.checkpoint.state, State::AwaitingConfirmation);
        assert!(
            runtime
                .register_basket_phase("dev", "paper", "basket1", &created.revision, "req2")
                .is_err()
        );
    }
    {
        let runtime = Runtime::open(&config).unwrap();
        let recovered = runtime
            .recover_basket_checkpoint("dev", "paper", "basket1")
            .unwrap()
            .unwrap();
        assert_eq!(recovered.revision, first);
        assert_eq!(recovered.checkpoint.state, State::AwaitingConfirmation);
        assert_eq!(recovered.checkpoint.request_id.as_deref(), Some("req1"));
    }
    let _ = std::fs::remove_dir_all(state);
}
#[test]
fn native_portfolio_preserves_unresolved_flight_on_restart() {
    let state = temp();
    let config = config(state.clone());
    {
        let runtime = Runtime::open(&config).unwrap();
        runtime
            .ensure_tenant("dev", "development", "active")
            .unwrap();
        let mut coordinator = Coordinator::new(d(1), d(100)).unwrap();
        coordinator.register("alpha").unwrap();
        let saved = runtime
            .save_native_portfolio("dev", "paper", "ETHUSDT", &coordinator, None)
            .unwrap();
        let order = VirtualOrder {
            id: "v1".into(),
            member_id: "alpha".into(),
            side: Side::Buy,
            quantity: d(2),
            filled: d(0),
            kind: VirtualKind::Market,
            risk_exit: false,
            armed_at: None,
        };
        let saved = runtime
            .apply_native_portfolio_event(
                "dev",
                "paper",
                "ETHUSDT",
                &saved.revision,
                NativePortfolioEvent::AcceptOrder {
                    order,
                    armed_at: None,
                },
            )
            .unwrap();
        let bar = Bar {
            time: 2,
            low: d(10),
            high: d(10),
            close: d(10),
        };
        let change = runtime
            .reconcile_native_portfolio(
                "dev",
                "paper",
                "ETHUSDT",
                &saved.revision,
                Some(bar),
                d(0),
                "order1",
            )
            .unwrap();
        assert_eq!(
            change.command,
            Some(Command::Submit {
                id: "order1".into(),
                signed_quantity: d(2)
            })
        );
    }
    {
        let runtime = Runtime::open(&config).unwrap();
        let recovered = runtime
            .recover_native_portfolio("dev", "paper", "ETHUSDT")
            .unwrap()
            .unwrap();
        assert_eq!(recovered.coordinator.flight.as_ref().unwrap().id, "order1");
        let result = runtime
            .reconcile_native_portfolio(
                "dev",
                "paper",
                "ETHUSDT",
                &recovered.revision,
                None,
                d(0),
                "order2",
            )
            .unwrap();
        assert_eq!(result.command, None);
    }
    let _ = std::fs::remove_dir_all(state);
}
