//! Read-only typed Nautilus order-event subscription for SeeAlpha attribution.
//!
//! Nautilus retains custody of order lifecycle, cache, reconciliation and raw
//! execution event recording. This observer never synthesizes an order state,
//! performs a network request or credits a trade to the economic ledger.
//! A registered order binding must originate from an already committed intent.
use nautilus_common::msgbus::{self, TypedHandler};
use nautilus_model::events::OrderEventAny;

use seealpha_nautilus_binance_data::native_order_link::{
    observe_native_order_event, NativeOrderObservation, OwnedNativeBinding,
};

/// Register with Nautilus' *typed* order event router. The message bus is
/// thread-local: create/use/drop this handle on the same Nautilus node thread.
/// The callback is a local observer, NOT an authenticated cross-process feed.
pub struct NativeOrderLinkSubscription {
    handler: TypedHandler<OrderEventAny>,
    topic: String,
}

impl NativeOrderLinkSubscription {
    /// Install exactly one native order-event subscription per isolated
    /// Nautilus node. Resolve immutable bindings by client order ID from the
    /// caller's registered-order catalog; do not duplicate the OMS order book.
    /// Unknown orders are ignored rather than adopted into SeeAlpha state.
    pub fn attach_resolver(
        resolve_binding: impl Fn(&str) -> Option<OwnedNativeBinding> + 'static,
        on_event: impl Fn(NativeOrderObservation) + 'static,
        on_error: impl Fn(String) + 'static,
    ) -> Self {
        // Native ExecutionEngine emits all order state transitions on the
        // strategy-scoped events.order.<strategy_id> typed topic.
        let topic = "events.order.*".to_owned();
        let handler = TypedHandler::from(move |event: &OrderEventAny| {
            let client_id = event.client_order_id().to_string();
            let Some(binding) = resolve_binding(&client_id) else {
                return;
            };
            if binding.client_order_id != client_id {
                on_error("native order resolver returned conflicting client ID".into());
                return;
            }
            match observe_native_order_event(event, &binding) {
                Ok(Some(observation)) => on_event(observation),
                Ok(None) => {}
                Err(error) => on_error(error.to_string()),
            }
        });
        msgbus::subscribe_order_events(topic.clone().into(), handler.clone(), None);
        Self { handler, topic }
    }

    /// Convenience helper for a single pre-registered order. Production
    /// adapters should prefer `attach_resolver` to avoid O(orders) subscribers.
    pub fn attach(
        binding: OwnedNativeBinding,
        on_event: impl Fn(NativeOrderObservation) + 'static,
        on_error: impl Fn(String) + 'static,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(binding.valid(), "invalid native order binding");
        Ok(Self::attach_resolver(
            move |client_id| {
                (client_id == binding.client_order_id).then(|| binding.clone())
            },
            on_event,
            on_error,
        ))
    }
}

impl Drop for NativeOrderLinkSubscription {
    fn drop(&mut self) {
        msgbus::unsubscribe_order_events(self.topic.clone().into(), &self.handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_identity_fails_before_subscribing_to_nautilus() {
        let binding = OwnedNativeBinding {
            account_id: String::new(),
            instrument_id: "ETHUSDT-PERP.BINANCE".into(),
            client_order_id: "SA123".into(),
            venue_order_id: None,
        };
        assert!(NativeOrderLinkSubscription::attach(binding, |_| {}, |_| {}).is_err());
    }
}
