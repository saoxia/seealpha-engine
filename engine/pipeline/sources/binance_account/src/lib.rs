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

/// Link the existing Nautilus event subscription to SeeAlpha virtual orders.
///
/// A caller supplies an already committed Coordinator and Nautilus's latest
/// OrderCore for the registered client order. The callback must CAS-persist the
/// result before any virtual fills are released; this API never submits orders.
pub fn attach_native_virtual_attribution(
    resolve_binding: impl Fn(&str) -> Option<OwnedNativeBinding> + 'static,
    resolve_snapshot: impl Fn(&str) -> Option<(
        seealpha_execution::coordinator::Coordinator,
        nautilus_model::orders::OrderCore,
    )> + 'static,
    commit: impl Fn(
        &str,
        &seealpha_nautilus_binance_data::native_virtual_attribution::VirtualAttribution,
    ) -> anyhow::Result<()> + 'static,
    publish: impl Fn(Vec<seealpha_execution::coordinator::VirtualFill>) + 'static,
    on_error: impl Fn(String) + 'static,
) -> NativeOrderLinkSubscription {
    use std::rc::Rc;
    use seealpha_nautilus_binance_data::{
        native_execution::NativeOrderBinding,
        native_virtual_attribution::project_native_order_observation,
    };
    // All handlers execute on the same thread-local Nautilus message bus.
    let resolver = Rc::new(resolve_binding);
    let errors = Rc::new(on_error);
    let resolver_for_events = Rc::clone(&resolver);
    let errors_for_events = Rc::clone(&errors);
    let events = move |event: NativeOrderObservation| {
        let client_id = match &event {
            NativeOrderObservation::Accepted(x) => &x.client_order_id,
            NativeOrderObservation::Filled(x) => &x.client_order_id,
            NativeOrderObservation::FillVoided { client_order_id } => client_order_id,
        };
        let Some(binding) = resolver_for_events(client_id) else {
            errors_for_events("native attribution binding disappeared".into());
            return;
        };
        let Some((coordinator, native_order)) = resolve_snapshot(client_id) else {
            errors_for_events("Nautilus order or committed coordinator unavailable".into());
            return;
        };
        let bound = NativeOrderBinding {
            account_id: &binding.account_id,
            instrument_id: &binding.instrument_id,
            client_order_id: &binding.client_order_id,
            venue_order_id: binding.venue_order_id.as_deref(),
        };
        match project_native_order_observation(
            &coordinator, &event, &native_order, &bound, None,
        ) {
            Ok(projected) => {
                // An unsuccessful CAS releases no attributed fills.
                if let Err(e) = commit(client_id, &projected) {
                    errors_for_events(format!("native attribution CAS failed: {e}"));
                    return;
                }
                publish(projected.fills);
            }
            Err(e) => errors_for_events(format!("native attribution rejected: {e}")),
        }
    };
    NativeOrderLinkSubscription::attach_resolver(
        move |id| resolver(id),
        events,
        move |message| errors(message),
    )
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
