//! Read-only typed Nautilus order-event subscription for SeeAlpha attribution.
//!
//! Nautilus retains custody of order lifecycle, cache, reconciliation and raw
//! execution event recording. This observer never synthesizes an order state,
//! performs a network request or credits a trade to the economic ledger.
//! A registered order binding must originate from an already committed intent.
use nautilus_common::msgbus::{self, TypedHandler};
use nautilus_model::events::OrderEventAny;

use crate::native_execution::{
    check_native_fill_binding, link_native_accept, link_native_fill,
    NativeAcceptLink, NativeFillLink, NativeOrderBinding,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeOrderObservation {
    /// This is only an acknowledgment, not a verified trade.
    Accepted(NativeAcceptLink),
    /// This event must be separately verified before attribution or settlement.
    Filled(NativeFillLink),
    /// An executed trade was corrected or voided: freeze attribution and
    /// reconcile using Nautilus-native order/position reports.
    FillVoided { client_order_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedNativeBinding {
    pub account_id: String,
    pub instrument_id: String,
    pub client_order_id: String,
    pub venue_order_id: Option<String>,
}

impl OwnedNativeBinding {
    fn borrowed(&self) -> NativeOrderBinding<'_> {
        NativeOrderBinding {
            account_id: &self.account_id,
            instrument_id: &self.instrument_id,
            client_order_id: &self.client_order_id,
            venue_order_id: self.venue_order_id.as_deref(),
        }
    }

    fn valid(&self) -> bool {
        !self.account_id.is_empty()
            && !self.instrument_id.is_empty()
            && !self.client_order_id.is_empty()
            && self.venue_order_id.as_deref() != Some("")
    }
}

/// Scope an order event against a pre-registered identity without caching
/// its mutable venue state. Unknown events are ignored, conflicting matched
/// events are surfaced as errors so the owner can freeze and reconcile.
pub fn observe_native_order_event(
    event: &OrderEventAny,
    binding: &OwnedNativeBinding,
) -> anyhow::Result<Option<NativeOrderObservation>> {
    anyhow::ensure!(binding.valid(), "invalid native order binding");
    match event {
        OrderEventAny::Accepted(accepted) => {
            let link = link_native_accept(accepted);
            if link.client_order_id != binding.client_order_id {
                return Ok(None);
            }
            anyhow::ensure!(
                link.account_id == binding.account_id
                    && link.instrument_id == binding.instrument_id
                    && binding.venue_order_id.as_deref().is_none_or(|id| link.venue_order_id == id)
                    && !link.venue_order_id.is_empty()
                    && !link.event_id.is_empty()
                    && link.event_ts_ns > 0,
                "native accepted order identity mismatch"
            );
            Ok(Some(NativeOrderObservation::Accepted(link)))
        }
        OrderEventAny::Filled(filled) => {
            let link = link_native_fill(filled);
            if link.client_order_id != binding.client_order_id {
                return Ok(None);
            }
            check_native_fill_binding(&link, &binding.borrowed())?;
            Ok(Some(NativeOrderObservation::Filled(link)))
        }
        OrderEventAny::FillVoided(voided) => {
            if voided.client_order_id.to_string() != binding.client_order_id {
                return Ok(None);
            }
            // A void/correction is just as sensitive to cross-account ID
            // collisions as a fill. Never freeze another account's order.
            anyhow::ensure!(
                event.account_id().is_some_and(|id| id.to_string() == binding.account_id)
                    && event.instrument_id().to_string() == binding.instrument_id
                    && binding.venue_order_id.as_deref().is_none_or(|id| {
                        event.venue_order_id().is_some_and(|actual| actual.to_string() == id)
                    }),
                "native fill void identity mismatch"
            );
            // Without a verified correction replay, the only safe action is
            // to suspend downstream fill attribution, not subtract cash.
            Ok(Some(NativeOrderObservation::FillVoided {
                client_order_id: binding.client_order_id.clone(),
            }))
        }
        _ => Ok(None),
    }
}

/// Register with Nautilus' *typed* order event router. The message bus is
/// thread-local: create/use/drop this handle on the same Nautilus node thread.
/// The callback is a local observer, NOT an authenticated cross-process feed.
pub struct NativeOrderLinkSubscription {
    handler: TypedHandler<OrderEventAny>,
    topic: String,
}

impl NativeOrderLinkSubscription {
    pub fn attach(
        binding: OwnedNativeBinding,
        on_event: impl Fn(NativeOrderObservation) + 'static,
        on_error: impl Fn(String) + 'static,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(binding.valid(), "invalid native order binding");
        // The typed bus delivers OrderEventAny, rather than individual fill
        // variants. Filter every event by the immutable client identity.
        let topic = "events.order.*".to_owned();
        let handler = TypedHandler::from(move |event: &OrderEventAny| {
            match observe_native_order_event(event, &binding) {
                Ok(Some(observation)) => on_event(observation),
                Ok(None) => {}
                Err(error) => on_error(error.to_string()),
            }
        });
        msgbus::subscribe_order_events(topic.clone().into(), handler.clone(), None);
        Ok(Self { handler, topic })
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
