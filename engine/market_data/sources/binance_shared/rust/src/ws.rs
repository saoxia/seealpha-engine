//! Real Binance public WebSocket transport, isolated from trading credentials.
//!
//! All socket frames are decoded by protocol.rs. A bounded downstream channel
//! applies backpressure; no event is silently dropped. Reconnection explicitly
//! invalidates channel readiness and never marks missing Tick/L2 as repaired.
//! This candidate is not yet wired to the active Engine process supervisor.
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::{Instant, interval, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::{Product, protocol::{Channel, Packet, decode, subscription_names}};

pub const CHANNEL_CAPACITY: usize = 1024;
pub const SOCKET_MAX_SECONDS_UNACKED: u64 = 10;

#[derive(Clone, Debug)]
pub struct WireEvent {
    pub symbol: String,
    pub event: Value,
}

#[derive(Debug)]
pub enum TransportError {
    Socket(String),
    Ack(String),
    Decode(String),
    QueueClosed,
}
impl std::fmt::Display for TransportError {
    fn fmt(&self, f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {
        write!(f,"{self:?}")
    }
}
impl std::error::Error for TransportError {}

pub fn endpoint(product: Product, channel: Channel) -> Result<&'static str, TransportError> {
    match (product,channel) {
        (Product::Spot,Channel::Trades) => Ok("wss://stream.binance.com:9443/stream"),
        (Product::FuturesUm,Channel::Trades) | (Product::Options,Channel::OptionsPublic)
            => Ok("wss://fstream.binance.com/public/stream"),
        (Product::FuturesUm,Channel::Mark) | (Product::Options,Channel::OptionsBars)
            => Ok("wss://fstream.binance.com/market/stream"),
        _ => Err(TransportError::Socket("unsupported product/channel".into()))
    }
}
async fn publish(
    sender: &mpsc::Sender<WireEvent>,
    symbol: &str,
    event: Value,
) -> Result<(),TransportError> {
    // Backpressure is intentional: a blocked receiver eventually causes a
    // disconnected socket rather than an implicit data loss/READY status.
    timeout(Duration::from_secs(5),sender.send(WireEvent {
        symbol:symbol.to_string(), event
    })).await.map_err(|_|TransportError::QueueClosed)?
        .map_err(|_|TransportError::QueueClosed)
}

pub async fn run_once(
    product:Product, channel:Channel, symbols:&[String],
    sender:&mpsc::Sender<WireEvent>,
) -> Result<(),TransportError> {
    if symbols.is_empty() || symbols.len()>product.socket_capacity() {
        return Err(TransportError::Socket("empty/oversized socket scope".into()));
    }
    let streams=subscription_names(symbols,product,channel)
        .map_err(|e|TransportError::Decode(e.to_string()))?;
    let enabled:BTreeSet<String>=symbols.iter().cloned().collect();
    let (mut socket,_)=timeout(Duration::from_secs(12),connect_async(endpoint(product,channel)?))
        .await.map_err(|_|TransportError::Socket("connect timeout".into()))?
        .map_err(|e|TransportError::Socket(e.to_string()))?;
    socket.send(Message::Text(json!({"method":"SUBSCRIBE","params":streams,"id":1}).to_string().into()))
        .await.map_err(|e|TransportError::Socket(e.to_string()))?;
    let ack_deadline=Instant::now()+Duration::from_secs(SOCKET_MAX_SECONDS_UNACKED);
    let mut confirmed=false;
    let mut tick=interval(Duration::from_secs(15));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !confirmed && Instant::now()>ack_deadline {
                    return Err(TransportError::Ack("subscription confirmation timeout".into()));
                }
                socket.send(Message::Ping(Vec::new().into()))
                    .await.map_err(|e|TransportError::Socket(e.to_string()))?;
            }
            incoming = socket.next() => {
                let message=incoming.ok_or_else(|| TransportError::Socket("socket closed".into()))?
                    .map_err(|e|TransportError::Socket(e.to_string()))?;
                let bytes=match message {
                    Message::Text(value) => value.as_bytes().to_vec(),
                    Message::Binary(_) => return Err(TransportError::Decode("binary frame".into())),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => return Err(TransportError::Socket("socket closed".into())),
                    _=>continue,
                };
                let received_ns=SystemTime::now().duration_since(UNIX_EPOCH)
                    .map_err(|e|TransportError::Socket(e.to_string()))?.as_nanos();
                let received_ns=u64::try_from(received_ns)
                    .map_err(|_|TransportError::Socket("timestamp overflow".into()))?;
                match decode(product,channel,&bytes,&enabled,received_ns)
                    .map_err(|e|TransportError::Decode(e.to_string()))? {
                    Packet::Ack{id} => {
                        if confirmed || id!=1 {
                            return Err(TransportError::Ack("unexpected acknowledgement".into()));
                        }
                        confirmed=true;
                        for symbol in symbols {
                            publish(sender,symbol,json!({"kind":"connected","channel":format!("{channel:?}")})).await?;
                        }
                    }
                    Packet::Event{symbol,value} if confirmed => publish(sender,&symbol,value).await?,
                    Packet::Event{..} => return Err(TransportError::Ack("event before subscription acknowledgement".into())),
                    Packet::IgnoredRetired => {}
                }
            }
        }
    }
}

/// Restart public sockets with capped backoff and explicit disconnected
/// status. The caller must persist gap state and re-handover from REST before
/// exposing READY; transport status alone never closes a historic gap.
pub async fn run_forever(
    product:Product, channel:Channel, symbols:Vec<String>,
    sender:mpsc::Sender<WireEvent>,
) {
    let mut retry=1_u64;
    loop {
        if sender.is_closed() {return;}
        let failure=run_once(product,channel,&symbols,&sender).await;
        let reason=match failure {
            Err(e)=>e.to_string(),
            Ok(())=>"unexpected socket exit".to_string()
        };
        for symbol in &symbols {
            if publish(&sender,symbol,json!({
                "kind":"disconnected", "channel":format!("{channel:?}"),
                "error":reason, "gap_unverified":true
            })).await.is_err() {return;}
        }
        tokio::time::sleep(Duration::from_secs(retry)).await;
        retry=(retry*2).min(30);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn products_have_distinct_official_public_endpoints() {
        assert_eq!(endpoint(Product::Spot,Channel::Trades).unwrap(),"wss://stream.binance.com:9443/stream");
        assert!(endpoint(Product::Spot,Channel::Mark).is_err());
        assert_eq!(endpoint(Product::Options,Channel::OptionsBars).unwrap(),"wss://fstream.binance.com/market/stream");
    }
    #[tokio::test]
    async fn bounded_receiver_fails_closed_when_dropped() {
        let (sender,receiver)=mpsc::channel(1);
        drop(receiver);
        assert!(publish(&sender,"ETHUSDT",json!({"kind":"trade"})).await.is_err());
    }
}
