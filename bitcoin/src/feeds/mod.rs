use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::{select, sync::mpsc, time};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[rustfmt::skip]
pub mod bybit;
#[rustfmt::skip]
pub mod gemini;
#[rustfmt::skip]
pub mod binance;
#[rustfmt::skip]
pub mod bitstamp;
#[rustfmt::skip]
pub mod coinbase;

#[derive(Clone, Debug)]
pub struct FeedConfig {
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            reconnect_min_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub best_bid: f64,
    pub best_ask: f64,
    pub delay_ms: Option<f64>,
}

impl Quote {
    pub fn mid(&self) -> f64 {
        (self.best_bid + self.best_ask) / 2.0
    }

    pub fn new(best_bid: f64, best_ask: f64, delay_ms: f64) -> Self {
        Self {
            best_bid,
            best_ask,
            delay_ms: finite_or_none(delay_ms),
        }
    }

    pub fn with_delay(
        timestamp_ms: f64,
        best_bid: f64,
        best_ask: f64,
        received_at_ms: f64,
    ) -> Self {
        Self {
            best_bid,
            best_ask,
            delay_ms: finite_or_none(received_at_ms - timestamp_ms),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FeedUpdate {
    pub exchange: &'static str,
    pub quote: Quote,
}

#[derive(Clone, Debug)]
pub enum FeedEvent {
    Quote(Quote),
    Info(String),
    Error(String),
    Ignore,
}

pub trait ExchangeFeed: Clone + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn url(&self) -> &'static str;
    fn subscriptions(&self) -> Vec<String>;
    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }
    fn heartbeat_message(&self) -> Option<String> {
        None
    }
    fn handle_text(&self, raw: &str, received_at_ms: f64) -> FeedEvent;
}

pub async fn run_feed<F>(
    feed: F,
    config: FeedConfig,
    updates: mpsc::Sender<FeedUpdate>,
) -> Result<()>
where
    F: ExchangeFeed,
{
    let mut reconnect_delay = config.reconnect_min_delay;

    loop {
        match run_once(feed.clone(), updates.clone()).await {
            Ok(()) => {
                eprintln!("{} websocket closed cleanly", feed.name());
                reconnect_delay = config.reconnect_min_delay;
            }
            Err(error) => {
                eprintln!("{} websocket error: {error:#}", feed.name());
            }
        }

        eprintln!(
            "{} reconnecting in {:.1}s",
            feed.name(),
            reconnect_delay.as_secs_f64()
        );
        time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(config.reconnect_max_delay);
    }
}

async fn run_once<F>(feed: F, updates: mpsc::Sender<FeedUpdate>) -> Result<()>
where
    F: ExchangeFeed,
{
    let (mut ws, _) = connect_async(feed.url())
        .await
        .with_context(|| format!("connect {}", feed.url()))?;

    eprintln!("{} connected {}", feed.name(), feed.url());

    for subscription in feed.subscriptions() {
        ws.send(Message::Text(subscription.into()))
            .await
            .with_context(|| format!("send {} subscription", feed.name()))?;
    }

    let heartbeat = feed.heartbeat_interval().map(time::interval);
    tokio::pin!(heartbeat);

    loop {
        select! {
            _ = async {
                match heartbeat.as_mut().as_pin_mut() {
                    Some(mut interval) => interval.tick().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(message) = feed.heartbeat_message() {
                    ws.send(Message::Text(message.into()))
                        .await
                        .with_context(|| format!("send {} heartbeat", feed.name()))?;
                }
            }
            message = ws.next() => {
                let Some(message) = message else {
                    return Ok(());
                };

                match message.with_context(|| format!("read {} message", feed.name()))? {
                    Message::Text(text) => handle_event(feed.name(), feed.handle_text(&text, now_unix_ms()), &updates).await?,
                    Message::Binary(bytes) => {
                        let text = String::from_utf8(bytes.to_vec())
                            .with_context(|| format!("decode {} binary message", feed.name()))?;
                        handle_event(feed.name(), feed.handle_text(&text, now_unix_ms()), &updates).await?;
                    }
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        eprintln!("{} closed by peer: {:?}", feed.name(), frame);
                        return Ok(());
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn handle_event(
    exchange: &'static str,
    event: FeedEvent,
    updates: &mpsc::Sender<FeedUpdate>,
) -> Result<()> {
    match event {
        FeedEvent::Quote(quote) => updates
            .send(FeedUpdate { exchange, quote })
            .await
            .with_context(|| format!("send {exchange} quote update")),
        FeedEvent::Info(message) => {
            eprintln!("{message}");
            Ok(())
        }
        FeedEvent::Error(message) => {
            eprintln!("{message}");
            Ok(())
        }
        FeedEvent::Ignore => Ok(()),
    }
}

pub fn now_unix_ms() -> f64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs_f64() * 1000.0
}

pub fn parse_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value? {
        serde_json::Value::String(text) => finite_or_none(text.parse().ok()?),
        serde_json::Value::Number(number) => finite_or_none(number.as_f64()?),
        _ => None,
    }
}

pub fn finite_or_none(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}
