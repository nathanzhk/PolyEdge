use serde::Deserialize;
use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64_str};

#[derive(Clone, Debug)]
pub struct Bitstamp {
    channel: &'static str,
}

impl Default for Bitstamp {
    fn default() -> Self {
        Self {
            channel: "order_book_btcusd",
        }
    }
}

#[derive(Deserialize)]
struct BitstampMessage {
    event: Option<String>,
    channel: Option<String>,
    data: Option<Value>,
}

#[derive(Deserialize)]
struct OrderBookData {
    microtimestamp: Option<String>,
    timestamp: Option<String>,
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

impl ExchangeFeed for Bitstamp {
    fn name(&self) -> &'static str {
        "bitstamp"
    }

    fn url(&self) -> &'static str {
        "wss://ws.bitstamp.net"
    }

    fn subscriptions(&self) -> Vec<String> {
        vec![
            json!({
                "event": "bts:subscribe",
                "data": { "channel": self.channel },
            })
            .to_string(),
        ]
    }

    fn handle_text(&self, raw: &str, received_at_ms: f64) -> FeedEvent {
        let Ok(msg) = serde_json::from_str::<BitstampMessage>(raw) else {
            return FeedEvent::Error("bitstamp failed to parse message".to_string());
        };

        match msg.event.as_deref() {
            Some("data") if msg.channel.as_deref() == Some(self.channel) => {
                let Some(data) = msg.data else {
                    return FeedEvent::Ignore;
                };
                let Ok(book) = serde_json::from_value::<OrderBookData>(data) else {
                    return FeedEvent::Ignore;
                };
                parse_order_book(&book, received_at_ms)
                    .map(FeedEvent::Quote)
                    .unwrap_or(FeedEvent::Ignore)
            }
            Some("bts:subscription_succeeded") => {
                FeedEvent::Info(format!("bitstamp subscription succeeded {}", self.channel))
            }
            Some("bts:error") => FeedEvent::Error(format!("bitstamp error: {raw}")),
            _ => FeedEvent::Ignore,
        }
    }
}

fn parse_order_book(book: &OrderBookData, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = book
        .microtimestamp
        .as_deref()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v / 1000.0)
        .or_else(|| {
            book.timestamp
                .as_deref()
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v * 1000.0)
        })?;

    let best_bid = parse_f64_str(book.bids.first()?.first()?)?;
    let best_ask = parse_f64_str(book.asks.first()?.first()?)?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
