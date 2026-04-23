use chrono::DateTime;
use serde::Deserialize;
use serde_json::json;

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64_str};

#[derive(Clone, Debug)]
pub struct Coinbase {
    product_id: &'static str,
}

impl Default for Coinbase {
    fn default() -> Self {
        Self {
            product_id: "BTC-USD",
        }
    }
}

#[derive(Deserialize)]
struct TickerMessage {
    #[serde(rename = "type")]
    msg_type: String,
    product_id: Option<String>,
    time: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
    message: Option<String>,
}

impl ExchangeFeed for Coinbase {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    fn url(&self) -> &'static str {
        "wss://ws-feed.exchange.coinbase.com"
    }

    fn subscriptions(&self) -> Vec<String> {
        vec![
            json!({
                "type": "subscribe",
                "product_ids": [self.product_id],
                "channels": ["ticker"],
            })
            .to_string(),
        ]
    }

    fn handle_text(&self, raw: &str, received_at_ms: f64) -> FeedEvent {
        let Ok(msg) = serde_json::from_str::<TickerMessage>(raw) else {
            return FeedEvent::Error("coinbase failed to parse message".to_string());
        };

        match msg.msg_type.as_str() {
            "ticker" if msg.product_id.as_deref() == Some(self.product_id) => {
                parse_ticker(&msg, received_at_ms)
                    .map(FeedEvent::Quote)
                    .unwrap_or(FeedEvent::Ignore)
            }
            "error" => FeedEvent::Error(format!(
                "coinbase error: {}",
                msg.message.as_deref().unwrap_or("unknown error")
            )),
            _ => FeedEvent::Ignore,
        }
    }
}

fn parse_ticker(msg: &TickerMessage, received_at_ms: f64) -> Option<Quote> {
    let time = msg.time.as_deref()?;
    let timestamp_ms = DateTime::parse_from_rfc3339(time).ok()?.timestamp_millis() as f64;
    let best_bid = parse_f64_str(msg.best_bid.as_deref()?)?;
    let best_ask = parse_f64_str(msg.best_ask.as_deref()?)?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
