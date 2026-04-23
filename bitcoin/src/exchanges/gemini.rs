use serde::Deserialize;
use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64_str};

#[derive(Clone, Debug)]
pub struct Gemini {
    symbol: &'static str,
    stream: &'static str,
}

impl Default for Gemini {
    fn default() -> Self {
        Self {
            symbol: "btcusd",
            stream: "btcusd@bookTicker",
        }
    }
}

#[derive(Deserialize)]
struct BookTickerMessage {
    s: Option<String>,
    #[serde(rename = "E")]
    event_time: Option<u64>,
    b: Option<String>,
    a: Option<String>,
    error: Option<Value>,
}

impl ExchangeFeed for Gemini {
    fn name(&self) -> &'static str {
        "gemini"
    }

    fn url(&self) -> &'static str {
        "wss://ws.gemini.com"
    }

    fn subscriptions(&self) -> Vec<String> {
        vec![
            json!({
                "id": "1",
                "method": "subscribe",
                "params": [self.stream],
            })
            .to_string(),
        ]
    }

    fn handle_text(&self, raw: &str, received_at_ms: f64) -> FeedEvent {
        let Ok(msg) = serde_json::from_str::<BookTickerMessage>(raw) else {
            return FeedEvent::Error("gemini failed to parse message".to_string());
        };

        if msg.s.as_deref() == Some(self.symbol)
            && msg.b.is_some()
            && msg.a.is_some()
        {
            return parse_book_ticker(&msg, received_at_ms)
                .map(FeedEvent::Quote)
                .unwrap_or(FeedEvent::Ignore);
        }

        if let Some(error) = &msg.error {
            return FeedEvent::Error(format!("gemini error: {error}"));
        }

        FeedEvent::Ignore
    }
}

fn parse_book_ticker(msg: &BookTickerMessage, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = msg.event_time? as f64 / 1_000_000.0;
    let best_bid = parse_f64_str(msg.b.as_deref()?)?;
    let best_ask = parse_f64_str(msg.a.as_deref()?)?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
