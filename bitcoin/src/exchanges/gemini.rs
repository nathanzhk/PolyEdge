use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64};

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
        let Ok(message) = serde_json::from_str::<Value>(raw) else {
            return FeedEvent::Error("gemini failed to parse message".to_string());
        };

        if message.get("s").and_then(Value::as_str) == Some(self.symbol)
            && message.get("b").is_some()
            && message.get("a").is_some()
        {
            return parse_book_ticker(&message, received_at_ms)
                .map(FeedEvent::Quote)
                .unwrap_or(FeedEvent::Ignore);
        }

        if let Some(error) = message.get("error") {
            return FeedEvent::Error(format!("gemini error: {error}"));
        }

        FeedEvent::Ignore
    }
}

fn parse_book_ticker(message: &Value, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = message.get("E")?.as_u64()? as f64 / 1_000_000.0;
    let best_bid = parse_f64(message.get("b"))?;
    let best_ask = parse_f64(message.get("a"))?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
