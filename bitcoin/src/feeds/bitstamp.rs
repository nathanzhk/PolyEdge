use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64};

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
        let Ok(message) = serde_json::from_str::<Value>(raw) else {
            return FeedEvent::Error("bitstamp failed to parse message".to_string());
        };

        match message.get("event").and_then(Value::as_str) {
            Some("data")
                if message.get("channel").and_then(Value::as_str) == Some(self.channel) =>
            {
                let Some(data) = message.get("data") else {
                    return FeedEvent::Ignore;
                };
                parse_order_book(data, received_at_ms)
                    .map(FeedEvent::Quote)
                    .unwrap_or(FeedEvent::Ignore)
            }
            Some("bts:subscription_succeeded") => {
                FeedEvent::Info(format!("bitstamp subscription succeeded {}", self.channel))
            }
            Some("bts:error") => FeedEvent::Error(format!("bitstamp error: {message}")),
            _ => FeedEvent::Ignore,
        }
    }
}

fn parse_order_book(data: &Value, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = data
        .get("microtimestamp")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value / 1000.0)
        .or_else(|| {
            data.get("timestamp")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<f64>().ok())
                .map(|value| value * 1000.0)
        })?;

    let best_bid = parse_f64(data.pointer("/bids/0/0"))?;
    let best_ask = parse_f64(data.pointer("/asks/0/0"))?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
