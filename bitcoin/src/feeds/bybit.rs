use std::time::Duration;

use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64};

#[derive(Clone, Debug)]
pub struct Bybit {
    topic: &'static str,
}

impl Default for Bybit {
    fn default() -> Self {
        Self {
            topic: "orderbook.1.BTCUSDT",
        }
    }
}

impl ExchangeFeed for Bybit {
    fn name(&self) -> &'static str {
        "bybit"
    }

    fn url(&self) -> &'static str {
        "wss://stream.bybit.com/v5/public/spot"
    }

    fn subscriptions(&self) -> Vec<String> {
        vec![
            json!({
                "op": "subscribe",
                "args": [self.topic],
            })
            .to_string(),
        ]
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(20))
    }

    fn heartbeat_message(&self) -> Option<String> {
        Some(json!({ "op": "ping" }).to_string())
    }

    fn handle_text(&self, raw: &str, received_at_ms: f64) -> FeedEvent {
        let Ok(message) = serde_json::from_str::<Value>(raw) else {
            return FeedEvent::Error("bybit failed to parse message".to_string());
        };

        if message.get("topic").and_then(Value::as_str) == Some(self.topic) {
            return parse_orderbook_top(&message, received_at_ms)
                .map(FeedEvent::Quote)
                .unwrap_or(FeedEvent::Ignore);
        }

        if message.get("op").and_then(Value::as_str) == Some("subscribe")
            && message.get("success").and_then(Value::as_bool) == Some(false)
        {
            return FeedEvent::Error(format!(
                "bybit subscribe error: {}",
                message
                    .get("ret_msg")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }

        if message.get("op").and_then(Value::as_str) == Some("pong")
            || message.get("ret_msg").and_then(Value::as_str) == Some("pong")
        {
            return FeedEvent::Ignore;
        }

        FeedEvent::Ignore
    }
}

fn parse_orderbook_top(message: &Value, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = parse_f64(message.get("ts"))?;
    let best_bid = parse_f64(message.pointer("/data/b/0/0"))?;
    let best_ask = parse_f64(message.pointer("/data/a/0/0"))?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
