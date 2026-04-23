use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64_str};

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

#[derive(Deserialize)]
struct OrderbookMessage {
    topic: Option<String>,
    op: Option<String>,
    success: Option<bool>,
    ret_msg: Option<String>,
    ts: Option<f64>,
    data: Option<OrderbookData>,
}

#[derive(Deserialize)]
struct OrderbookData {
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
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
        let Ok(msg) = serde_json::from_str::<OrderbookMessage>(raw) else {
            return FeedEvent::Error("bybit failed to parse message".to_string());
        };

        if msg.topic.as_deref() == Some(self.topic) {
            return parse_orderbook_top(&msg, received_at_ms)
                .map(FeedEvent::Quote)
                .unwrap_or(FeedEvent::Ignore);
        }

        if msg.op.as_deref() == Some("subscribe") && msg.success == Some(false) {
            return FeedEvent::Error(format!(
                "bybit subscribe error: {}",
                msg.ret_msg.as_deref().unwrap_or("unknown error")
            ));
        }

        if msg.op.as_deref() == Some("pong")
            || msg.ret_msg.as_deref() == Some("pong")
        {
            return FeedEvent::Ignore;
        }

        FeedEvent::Ignore
    }
}

fn parse_orderbook_top(msg: &OrderbookMessage, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = msg.ts?;
    let data = msg.data.as_ref()?;
    let best_bid = parse_f64_str(data.b.first()?.first()?)?;
    let best_ask = parse_f64_str(data.a.first()?.first()?)?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}
