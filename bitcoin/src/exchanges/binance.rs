use serde::Deserialize;

use super::{ExchangeFeed, FeedEvent, Quote, finite_or_none, parse_f64_str};

#[derive(Clone, Debug)]
pub struct Binance;

impl Default for Binance {
    fn default() -> Self {
        Self
    }
}

#[derive(Deserialize)]
struct BookTicker {
    b: String,
    a: String,
}

impl ExchangeFeed for Binance {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn url(&self) -> &'static str {
        "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
    }

    fn subscriptions(&self) -> Vec<String> {
        Vec::new()
    }

    fn handle_text(&self, raw: &str, _received_at_ms: f64) -> FeedEvent {
        let Ok(msg) = serde_json::from_str::<BookTicker>(raw) else {
            return FeedEvent::Error("binance failed to parse message".to_string());
        };

        let best_bid = parse_f64_str(&msg.b);
        let best_ask = parse_f64_str(&msg.a);

        match (best_bid, best_ask) {
            (Some(bid), Some(ask))
                if finite_or_none(bid).is_some() && finite_or_none(ask).is_some() =>
            {
                FeedEvent::Quote(Quote::new(bid, ask, 0.0))
            }
            _ => FeedEvent::Ignore,
        }
    }
}
