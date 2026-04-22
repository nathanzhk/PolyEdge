use serde_json::{Value, json};

use super::{ExchangeFeed, FeedEvent, Quote, parse_f64};

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
        let Ok(message) = serde_json::from_str::<Value>(raw) else {
            return FeedEvent::Error("coinbase failed to parse message".to_string());
        };

        match message.get("type").and_then(Value::as_str) {
            Some("ticker")
                if message.get("product_id").and_then(Value::as_str) == Some(self.product_id) =>
            {
                parse_ticker(&message, received_at_ms)
                    .map(FeedEvent::Quote)
                    .unwrap_or(FeedEvent::Ignore)
            }
            Some("error") => FeedEvent::Error(format!(
                "coinbase error: {}",
                message
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            )),
            _ => FeedEvent::Ignore,
        }
    }
}

fn parse_ticker(message: &Value, received_at_ms: f64) -> Option<Quote> {
    let timestamp_ms = parse_coinbase_time(message.get("time")?.as_str()?)?;
    let best_bid = parse_f64(message.get("best_bid"))?;
    let best_ask = parse_f64(message.get("best_ask"))?;

    Some(Quote::with_delay(
        timestamp_ms,
        best_bid,
        best_ask,
        received_at_ms,
    ))
}

fn parse_coinbase_time(time: &str) -> Option<f64> {
    let (date_time, fraction) = time.split_once('.')?;
    let mut fraction = fraction.trim_end_matches('Z').to_string();
    fraction.truncate(3);
    while fraction.len() < 3 {
        fraction.push('0');
    }

    let seconds = parse_utc_seconds(date_time)?;
    let millis = fraction.parse::<u64>().ok()?;
    Some((seconds * 1000 + millis) as f64)
}

fn parse_utc_seconds(date_time: &str) -> Option<u64> {
    let (date, time) = date_time.split_once('T')?;
    let mut date = date.split('-').map(|part| part.parse::<u64>().ok());
    let year = date.next()?? as i32;
    let month = date.next()??;
    let day = date.next()??;

    let mut time = time.split(':').map(|part| part.parse::<u64>().ok());
    let hour = time.next()??;
    let minute = time.next()??;
    let second = time.next()??;

    Some(days_from_civil(year, month, day)? * 86_400 + hour * 3600 + minute * 60 + second)
}

fn days_from_civil(year: i32, month: u64, day: u64) -> Option<u64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let year = year - (month <= 2) as i32;
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u64;
    let month_prime = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era as i64 * 146_097 + day_of_era as i64 - 719_468;

    (days >= 0).then_some(days as u64)
}
