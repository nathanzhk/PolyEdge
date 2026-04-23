use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Map, Value, json};
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::{broadcast, mpsc},
};

use crate::exchanges::{FeedUpdate, Quote, now_unix_ms};

const MAX_DELAY_MS: f64 = 200.0;
const WINDOW_SECONDS: u64 = 300;
const EXCHANGES: [WeightedExchange; 5] = [
    WeightedExchange {
        name: "bybit",
        weight: 0.20,
    },
    WeightedExchange {
        name: "gemini",
        weight: 0.20,
    },
    WeightedExchange {
        name: "binance",
        weight: 0.25,
    },
    WeightedExchange {
        name: "bitstamp",
        weight: 0.05,
    },
    WeightedExchange {
        name: "coinbase",
        weight: 0.30,
    },
];

#[derive(Clone, Debug)]
pub struct AggregateConfig {
    pub log_path: PathBuf,
}

impl AggregateConfig {
    pub fn new(log_path: impl Into<PathBuf>) -> Self {
        Self {
            log_path: log_path.into(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WeightedExchange {
    name: &'static str,
    weight: f64,
}

#[derive(Default, Debug)]
struct LatestQuotes {
    quotes: HashMap<&'static str, Quote>,
}

impl LatestQuotes {
    fn update(&mut self, update: FeedUpdate) {
        self.quotes.insert(update.exchange, update.quote);
    }

    fn get(&self, exchange: &str) -> Option<&Quote> {
        self.quotes.get(exchange)
    }

    fn composite_price(&self) -> Option<f64> {
        let (total_weight, weighted_sum) = EXCHANGES.iter().fold((0.0, 0.0), |(tw, ws), ex| {
            match self.eligible_quote(ex.name) {
                Some(quote) => (tw + ex.weight, ws + quote.mid() * ex.weight),
                None => (tw, ws),
            }
        });

        if total_weight == 0.0 {
            return None;
        }

        Some(weighted_sum / total_weight)
    }

    fn eligible_quote(&self, exchange: &str) -> Option<&Quote> {
        let quote = self.get(exchange)?;
        let delay_ms = quote.delay_ms?;

        (delay_ms <= MAX_DELAY_MS).then_some(quote)
    }

    fn log_line(&self, local_timestamp_ms: f64) -> String {
        let composite = self
            .composite_price()
            .map(|price| format!("{price:.8}"))
            .unwrap_or_else(|| "NA".to_string());

        let exchanges: Vec<String> = EXCHANGES
            .iter()
            .map(|ex| self.format_exchange(ex.name))
            .collect();

        format!(
            "{local_timestamp_ms:.0} -> {} -> {composite}",
            exchanges.join(", ")
        )
    }

    fn format_exchange(&self, exchange: &str) -> String {
        match self.get(exchange) {
            Some(quote) => format!(
                "{} {:.8} ({})",
                exchange,
                quote.mid(),
                quote
                    .delay_ms
                    .map(|delay_ms| format!("{delay_ms:.2}ms"))
                    .unwrap_or_else(|| "NA".to_string())
            ),
            None => format!("{exchange} NA (NA)"),
        }
    }
}

pub async fn run_aggregator(
    mut updates: mpsc::Receiver<FeedUpdate>,
    config: AggregateConfig,
    dashboard_tx: broadcast::Sender<String>,
) -> Result<()> {
    let mut state = LatestQuotes::default();
    let mut current_window_start = None;
    let mut baseline: Option<PriceMap> = None;
    let mut baseline_composite: Option<f64> = None;
    let mut writer = open_log(&config.log_path).await?;

    while let Some(update) = updates.recv().await {
        let local_timestamp_ms = now_unix_ms();
        let window = Window::from_timestamp_ms(local_timestamp_ms);

        state.update(update);
        write_line(&mut writer, &state.log_line(local_timestamp_ms)).await?;

        let mut should_log_window = false;
        match current_window_start {
            None => {
                current_window_start = Some(window.start_seconds);
                if window.contains_start_second(local_timestamp_ms) {
                    baseline = Some(state.prices());
                    baseline_composite = state.composite_price();
                    should_log_window = true;
                }
            }
            Some(start) if start != window.start_seconds => {
                current_window_start = Some(window.start_seconds);
                baseline = Some(state.prices());
                should_log_window = true;
            }
            Some(_) => {}
        }

        if should_log_window {
            write_line(
                &mut writer,
                &state.window_log_line(window, local_timestamp_ms),
            )
            .await?;
        }

        let snapshot = state.dashboard_snapshot(local_timestamp_ms, window, baseline.as_ref(), baseline_composite);
        let _ = dashboard_tx.send(snapshot.to_string());
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct Window {
    start_seconds: u64,
    end_seconds: u64,
}

impl Window {
    fn from_timestamp_ms(timestamp_ms: f64) -> Self {
        let timestamp_seconds = (timestamp_ms / 1000.0).floor() as u64;
        let start_seconds = timestamp_seconds - (timestamp_seconds % WINDOW_SECONDS);

        Self {
            start_seconds,
            end_seconds: start_seconds + WINDOW_SECONDS,
        }
    }

    fn contains_start_second(&self, timestamp_ms: f64) -> bool {
        (timestamp_ms / 1000.0).floor() as u64 == self.start_seconds
    }
}

impl LatestQuotes {
    fn prices(&self) -> PriceMap {
        let mut prices = Map::new();

        for exchange in EXCHANGES {
            prices.insert(
                exchange.name.to_string(),
                price_value(self.get(exchange.name)),
            );
        }

        prices
    }

    fn exchanges_snapshot(&self, baseline: Option<&PriceMap>) -> Value {
        let mut exchanges = Map::new();

        for ex in EXCHANGES {
            let price = price_value(self.get(ex.name));
            let delay = self.get(ex.name)
                .and_then(|q| q.delay_ms)
                .map(Value::from)
                .unwrap_or(Value::Null);
            let change = price_number(Some(&price)).and_then(|p| {
                baseline
                    .and_then(|b| price_number(b.get(ex.name)))
                    .map(|b| p - b)
            });
            let baseline_price = baseline
                .and_then(|b| b.get(ex.name).cloned())
                .unwrap_or(Value::Null);

            exchanges.insert(ex.name.to_string(), json!({
                "price": price,
                "delay": delay,
                "change": change.map(Value::from).unwrap_or(Value::Null),
                "baseline": baseline_price,
            }));
        }

        Value::Object(exchanges)
    }

    fn dashboard_snapshot(
        &self,
        local_timestamp_ms: f64,
        window: Window,
        baseline: Option<&PriceMap>,
        baseline_composite: Option<f64>,
    ) -> Value {
        let composite_change = self.composite_price()
            .and_then(|price| baseline_composite.map(|b| price - b));

        json!({
            "timestamp": local_timestamp_ms,
            "window_start": window.start_seconds,
            "window_end": window.end_seconds,
            "price": self.composite_price().map(Value::from).unwrap_or(Value::Null),
            "change": composite_change.map(Value::from).unwrap_or(Value::Null),
            "baseline": baseline_composite.map(Value::from).unwrap_or(Value::Null),
            "exchanges": self.exchanges_snapshot(baseline),
        })
    }

    fn window_log_line(&self, window: Window, local_timestamp_ms: f64) -> String {
        let composite = self
            .composite_price()
            .map(|price| format!("{price:.8}"))
            .unwrap_or_else(|| "NA".to_string());

        let exchanges: Vec<String> = EXCHANGES
            .iter()
            .map(|ex| self.format_exchange(ex.name))
            .collect();

        format!(
            "WINDOW start={} end={} frozen_at={local_timestamp_ms:.0} -> {} -> {composite}",
            window.start_seconds,
            window.end_seconds,
            exchanges.join(", ")
        )
    }
}

type PriceMap = Map<String, Value>;

fn price_value(quote: Option<&Quote>) -> Value {
    quote
        .map(|quote| Value::from(quote.mid()))
        .unwrap_or(Value::Null)
}

fn price_number(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}


async fn write_line(writer: &mut BufWriter<tokio::fs::File>, line: &str) -> Result<()> {
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn open_log(path: &Path) -> Result<BufWriter<tokio::fs::File>> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    Ok(BufWriter::new(file))
}
