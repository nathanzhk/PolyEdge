use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

use crate::feeds::{FeedUpdate, Quote, now_unix_ms};

const MAX_DELAY_MS: f64 = 200.0;
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
        weight: 0.15,
    },
    WeightedExchange {
        name: "bitstamp",
        weight: 0.15,
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
    bybit: Option<Quote>,
    gemini: Option<Quote>,
    binance: Option<Quote>,
    bitstamp: Option<Quote>,
    coinbase: Option<Quote>,
}

impl LatestQuotes {
    fn update(&mut self, update: FeedUpdate) {
        match update.exchange {
            "bybit" => self.bybit = Some(update.quote),
            "gemini" => self.gemini = Some(update.quote),
            "binance" => self.binance = Some(update.quote),
            "bitstamp" => self.bitstamp = Some(update.quote),
            "coinbase" => self.coinbase = Some(update.quote),
            exchange => eprintln!("ignoring unknown exchange update: {exchange}"),
        }
    }

    fn get(&self, exchange: &str) -> Option<&Quote> {
        match exchange {
            "bybit" => self.bybit.as_ref(),
            "gemini" => self.gemini.as_ref(),
            "binance" => self.binance.as_ref(),
            "bitstamp" => self.bitstamp.as_ref(),
            "coinbase" => self.coinbase.as_ref(),
            _ => None,
        }
    }

    fn composite_price(&self) -> Option<f64> {
        let total_weight: f64 = EXCHANGES
            .iter()
            .filter_map(|exchange| self.eligible_quote(exchange.name).map(|_| exchange.weight))
            .sum();

        if total_weight == 0.0 {
            return None;
        }

        Some(
            EXCHANGES
                .iter()
                .filter_map(|exchange| {
                    self.eligible_quote(exchange.name)
                        .map(|quote| quote.mid() * exchange.weight / total_weight)
                })
                .sum(),
        )
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

        format!(
            "{local_timestamp_ms:.0} -> {}, {}, {}, {}, {} -> {composite}",
            self.format_exchange("bybit"),
            self.format_exchange("gemini"),
            self.format_exchange("binance"),
            self.format_exchange("bitstamp"),
            self.format_exchange("coinbase"),
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
) -> Result<()> {
    let mut state = LatestQuotes::default();
    let mut writer = open_log(&config.log_path).await?;

    while let Some(update) = updates.recv().await {
        state.update(update);
        let line = state.log_line(now_unix_ms());
        println!("{line}");
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }

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
