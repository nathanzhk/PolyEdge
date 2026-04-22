mod aggregate;
mod feeds;

use aggregate::{AggregateConfig, run_aggregator};
use anyhow::{Result, bail};
use feeds::{FeedConfig, run_feed};
#[rustfmt::skip]
use feeds::{
    binance::Binance,
    bitstamp::Bitstamp,
    coinbase::Coinbase,
    bybit::Bybit,
    gemini::Gemini,
};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    let feed_config = FeedConfig::default();
    let aggregate_config = AggregateConfig::new(args.log_path);

    run(args.exchange.as_deref(), feed_config, aggregate_config).await
}

async fn run(
    exchange: Option<&str>,
    feed_config: FeedConfig,
    aggregate_config: AggregateConfig,
) -> Result<()> {
    let (updates_tx, updates_rx) = mpsc::channel(4096);
    let aggregator = tokio::spawn(run_aggregator(updates_rx, aggregate_config));

    match exchange {
        Some("bybit") => spawn_feed(Bybit::default(), feed_config, updates_tx),
        Some("gemini") => spawn_feed(Gemini::default(), feed_config, updates_tx),
        Some("binance") => spawn_feed(Binance::default(), feed_config, updates_tx),
        Some("bitstamp") => spawn_feed(Bitstamp::default(), feed_config, updates_tx),
        Some("coinbase") => spawn_feed(Coinbase::default(), feed_config, updates_tx),
        Some(exchange) => bail!("unknown exchange '{exchange}'"),
        None => {
            spawn_feed(Bybit::default(), feed_config.clone(), updates_tx.clone());
            spawn_feed(Gemini::default(), feed_config.clone(), updates_tx.clone());
            spawn_feed(Binance::default(), feed_config.clone(), updates_tx.clone());
            spawn_feed(Bitstamp::default(), feed_config.clone(), updates_tx.clone());
            spawn_feed(Coinbase::default(), feed_config, updates_tx);
        }
    };

    tokio::signal::ctrl_c().await?;
    eprintln!("received ctrl-c, shutting down");
    aggregator.abort();
    Ok(())
}

fn spawn_feed<F>(feed: F, config: FeedConfig, updates: mpsc::Sender<feeds::FeedUpdate>)
where
    F: feeds::ExchangeFeed,
{
    tokio::spawn(async move {
        if let Err(error) = run_feed(feed, config, updates).await {
            eprintln!("feed task exited: {error:#}");
        }
    });
}

#[derive(Debug)]
struct Args {
    exchange: Option<String>,
    log_path: String,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut exchange = None;
        let mut log_path = "exp/aggregate.log".to_string();
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--exchange" | "-e" => {
                    exchange = args.next();
                    if exchange.is_none() {
                        bail!("{arg} requires an exchange name");
                    }
                }
                "--log-path" | "-l" => {
                    log_path = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} requires a log path"))?;
                }
                "--help" | "-h" => {
                    println!(
                        "Usage: bitcoin [--exchange bybit|gemini|binance|bitstamp|coinbase] [--log-path FILE]"
                    );
                    std::process::exit(0);
                }
                exchange_name if exchange.is_none() => {
                    exchange = Some(exchange_name.to_string());
                }
                _ => bail!("unexpected argument '{arg}'"),
            }
        }

        Ok(Self { exchange, log_path })
    }
}
