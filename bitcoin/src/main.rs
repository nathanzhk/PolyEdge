mod aggregate;
mod dashboard;
mod feeds;

use std::net::SocketAddr;

use aggregate::{AggregateConfig, run_aggregator};
use anyhow::{Result, bail};
use dashboard::run_dashboard;
use feeds::{FeedConfig, run_feed};
#[rustfmt::skip]
use feeds::{
    binance::Binance,
    bitstamp::Bitstamp,
    coinbase::Coinbase,
    bybit::Bybit,
    gemini::Gemini,
};
use tokio::sync::{broadcast, mpsc};

const DEFAULT_LOG_PATH: &str = "logs/aggregate.log";
const DASHBOARD_ADDR: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse()?;
    let feed_config = FeedConfig::default();
    let aggregate_config = AggregateConfig::new(DEFAULT_LOG_PATH);

    run(feed_config, aggregate_config, args.enable_dashboard).await
}

async fn run(
    feed_config: FeedConfig,
    aggregate_config: AggregateConfig,
    enable_dashboard: bool,
) -> Result<()> {
    let (updates_tx, updates_rx) = mpsc::channel(4096);
    let (dashboard_tx, _) = broadcast::channel(4096);
    let aggregator = tokio::spawn(run_aggregator(
        updates_rx,
        aggregate_config,
        dashboard_tx.clone(),
    ));

    let dashboard = if enable_dashboard {
        let dashboard_addr: SocketAddr = DASHBOARD_ADDR.parse()?;
        Some(tokio::spawn(run_dashboard(dashboard_addr, dashboard_tx)))
    } else {
        None
    };

    spawn_feed(Bybit::default(), feed_config.clone(), updates_tx.clone());
    spawn_feed(Gemini::default(), feed_config.clone(), updates_tx.clone());
    spawn_feed(Binance::default(), feed_config.clone(), updates_tx.clone());
    spawn_feed(Bitstamp::default(), feed_config.clone(), updates_tx.clone());
    spawn_feed(Coinbase::default(), feed_config, updates_tx);

    tokio::signal::ctrl_c().await?;
    eprintln!("received ctrl-c, shutting down");
    aggregator.abort();
    if let Some(dashboard) = dashboard {
        dashboard.abort();
    }
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
    enable_dashboard: bool,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut enable_dashboard = false;
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dashboard" => enable_dashboard = true,
                "--help" | "-h" => {
                    println!("usage: bitcoin [--dashboard]");
                    std::process::exit(0);
                }
                _ => bail!("unexpected argument '{arg}'"),
            }
        }

        Ok(Self { enable_dashboard })
    }
}
