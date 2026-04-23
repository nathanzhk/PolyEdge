mod aggregate;
mod dashboard;
mod exchanges;

use std::net::SocketAddr;

use aggregate::{AggregateConfig, run_aggregator};
use anyhow::Result;
use clap::Parser;
use dashboard::run_dashboard;
use exchanges::{FeedConfig, run_feed};
use exchanges::{
    binance::Binance,
    bitstamp::Bitstamp,
    bybit::Bybit,
    coinbase::Coinbase,
    gemini::Gemini,
};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

const DEFAULT_LOG_PATH: &str = "logs/aggregate.log";
const DASHBOARD_ADDR: &str = "127.0.0.1:8080";

#[derive(Parser)]
#[command(name = "bitcoin", about = "BTC exchange composite price tracker")]
struct Args {
    #[arg(long)]
    dashboard: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let file_appender = tracing_appender::rolling::daily("logs", "tracing.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    let args = Args::parse();
    let feed_config = FeedConfig::default();
    let aggregate_config = AggregateConfig::new(DEFAULT_LOG_PATH);

    let result = run(feed_config, aggregate_config, args.dashboard).await;
    drop(_guard);
    result
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
    spawn_feed(Binance, feed_config.clone(), updates_tx.clone());
    spawn_feed(Bitstamp::default(), feed_config.clone(), updates_tx.clone());
    spawn_feed(Coinbase::default(), feed_config, updates_tx);

    tokio::signal::ctrl_c().await?;
    info!("received ctrl-c, shutting down");
    aggregator.abort();
    if let Some(dashboard) = dashboard {
        dashboard.abort();
    }
    Ok(())
}

fn spawn_feed<F>(feed: F, config: FeedConfig, updates: mpsc::Sender<exchanges::FeedUpdate>)
where
    F: exchanges::ExchangeFeed,
{
    tokio::spawn(async move {
        if let Err(error) = run_feed(feed, config, updates).await {
            tracing::error!("feed task exited: {error:#}");
        }
    });
}
