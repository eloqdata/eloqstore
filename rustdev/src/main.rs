//! EloqStore server binary

use clap::Parser;
use eloqstore_rs::{config::Config, EloqStore};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "eloqstore")]
#[command(about = "High-performance key-value storage engine", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "eloqstore.toml")]
    config: PathBuf,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    let filter = if args.verbose {
        "debug"
    } else {
        "info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    info!("Starting EloqStore v{}", eloqstore_rs::VERSION);

    // Load configuration
    let config = if args.config.exists() {
        let config_str = std::fs::read_to_string(&args.config)?;
        toml::from_str(&config_str)?
    } else {
        info!("No config file found, using default configuration");
        Config::default()
    };

    // Create and start the store
    let store = EloqStore::new(config)?;

    info!("EloqStore initialized, starting server...");
    store.start().await?;

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

    info!("Shutting down EloqStore...");
    store.stop().await?;

    info!("EloqStore shutdown complete");
    Ok(())
}