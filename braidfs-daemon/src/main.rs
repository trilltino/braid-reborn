use braid_core::fs;
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(name = "braidfs-daemon")]
#[command(about = "BraidFS Sync Daemon")]
struct Cli {
    #[arg(short, long, default_value = "45678")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!("=== BraidFS Daemon [crate: braidfs-daemon] ===");
    info!("Role: Core Braid Protocol & Sync Node");
    info!("Listening on port {}...", cli.port);

    fs::run_daemon(cli.port).await?;
    Ok(())
}
