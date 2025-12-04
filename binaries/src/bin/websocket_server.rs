#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;

use clap::{ArgAction, Parser};
use server::{run_websocket_server, Result};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,

    /// When enabled (default), snapshot mismatches are logged but do not stop the server.
    /// Can also be configured via the `SNAPSHOT_TOLERANT` environment variable.
    #[arg(long, env = "SNAPSHOT_TOLERANT", default_value_t = true, action = ArgAction::Set)]
    snapshot_tolerant: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    run_websocket_server(&full_address, true, compression_level, args.snapshot_tolerant).await?;

    Ok(())
}
