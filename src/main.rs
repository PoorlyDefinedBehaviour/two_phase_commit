use anyhow::Result;
use clap::Parser;
use tracing::error;

use crate::transaction_manager::TransactionManager;

mod core_proto;
mod failure_sim;
mod grpc_server;
mod http_server;
mod storage;
mod transaction_manager;

#[derive(Debug, Parser)]
struct Cli {
    /// The id of the process.
    #[arg(long)]
    id: usize,
}

#[derive(Debug)]
struct Config {
    cluster_members: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    assert!(cli.id <= 5, "process id cannot be greater than 5");

    let http_server_port: u16 = format!("500{}", cli.id).parse()?;
    let grpc_server_addr = format!("[::1]:600{}", cli.id).parse().unwrap();

    let config = Config {
        cluster_members: vec![],
    };

    let node_services = todo!();

    let transaction_manager = TransactionManager::new(node_services);

    tokio::spawn(async move {
        if let Err(err) = grpc_server::start(grpc_server_addr).await {
            error!(?err, "unable to start grpc server");
            std::process::exit(1);
        }
    });

    tokio::spawn(async move {
        if let Err(err) = http_server::start(transaction_manager, http_server_port).await {
            error!(?err, "unable to start http server");
            std::process::exit(1);
        }
    });

    Ok(())
}
