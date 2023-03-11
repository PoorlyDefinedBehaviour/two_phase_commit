use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use tracing::error;

use crate::{
    node_service::NodeService,
    storage::DiskLogStorage,
    transaction_manager::{Config, TransactionManager},
};

mod core_proto;
mod failure_sim;
mod grpc_server;
mod http_server;
mod node_service;
mod storage;
mod transaction_manager;

const MAX_CLUSTER_MEMBERS: usize = 3;

#[derive(Debug, Parser)]
struct Cli {
    /// The id of the process.
    #[arg(long)]
    id: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let cli = Cli::parse();
    assert!(
        cli.id < MAX_CLUSTER_MEMBERS,
        "process id must be less than {MAX_CLUSTER_MEMBERS}"
    );

    let http_server_port: u16 = format!("500{}", cli.id).parse()?;
    let grpc_server_addr = format!("[::1]:600{}", cli.id).parse().unwrap();

    let cluster_members: Vec<String> = (0..MAX_CLUSTER_MEMBERS)
        .into_iter()
        .filter(|id| *id != cli.id)
        .map(|id| format!("[::1]:600{id}",))
        .collect();

    let stable_storage = Arc::new(DiskLogStorage::new(&format!("./log/node_{}", cli.id)).await?);

    let config = Config {
        cluster_members,
        prepare_request_timeout: Duration::from_secs(2),
        abort_request_timeout: Duration::from_secs(2),
        commit_request_timeout: Duration::from_secs(2),
    };

    let transaction_manager = Arc::new(TransactionManager::new(
        NodeService::new(),
        stable_storage,
        config,
    ));

    {
        let transaction_manager = Arc::clone(&transaction_manager);
        tokio::spawn(async move {
            if let Err(err) = grpc_server::start(grpc_server_addr, transaction_manager).await {
                error!(?err, "unable to start grpc server");
                std::process::exit(1);
            }
        });
    }

    if let Err(err) = http_server::start(http_server_port, transaction_manager).await {
        error!(?err, "unable to start http server");
        std::process::exit(1);
    }

    Ok(())
}
