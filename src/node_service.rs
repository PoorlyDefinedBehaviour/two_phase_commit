use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request, Response};

use crate::core_proto::{
    node_client::NodeClient, AbortRequest, AbortResponse, CommitRequest, CommitResponse, HostAddr,
    PrepareCommitRequest, PrepareCommitResponse,
};

pub struct NodeService {
    /// Caches clients used to connect to other nodes to avoid
    /// the overhead of reconnecting every time.
    clients: Mutex<HashMap<String, NodeClient<Channel>>>,
}

impl NodeService {
    #[tracing::instrument(name = "NodeService::new", skip_all)]
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
        }
    }

    #[tracing::instrument(name = "NodeService::prepare_commit", skip_all, fields(
        host_addr = ?host_addr,
        request= ?request
    ))]
    pub async fn prepare_commit(
        &self,
        host_addr: HostAddr,
        request: Request<PrepareCommitRequest>,
    ) -> Result<Response<PrepareCommitResponse>> {
        // TODO: should handle reconnects

        let mut clients = self.clients.lock().await;

        let mut client = match clients.get(&host_addr).cloned() {
            None => {
                let client = NodeClient::connect(host_addr.clone()).await?;
                clients.insert(host_addr, client.clone());
                client
            }
            Some(client) => client,
        };

        let response = client.prepare_commit(request).await?;

        Ok(response)
    }

    #[tracing::instrument(name = "NodeService::commit", skip_all, fields(
        host_addr = ?host_addr,
        request= ?request
    ))]
    pub async fn commit(
        &self,
        host_addr: HostAddr,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>> {
        // TODO: should handle reconnects

        let mut clients = self.clients.lock().await;

        let mut client = match clients.get(&host_addr).cloned() {
            None => {
                let client = NodeClient::connect(host_addr.clone()).await?;
                clients.insert(host_addr, client.clone());
                client
            }
            Some(client) => client,
        };

        let response = client.commit(request).await?;

        Ok(response)
    }

    #[tracing::instrument(name = "NodeService::abort", skip_all, fields(
        host_addr = ?host_addr,
        request= ?request
    ))]
    pub async fn abort(
        &self,
        host_addr: HostAddr,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>> {
        // TODO: should handle reconnects

        let mut clients = self.clients.lock().await;

        let mut client = match clients.get(&host_addr).cloned() {
            None => {
                let client = NodeClient::connect(host_addr.clone()).await?;
                clients.insert(host_addr, client.clone());
                client
            }
            Some(client) => client,
        };

        let response = client.abort(request).await?;

        Ok(response)
    }
}
