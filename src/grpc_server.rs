//! The grpc server is used to send internal requests.
//! Prepare and commit requests are sent using grpc.

use std::net::SocketAddr;

use crate::core_proto;
use anyhow::Result;
use core_proto::{CommitRequest, CommitResponse, PrepareCommitRequest, PrepareCommitResponse};
use tonic::{transport::Server, Request, Response, Status};

#[tracing::instrument(name = "grpc_server::start", skip_all, fields(
    addr = ?addr
))]
pub async fn start(addr: SocketAddr) -> Result<()> {
    let svc = core_proto::node_server::NodeServer::new(NodeService::new());

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

pub struct NodeService {}

impl NodeService {
    #[tracing::instrument(name = "NodeService::new()", skip_all)]
    fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl core_proto::core_proto::node_server::Node for NodeService {
    #[tracing::instrument(name = "NodeService::prepare_commit", skip_all, fields(
        request = ?request
    ))]
    async fn prepare_commit(
        &self,
        request: Request<PrepareCommitRequest>,
    ) -> Result<Response<PrepareCommitResponse>, Status> {
        todo!()
    }

    #[tracing::instrument(name = "NodeService::commit", skip_all, fields(
        request = ?request
    ))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        todo!()
    }
}
