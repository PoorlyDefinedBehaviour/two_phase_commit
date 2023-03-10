//! The grpc server is used to send internal requests.
//! Prepare and commit requests are sent using grpc.

use std::{net::SocketAddr, sync::Arc};

use crate::{core_proto, transaction_manager::TransactionManager};
use anyhow::Result;
use core_proto::{
    AbortRequest, AbortResponse, CommitRequest, CommitResponse, PrepareCommitRequest,
    PrepareCommitResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;

#[tracing::instrument(name = "grpc_server::start", skip_all, fields(
    addr = ?addr
))]
pub async fn start(addr: SocketAddr, transaction_manager: Arc<TransactionManager>) -> Result<()> {
    let svc = core_proto::node_server::NodeServer::new(NodeService::new(transaction_manager));

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

pub struct NodeService {
    transaction_manager: Arc<TransactionManager>,
}

impl NodeService {
    #[tracing::instrument(name = "NodeService::new()", skip_all)]
    fn new(transaction_manager: Arc<TransactionManager>) -> Self {
        Self {
            transaction_manager,
        }
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
        match self
            .transaction_manager
            .prepare_commit(request.into_inner())
            .await
        {
            Err(err) => {
                error!(?err, "unable to handle prepare commit request");
                Err(Status::internal(err.to_string()))
            }
            Ok(ok) => Ok(Response::new(PrepareCommitResponse { ok })),
        }
    }

    #[tracing::instrument(name = "NodeService::abort", skip_all, fields(
        request = ?request
    ))]
    async fn abort(
        &self,
        request: Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        match self.transaction_manager.abort(request.into_inner()).await {
            Err(err) => {
                error!(?err, "unable to handle abort request");
                Err(Status::internal(err.to_string()))
            }
            Ok(()) => Ok(Response::new(AbortResponse { ok: true })),
        }
    }

    #[tracing::instrument(name = "NodeService::commit", skip_all, fields(
        request = ?request
    ))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        match self.transaction_manager.commit(request.into_inner()).await {
            Err(err) => {
                error!(?err, "unable to handle commit request");
                Err(Status::internal(err.to_string()))
            }
            Ok(()) => Ok(Response::new(CommitResponse { ok: true })),
        }
    }
}
