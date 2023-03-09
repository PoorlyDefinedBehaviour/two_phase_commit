use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use prost::Message;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request};
use tracing::error;
use uuid::Uuid;

use crate::{
    core_proto::{
        node_client::NodeClient, AbortRequest, CommitRequest, CommitTransactionDecision, HostAddr,
        PrepareCommitRequest, TransactionCommitted, HOST_ADDR_HEADER_KEY,
    },
    storage::StableStorage,
};

#[derive(Debug)]
pub struct Config {
    /// How long to wait before an in-flight prepare request is canceled if a response is not received.
    pub prepare_request_timeout: Duration,

    /// How long to wait before an in-flight commit request is canceled if a response is not received.
    pub commit_request_timeout: Duration,
}

/// Responsible for requesting the participaints in a transaction to
/// prepare to commit and then to commit if possible.
pub struct TransactionManager {
    node_services: Mutex<HashMap<HostAddr, NodeClient<Channel>>>,
    stable_storage: Arc<dyn StableStorage>,
    config: Config,
}

impl TransactionManager {
    #[tracing::instrument(name = "Manager::new", skip_all, fields(
        config = ?config
    ))]
    pub fn new(
        node_services: HashMap<HostAddr, NodeClient<Channel>>,
        stable_storage: Arc<dyn StableStorage>,
        config: Config,
    ) -> Self {
        Self {
            node_services: Mutex::new(node_services),
            stable_storage,
            config,
        }
    }

    #[tracing::instrument(name = "Manager::handle_request", skip_all, fields(
        op_id,
        op = op
    ))]
    pub async fn handle_request(&self, op: u8) -> Result<()> {
        let mut node_services = self.node_services.lock().await;

        let op_id = Uuid::new_v4().to_string();
        tracing::Span::current().record("op_id", &op_id);

        let prepare_commit_request = PrepareCommitRequest {
            id: op_id.clone(),
            op: op as i32,
        };

        // Check if every participant is willing to go ahead with the transaction.
        let responses = futures::future::join_all(node_services.iter_mut().map(
            |(_host_addr, node_service)| {
                let mut request = Request::new(prepare_commit_request.clone());
                request.set_timeout(self.config.prepare_request_timeout);
                node_service.prepare_commit(request)
            },
        ))
        .await;

        let mut ok_responses = Vec::new();

        for result in responses {
            if let Ok(response) = result {
                let host_addr = response
                    .metadata()
                    .get(HOST_ADDR_HEADER_KEY)
                    .expect("bug: response is missing host address")
                    .to_str()
                    .expect("bug: host address is not a string")
                    .to_owned();

                if response.into_inner().ok {
                    ok_responses.push(host_addr);
                }
            }
        }

        // If not all participants are willing to commit, abort the transaction.
        if ok_responses.len() != node_services.len() {
            for host_addr in ok_responses {
                let node_service = node_services
                    .get_mut(&host_addr)
                    .expect("bug: every host address must exist in the node services map");

                // If we fail to abort, just ignore the error. The participant will poll
                // the transaction manager at a later time and find out that the transaction
                // has been aborted because the manager won't remember the transaction.
                if let Err(err) = node_service.abort(AbortRequest { id: op_id.clone() }).await {
                    error!(?err, id = ?op_id, "unable to abort request");
                };
            }

            let err = anyhow!("not all participants are willing to go ahead with the transaction");
            error!(?err);
            return Err(err);
        }

        // Store the decision in stable storage.
        let decision = CommitTransactionDecision { id: op_id.clone() };
        let mut buffer = Vec::new();
        decision
            .encode(&mut buffer)
            .context("encoding commit transaction decision")?;
        self.stable_storage
            .append(op_id.as_bytes(), &buffer)
            .await?;
        self.stable_storage.flush().await?;

        // Ask every participant to commit the transaction.
        let commit_request = CommitRequest { id: op_id.clone() };

        let responses = futures::future::join_all(node_services.iter_mut().map(
            |(_host_addr, node_service)| {
                let mut request = Request::new(commit_request.clone());
                request.set_timeout(self.config.commit_request_timeout);
                node_service.commit(request)
            },
        ))
        .await;

        let every_participant_committed = responses.into_iter().all(|result| {
            result
                .map(|response| response.into_inner().ok)
                .unwrap_or(false)
        });

        if every_participant_committed {
            let transaction_commited = TransactionCommitted { id: op_id.clone() };
            let mut buffer = Vec::new();
            transaction_commited.encode(&mut buffer)?;

            // There's no need to flush this one since the commit decision has already been flushed.
            self.stable_storage
                .append(op_id.as_bytes(), &buffer)
                .await?;
        }

        Ok(())
    }
}
