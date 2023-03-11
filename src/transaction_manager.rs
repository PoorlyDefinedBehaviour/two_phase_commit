use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use prost::Message;

use tonic::Request;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    core_proto::{
        AbortRequest, CommitRequest, HostAddr, PrepareCommitRequest, TransactionDecision,
    },
    failure_sim,
    node_service::NodeService,
    storage::StableStorage,
};

// TODO: should use an enum so the compiler errors when a case is not checked.
const TRANSACTION_STATE_DECIDED_TO_COMMIT: i32 = 1;
const TRANSACTION_STATE_COMMITTED: i32 = 2;
const TRANSACTION_STATE_ABORTED: i32 = 3;

#[derive(Debug)]
pub struct Config {
    /// Addresses of members of the cluster.
    pub cluster_members: Vec<HostAddr>,

    /// How long to wait before an in-flight prepare request is canceled if a response is not received.
    pub prepare_request_timeout: Duration,

    /// How long to wait before an in-flight abort request is canceled if a response is not received.
    pub abort_request_timeout: Duration,

    /// How long to wait before an in-flight commit request is canceled if a response is not received.
    pub commit_request_timeout: Duration,
}

/// Responsible for requesting the participaints in a transaction to
/// prepare to commit and then to commit if possible.
pub struct TransactionManager {
    node_service: NodeService,
    stable_storage: Arc<dyn StableStorage>,
    config: Config,
}

impl TransactionManager {
    #[tracing::instrument(name = "Manager::new", skip_all, fields(
        config = ?config
    ))]
    pub fn new(
        node_service: NodeService,
        stable_storage: Arc<dyn StableStorage>,
        config: Config,
    ) -> Self {
        Self {
            node_service,
            stable_storage,
            config,
        }
    }

    #[tracing::instrument(name = "Manager::handle_request", skip_all, fields(
        op_id,
        op = op
    ))]
    pub async fn handle_request(&self, op: u8) -> Result<()> {
        let op_id = Uuid::new_v4().to_string();
        tracing::Span::current().record("op_id", &op_id);

        let prepare_commit_request = PrepareCommitRequest {
            id: op_id.clone(),
            op: op as i32,
        };

        // Check if every participant is willing to go ahead with the transaction.
        let responses =
            futures::future::join_all(self.config.cluster_members.clone().into_iter().map(
                |host_addr| async {
                    let mut request = Request::new(prepare_commit_request.clone());
                    request.set_timeout(self.config.prepare_request_timeout);
                    let host_addr_clone = host_addr.clone();
                    (
                        host_addr,
                        self.node_service
                            .prepare_commit(host_addr_clone, request)
                            .await,
                    )
                },
            ))
            .await;

        let mut ok_responses = Vec::new();

        for (host_addr, result) in responses {
            match result {
                Err(err) => {
                    error!(?host_addr, ?err, "error response to prepare commit request");
                }
                Ok(response) => {
                    if response.into_inner().ok {
                        ok_responses.push(host_addr);
                    } else {
                        info!(
                            ?host_addr,
                            "node is not willing to go ahead with the transaction"
                        );
                    }
                }
            }
        }

        // If not all participants are willing to commit, abort the transaction.
        if ok_responses.len() != self.config.cluster_members.len() {
            for host_addr in ok_responses {
                // If we fail to abort, just ignore the error. The participant will poll
                // the transaction manager at a later time and find out that the transaction
                // has been aborted because the manager won't remember the transaction.
                let mut request = Request::new(AbortRequest { id: op_id.clone() });
                request.set_timeout(self.config.abort_request_timeout);

                if let Err(err) = self.node_service.abort(host_addr.clone(), request).await {
                    error!(?err, id = ?op_id, ?host_addr, "unable to abort request");
                };
            }

            let err = anyhow!("not all participants are willing to go ahead with the transaction");
            error!(?err);
            return Err(err);
        }

        // Store the decision in stable storage.
        let decision = TransactionDecision {
            id: op_id.clone(),
            state: TRANSACTION_STATE_DECIDED_TO_COMMIT,
        };
        let mut buffer = Vec::new();
        decision
            .encode(&mut buffer)
            .context("encoding commit transaction decision")?;
        self.stable_storage
            .append(op_id.as_bytes().to_vec(), buffer)
            .await?;
        self.stable_storage.flush().await?;

        // Ask every participant to commit the transaction.
        let commit_request = CommitRequest { id: op_id.clone() };

        let responses =
            futures::future::join_all(self.config.cluster_members.clone().into_iter().map(
                |host_addr| async {
                    let mut request = Request::new(commit_request.clone());
                    request.set_timeout(self.config.commit_request_timeout);
                    (
                        host_addr.clone(),
                        self.node_service.commit(host_addr, request).await,
                    )
                },
            ))
            .await;

        let every_participant_committed =
            responses
                .into_iter()
                .all(|(host_addr, result)| match result {
                    Err(err) => {
                        error!(
                            ?host_addr,
                            ?err,
                            "participant returned error response for commit request"
                        );
                        false
                    }
                    Ok(response) => {
                        info!(?host_addr, "participant did not commit transaction");
                        response.into_inner().ok
                    }
                });

        if !every_participant_committed {
            return Err(anyhow!("transaction wasn't committed by every participant"));
        }

        // Every participant committed the transaction.
        let transaction_commited = TransactionDecision {
            id: op_id.clone(),
            state: TRANSACTION_STATE_COMMITTED,
        };
        let mut buffer = Vec::new();
        transaction_commited.encode(&mut buffer)?;

        // There's no need to flush this one since the commit decision has already been flushed.
        self.stable_storage
            .append(op_id.as_bytes().to_vec(), buffer)
            .await?;

        Ok(())
    }

    /// Handles a request to prepare to commit when acting as a participant.
    #[tracing::instrument(name = "TransactionManager::prepare_commit", skip_all, fields(
        request = ?request
    ))]
    pub async fn prepare_commit(&self, request: PrepareCommitRequest) -> Result<bool> {
        if failure_sim::prepare_commit_should_fail() {
            return Err(anyhow!(
                "simulating(ERROR): prepare commit fails because participant is in error mode"
            ));
        }
        if failure_sim::participant_should_not_be_able_to_commit() {
            info!("simulating(returns false): participant rejects prepare commit request");
            return Ok(false);
        }

        // Assuming the participant can proceed with the trasaction.
        let commit_decision = TransactionDecision {
            id: request.id.clone(),
            state: TRANSACTION_STATE_DECIDED_TO_COMMIT,
        };

        // Store the decision in stable storage so it can be remembed when a crash happens.
        let mut buffer = Vec::new();
        commit_decision.encode(&mut buffer)?;
        self.stable_storage
            .append(commit_decision.id.as_bytes().to_vec(), buffer)
            .await?;
        self.stable_storage.flush().await?;

        // Let the transaction manager know that this participant is ready to commit.
        Ok(true)
    }

    /// Handles a request to abort when acting as a participant.
    #[tracing::instrument(name = "TransactionManager::abort", skip_all, fields(
        request = ?request
    ))]
    pub async fn abort(&self, request: AbortRequest) -> Result<bool> {
        if failure_sim::abort_should_fail() {
            return Err(anyhow!("simulating(ERROR): abort failure"));
        }

        if let Some(entry) = self.stable_storage.get(request.id.as_bytes()).await {
            let transaction_decision = TransactionDecision::decode(entry.as_ref())?;
            return Ok(transaction_decision.state == TRANSACTION_STATE_ABORTED);
        }

        let decision = TransactionDecision {
            id: request.id,
            state: TRANSACTION_STATE_ABORTED,
        };
        let mut buffer = Vec::new();
        decision.encode(&mut buffer)?;
        self.stable_storage
            .append(decision.id.as_bytes().to_vec(), buffer)
            .await?;
        self.stable_storage.flush().await?;

        Ok(true)
    }

    /// Handles a request to commit when acting as a participant.
    #[tracing::instrument(name = "TransactionManager::commit", skip_all, fields(
        request = ?request
    ))]
    pub async fn commit(&self, request: CommitRequest) -> Result<bool> {
        if failure_sim::commit_should_fail() {
            return Err(anyhow!("simulating(ERROR): commit failure"));
        }

        // If the request id is already in the log storage, it means the transaction manager
        // has failed and it is sending a duplicated request.
        if let Some(entry) = self.stable_storage.get(request.id.as_bytes()).await {
            let transaction_decision = TransactionDecision::decode(entry.as_ref())?;

            if transaction_decision.state == TRANSACTION_STATE_COMMITTED {
                // Let the transaction mananager know that the transaction has already beeen comitted.
                return Ok(true);
            } else if transaction_decision.state == TRANSACTION_STATE_ABORTED {
                // Let the transaction mananager know that the transaction has already beeen aborted.
                return Ok(false);
            }

            // The transaction has been accepted but it hasn't been committed yet.
            assert!(transaction_decision.state == TRANSACTION_STATE_DECIDED_TO_COMMIT);
        }

        let decision = TransactionDecision {
            id: request.id,
            state: TRANSACTION_STATE_COMMITTED,
        };
        let mut buffer = Vec::new();
        decision.encode(&mut buffer)?;
        self.stable_storage
            .append(decision.id.as_bytes().to_vec(), buffer)
            .await?;
        self.stable_storage.flush().await?;

        Ok(true)
    }

    /// Returns a boolean indicating whether a transaction has been committed.
    /// Unknown and transactions have been deleted from the log are considered aborted.
    #[tracing::instrument(name = "TransactionManager::query_transaction_state", skip_all, fields(
        transaction_id = ?transaction_id
    ))]
    pub async fn query_transaction_state(&self, transaction_id: &str) -> Result<bool> {
        if failure_sim::query_transaction_state_should_fail() {
            return Err(anyhow!("simulating(ERROR): query transaction failure"));
        }

        match self.stable_storage.get(transaction_id.as_bytes()).await {
            None => Ok(false),
            Some(value) => {
                let transaction_decision = TransactionDecision::decode(value.as_ref())?;

                Ok(
                    transaction_decision.state == TRANSACTION_STATE_DECIDED_TO_COMMIT
                        || transaction_decision.state == TRANSACTION_STATE_COMMITTED,
                )
            }
        }
    }
}
