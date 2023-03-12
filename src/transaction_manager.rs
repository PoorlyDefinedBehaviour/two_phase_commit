use anyhow::{anyhow, Context, Result};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::Mutex;

use tonic::Request;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    core_proto::{
        AbortRequest, CommitRequest, HostAddr, PrepareCommitRequest, TransactionDecision,
    },
    failure_sim,
    node_service::NodeService,
    storage::StableStorage,
};

type TransactionId = String;

// TODO: should use an enum so the compiler errors when a case is not checked.
type TransactionState = i32;

const TRANSACTION_STATE_DECIDED_TO_COMMIT: TransactionState = 1;
const TRANSACTION_STATE_COMMITTED: TransactionState = 2;
const TRANSACTION_STATE_ABORTED: TransactionState = 3;

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

    /// The amount of time the system should wait before checking if there are
    /// transactions be completed by at least one node again and trying to complete them.
    pub try_to_commit_transactions_interval: Duration,
}

/// Responsible for requesting the participaints in a transaction to
/// prepare to commit and then to commit if possible.
pub struct TransactionManager {
    node_service: NodeService,
    stable_storage: Arc<dyn StableStorage>,
    config: Config,
    /// Mapping from transaction id to the transaction state.
    /// The map includes only transactions that need to be aborted
    /// or committed.
    pending_transactions: Mutex<HashMap<TransactionId, PendingTransaction>>,
}

#[derive(Debug)]
struct PendingTransaction {
    /// Should the transaction state or commit?
    desired_state: TransactionState,
    /// Nodes that haven't committed or aborted the transaction yet.
    pending_nodes: HashSet<String>,
}

impl TransactionManager {
    #[tracing::instrument(name = "Manager::new", skip_all, fields(
        config = ?config
    ))]
    pub fn new(
        node_service: NodeService,
        stable_storage: Arc<dyn StableStorage>,
        config: Config,
    ) -> Arc<Self> {
        let try_to_commit_transactions_interval = config.try_to_commit_transactions_interval;

        let transaction_manager = Arc::new(Self {
            node_service,
            stable_storage,
            config,
            pending_transactions: Mutex::new(HashMap::new()),
        });

        tokio::spawn(try_to_commit_transactions_periodically(
            try_to_commit_transactions_interval,
            Arc::downgrade(&transaction_manager),
        ));

        transaction_manager
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
            // TODO: could be done in parallel.
            for host_addr in ok_responses {
                // If we fail to abort, just ignore the error. The participant will poll
                // the transaction manager at a later time and find out that the transaction
                // has been aborted because the manager won't remember the transaction.
                let mut request = Request::new(AbortRequest { id: op_id.clone() });
                request.set_timeout(self.config.abort_request_timeout);

                // If the request to abort fails, the node with the pending request will query the transaction manager
                // to get the state of the transaction and find out that the transaction has been aborted
                // because it is not in the log.
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

        let mut pending_transactions = self.pending_transactions.lock().await;

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
            // Mark the transaction as pending so it is retried later.
            pending_transactions.insert(
                op_id.clone(),
                PendingTransaction {
                    desired_state: TRANSACTION_STATE_DECIDED_TO_COMMIT,
                    pending_nodes: self.config.cluster_members.iter().cloned().collect(),
                },
            );

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

    /// Tries to commit transactions that failed to commit.
    #[tracing::instrument(name = "try_to_commit_transactions", skip_all)]
    async fn try_to_commit_transactions(&self) {
        loop {
            let mut pending_transactions = self.pending_transactions.lock().await;
            let mut completed_transactions = Vec::new();

            for (transaction_id, pending_transaction) in pending_transactions.iter_mut() {
                info!(?transaction_id, "trying to complete transaction");
                // Every pending transaction should be in the state TRANSACTION_STATE_DECIDED_TO_COMMIT.
                assert!(pending_transaction.desired_state == TRANSACTION_STATE_DECIDED_TO_COMMIT);

                let futures =
                    pending_transaction
                        .pending_nodes
                        .iter()
                        .cloned()
                        .map(|host_addr| async {
                            // If we fail to abort, just ignore the error. The participant will poll
                            // the transaction manager at a later time and find out that the transaction
                            // has been aborted because the manager won't remember the transaction.
                            let mut request = Request::new(CommitRequest {
                                id: transaction_id.to_owned(),
                            });
                            request.set_timeout(self.config.abort_request_timeout);

                            (
                                host_addr.clone(),
                                self.node_service.commit(host_addr, request).await,
                            )
                        });

                for (host_addr, result) in futures::future::join_all(futures).await {
                    match result {
                        Err(err) => {
                            error!(
                                ?err,
                                "got error response when requesting node to complete transaction"
                            );
                        }
                        Ok(response) => {
                            if response.into_inner().ok {
                                pending_transaction.pending_nodes.remove(&host_addr);
                                // If every pending node completed the transaction,
                                // mark it as completed so it can be removed from the pending transactions set.
                                if pending_transaction.pending_nodes.is_empty() {
                                    completed_transactions.push(transaction_id.clone());
                                }
                            } else {
                                error!(?host_addr, "node was unable to complete transaction");
                            }
                        }
                    }
                }
            }

            info!(
                ?completed_transactions,
                "completed {} out of {} transactions",
                completed_transactions.len(),
                pending_transactions.len()
            );
            for transaction_id in completed_transactions {
                pending_transactions.remove(&transaction_id);
            }
        }
    }
}

/// Tries to commit transactions that failed to commit.
#[tracing::instrument(name = "try_to_commit_transactions", skip_all, fields(
    interval = ?interval
))]
async fn try_to_commit_transactions_periodically(
    interval: Duration,
    transaction_manager: Weak<TransactionManager>,
) {
    loop {
        match transaction_manager.upgrade() {
            None => {
                info!("transaction manager has been dropped, exiting loop");
                return;
            }
            Some(transaction_manager) => {
                transaction_manager.try_to_commit_transactions().await;
            }
        };

        debug!("waiting before trying to complete transactions");
        tokio::time::sleep(interval).await;
    }
}
