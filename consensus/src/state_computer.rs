// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::StateSyncError,
    state_replication::{StateComputer, StateComputerCommitCallBackType, TxnManager},
};
use anyhow::Result;
use consensus_notifications::ConsensusNotificationSender;
use consensus_types::{block::Block, executed_block::ExecutedBlock};
use diem_crypto::HashValue;
use diem_logger::prelude::*;
use diem_metrics::monitor;
use diem_types::ledger_info::LedgerInfoWithSignatures;
use execution_correctness::ExecutionCorrectness;
use executor_types::{Error as ExecutionError, StateComputeResult};
use fail::fail_point;
use std::{boxed::Box, sync::Arc};

/// Basic communication with the Execution module;
/// implements StateComputer traits.
pub struct ExecutionProxy {
    execution_correctness_client: Box<dyn ExecutionCorrectness + Send + Sync>,
    mempool_notifier: Arc<dyn TxnManager>,
    state_sync_notifier: Arc<dyn ConsensusNotificationSender>,
}

impl ExecutionProxy {
    pub fn new(
        execution_correctness_client: Box<dyn ExecutionCorrectness + Send + Sync>,
        mempool_notifier: Arc<dyn TxnManager>,
        state_sync_notifier: Arc<dyn ConsensusNotificationSender>,
    ) -> Self {
        Self {
            execution_correctness_client,
            mempool_notifier,
            state_sync_notifier,
        }
    }
}

#[async_trait::async_trait]
impl StateComputer for ExecutionProxy {
    async fn compute(
        &self,
        // The block to be executed.
        block: &Block,
        // The parent block id.
        parent_block_id: HashValue,
    ) -> Result<StateComputeResult, ExecutionError> {
        fail_point!("consensus::compute", |_| {
            Err(ExecutionError::InternalError {
                error: "Injected error in compute".into(),
            })
        });
        debug!(
            block_id = block.id(),
            parent_id = block.parent_id(),
            "Executing block",
        );

        // TODO: figure out error handling for the prologue txn
        let compute_result = monitor!(
            "execute_block",
            self.execution_correctness_client
                .execute_block(block.clone(), parent_block_id)
        )?;

        // notify mempool about failed transaction
        if let Err(e) = self
            .mempool_notifier
            .notify_failed_txn(block, &compute_result)
            .await
        {
            error!(
                error = ?e, "Failed to notify mempool of rejected txns",
            );
        }
        Ok(compute_result)
    }

    /// Send a successful commit. A future is fulfilled when the state is finalized.
    async fn commit(
        &self,
        blocks: &[Arc<ExecutedBlock>],
        finality_proof: LedgerInfoWithSignatures,
        callback: StateComputerCommitCallBackType,
    ) -> Result<(), ExecutionError> {
        let mut block_ids = Vec::new();
        let mut txns = Vec::new();
        let mut reconfig_events = Vec::new();

        for block in blocks {
            block_ids.push(block.id());
            txns.extend(block.transactions_to_commit());
            reconfig_events.extend(block.reconfig_event());
        }

        monitor!(
            "commit_block",
            self.execution_correctness_client
                .commit_blocks(block_ids, finality_proof.clone())?
        );
        let client = self.state_sync_notifier.clone();
        let blocks_clone = blocks.to_vec();

        tokio::spawn(async move {
            if let Err(e) = monitor!(
                "notify_state_sync",
                client.notify_new_commit(txns, reconfig_events).await
            ) {
                error!(error = ?e, "Failed to notify state synchronizer");
            }

            callback(&blocks_clone, finality_proof);
        });

        Ok(())
    }

    /// Synchronize to a commit that not present locally.
    async fn sync_to(&self, target: LedgerInfoWithSignatures) -> Result<(), StateSyncError> {
        fail_point!("consensus::sync_to", |_| {
            Err(anyhow::anyhow!("Injected error in sync_to").into())
        });
        // Here to start to do state synchronization where ChunkExecutor inside will
        // process chunks and commit to Storage. However, after block execution and
        // commitments, the the sync state of ChunkExecutor may be not up to date so
        // it is required to reset the cache of ChunkExecutor in State Sync
        // when requested to sync.
        let res = monitor!(
            "sync_to",
            self.state_sync_notifier.sync_to_target(target).await
        );
        // Similarily, after the state synchronization, we have to reset the cache
        // of BlockExecutor to guarantee the latest committed state is up to date.
        self.execution_correctness_client.reset()?;

        res.map_err(|error| {
            let anyhow_error: anyhow::Error = error.into();
            anyhow_error.into()
        })
    }
}
