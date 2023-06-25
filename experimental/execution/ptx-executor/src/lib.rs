// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! This crate defines `PtxBlockExecutor` and supporting type that executes purely P-Transactions which
//! have accurately predicable read/write sets.

mod analyzer;
mod scheduler;
mod state_reader;
mod executor;
mod common;

use crate::{analyzer::PtxAnalyzer, scheduler::PtxScheduler, state_reader::PtxStateReader};
use aptos_state_view::StateView;
use aptos_types::{
    block_executor::partitioner::SubBlocksForShard,
    transaction::{Transaction, TransactionOutput},
};
use aptos_vm::{
    aptos_vm::RAYON_EXEC_POOL, sharded_block_executor::ShardedBlockExecutor, AptosVM, VMExecutor,
};
use move_core_types::vm_status::VMStatus;
use std::sync::Arc;
use crate::executor::PtxExecutor;

struct PtxBlockExecutor;

impl VMExecutor for PtxBlockExecutor {
    fn execute_block(
        transactions: Vec<Transaction>,
        state_view: &(impl StateView + Sync),
        _maybe_block_gas_limit: Option<u64>,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        // 1. Analyze: annotate read / write sets.
        // 2. Sort: build dependency graph.
        // 3. Schedule: start executing a transaction once its dependencies are met.
        // 4. Execute: and inform dependent transactions after execution.
        // 5. Finalize: materialize aggregators, etc.
        let concurrency_level = AptosVM::get_concurrency_level();
        assert!(
            concurrency_level > 2,
            "Analyzer and scheduler need their own threads."
        );
        let num_executor_threads = concurrency_level - 2;

        let finalizer = PtxFinalizer::new();
        let executor = PtxExecutor::new(&finalizer);
        let state_reader = PtxStateReader::new(state_view, &executor);
        let scheduler = PtxScheduler::new(&executor, &storage_reader);
        let analyzer = PtxAnalyzer::new(&scheduler);

        RAYON_EXEC_POOL.scope(|scope| {
            finalizer.spawn_work_thread(scope);
            executor.spawn_work_threads(scope, num_executor_threads);
            state_reader.spawn_work_thread(scope);
            scheduler.spawn_work_thread(scope);
            analyzer.spawn_work_thread(scope);
        });

        analyzer.analyze(&transactions);
        finalizer.finalize()
    }

    fn execute_block_sharded<S: StateView + Sync + Send + 'static>(
        _sharded_block_executor: &ShardedBlockExecutor<S>,
        _block: Vec<SubBlocksForShard<Transaction>>,
        _state_view: Arc<S>,
        _maybe_block_gas_limit: Option<u64>,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        unimplemented!()
    }
}
