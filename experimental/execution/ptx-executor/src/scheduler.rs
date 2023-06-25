// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use crate::state_reader::PtxStateReader;
use aptos_types::{
    state_store::state_key::StateKey,
    transaction::{analyzed_transaction::AnalyzedTransaction, Transaction},
};
use dashmap::DashMap;
use rayon::Scope;
use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::{channel, Receiver, Sender},
};
use std::collections::hash_map::Entry;
use crate::common::{BASE_VERSION, TxnIndex, VersionedKey};
use crate::executor::PtxExecutor;

pub(crate) struct PtxScheduler<'a> {
    executor: &'a PtxExecutor<'a>,
    state_reader: &'a PtxStateReader<'a>,
    work_tx: Sender<Command>,
    work_rx: Receiver<Command>,
}

impl PtxScheduler {
    pub fn new(executor: &PtxExecutor, state_reader: &PtxStateReader) -> Self {
        let (work_tx, work_rx) = channel();
        Self {
            executor,
            state_reader,
            work_tx,
            work_rx,
        }
    }

    pub fn spawn_work_thread(&self, scope: &Scope<'_>) {
        scope.spawn(|| self.work())
    }

    pub fn add_analyzed_transaction(&self, txn: AnalyzedTransaction) {
        self.work_tx
            .send(Command::AddAnalyzedTransaction(txn))
            .expect("Work thread died.");
    }

    pub fn finish_block(&self) {
        self.work_tx
            .send(Command::FinishBlock)
            .expect("Work thread died.");
    }

    fn work(&self) {
        let mut worker = Worker::new(self.scheduler, self.state_reader);
        loop {
            match self.work_rx.recv().expect("Channel closed.") {
                Command::AddAnalyzedTransaction(txn) => {
                    worker.add_analyzed_transaction(txn);
                },
                Command::FinishBlock => {
                    self.executor.finish_block();
                    break
                },
            }
        }
    }
}

enum Command {
    AddAnalyzedTransaction(AnalyzedTransaction),
    FinishBlock,
}

struct Worker<'a> {
    executor: &'a PtxExecutor<'a>,
    state_reader: &'a PtxStateReader<'a>,
    latest_writes: HashMap<StateKey, TxnIndex>,
    num_txns: usize,
}

impl Worker {
    fn new(executor: &PtxExecutor, state_reader: &PtxStateReader) -> Self {
        Self {
            state_reader,
            executor,
            ..Default::default()
        }
    }

    fn add_analyzed_transaction(&mut self, txn: AnalyzedTransaction) {
        let txn_index = self.num_txns;
        self.num_txns += 1;

        // TODO(sharding): Reorder Non-P-Transacttions
        let (txn, reads, read_writes) = txn.expect_p_txn();
        let mut dependencies = HashSet::new();
        self.process_txn_dependencies(txn_index, reads, false /* is_write_set */, &mut dependencies);
        self.process_txn_dependencies(txn_index, read_writes, true /* is_write_set */, &mut dependencies);

        self.executor.schedule_execute_transaction(txn, dependencies);
    }

    fn process_txn_dependencies(&mut self, txn_index: TxnIndex, keys: Vec<StateKey>, is_write_set: bool, dependencies: &mut HashSet<VersionedKey>) {
        for key in keys {
            match self.latest_writes.entry(key) {
                Entry::Occupied(mut entry) => {
                    dependencies.insert((key.clone(), *entry));
                    if is_write_set {
                        *entry =  txn_index;
                    }
                },
                Entry::Vacant(entry) => {
                    dependencies.insert((read_key.clone(), BASE_VERSION));

                    // TODO(sharding): maybe prioritize reads that unblocks execution immediately
                    self.state_reader.schedule_read(&read_key);

                    if is_write_set {
                        entry.insert(txn_index);
                    } else {
                        entry.insert(BASE_VERSION);
                    }
                }, // end Entry::Vacant
            } // end match
        } // end for
    }
}
