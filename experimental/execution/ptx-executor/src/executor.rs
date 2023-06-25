// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use crate::scheduler::PtxScheduler;
use aptos_types::transaction::{Transaction, Version};
use rayon::Scope;
use std::sync::mpsc::{channel, Receiver, Sender};
use anyhow::anyhow;
use aptos_state_view::in_memory_state_view::InMemoryStateView;
use aptos_state_view::{StateView, TStateView};
use aptos_types::state_store::state_key::StateKey;
use aptos_types::state_store::state_storage_usage::StateStorageUsage;
use aptos_types::state_store::state_value::StateValue;
use aptos_vm::AptosVM;
use crate::common::{TxnIndex, VersionedKey};

pub(crate) struct PtxExecutor<'a> {
    finalizer: &'a PtxFinalizer,
    work_tx: Sender<Command<'a>>,
    work_rx: Receiver<Command<'a>>,
}

impl PtxExecutor{
    pub fn new(finalizer: &PtxFinalizer) -> Self {
        let (work_tx, work_rx) = channel();
        Self {
            finalizer,
            work_tx,
            work_rx,
        }
    }

    pub fn spawn_work_thread(&self, scope: &Scope<'_>) {
        scope.spawn(|scope| self.work(scope))
    }

    pub fn inform_state_value(&self, key: VersionedKey, value: Option<StateValue>) {
        self.work_tx
            .send(Command::InformStateValue{key, value})
            .expect("Work thread died.");
    }

    pub fn schedule_execute_transaction(&self, transaction: Transaction, dependencies: HashSet<(StateKey, TxnIndex)>) {
        self.work_tx
            .send(Command::AddTransaction {transaction, dependencies})
            .expect("Work thread died.");
    }

    fn work(&self, scope: &Scope<'_>) {
        let mut worker = PtxExecutorImpl::new(self.finalizer);
        loop {
            match self.work_rx.recv().expect("Work thread died.") {
                Command::InformStateValue{key, value} => {
                    worker.inform_state_value(key, value);
                }
                Command::AddTransaction {transaction, dependencies} => {
                    worker.add_transaction(transaction, dependencies);
                }
                Command::FinishBlock => {
                    worker.finish_block();
                    break;
                }
            }
        }
    }
}

enum Command {
    InformStateValue {
        key: VersionedKey,
        value: Option<StateValue>
    },
    AddTransaction {
        transaction: Transaction,
        dependencies: HashSet<(StateKey, TxnIndex)>
    },
    FinishBlock,
}

type TxnDependency = HashMap<StateKey, TxnIndex>;

type KeyDependants = HashSet<TxnIndex>;

type WorkerIndex = usize;

struct PtxExecutorImpl<'a> {
    finalizer: &'a PtxFinalizer,
    transactions: Vec<Option<Transaction>>,
    state_values: HashMap<VersionedKey, Option<StateValue>>,
    key_subscribers: HashMap<VersionedKey, Vec<TxnIndex>>,
    met_dependencies: Vec<Option<HashMap<StateKey, Option<StateValue>>>>,
    pending_dependencies: Vec<HashSet<VersionedKey>>,
    txs: Vec<Sender<WorkerCommand>>,
    ready_workers: Receiver<WorkerIndex>,
    worker_txs: Vec<Sender<WorkerCommand>>,
}

impl<'a> PtxExecutorImpl<'a> {
    fn new(scope: &'a Scope<'a>, num_workers: usize, finalizer: &PtxFinalizer) -> Self {
        let (worker_ready_tx, worker_ready_rx) = channel();
        for worker_index in 0..num_workers {

        }

        Self {
            finalizer,
            ..Default::default()
        }
    }

    fn inform_state_value(&mut self, key: VersionedKey, value: Option<StateValue>) {
        assert!(self.state_values.insert(key.clone(), value).is_none());

        if let Some(subscribers) = self.key_subscribers.remove(&key) {
            for txn_index in subscribers {
                self.inform_state_value_to_txn( txn_index, key.clone(), value.clone());
            }
        }
    }

    fn inform_state_value_to_txn(&mut self, txn_index: TxnIndex, key: VersionedKey, value: Option<StateValue>) {
        let pending_dependencies = &mut self.pending_dependencies[txn_index];
        assert!(pending_dependencies.remove(&key), "Pending dependency not found.");
        self.met_dependencies[txn_index].expect("met_dependencies not initialized.").insert(key.0, value);

        if pending_dependencies.is_empty() {
            self.execute_transaction(txn_index);
        }
    }

    fn add_transaction(&mut self, transaction: Transaction, dependencies: HashSet<VersionedKey>) {
        let txn_index = self.transactions.len();
        self.transactions.push(Some(transaction));
        let mut met_dependencies = HashMap::new();
        let mut pending_dependencies = HashSet::new();
        for key in dependencies {
            if let Some(value) = self.state_values.get(&key) {
                met_dependencies.insert(key.0, value.clone());
            } else {
                pending_dependencies.insert(key);
            }
        }
        let ready_to_execute = pending_dependencies.is_empty();

        self.met_dependencies.push(Some(met_dependencies));
        self.pending_dependencies.push(pending_dependencies);

        if ready_to_execute {
            self.execute_transaction(txn_index);
        }
    }

    fn execute_transaction(&self, txn_index: TxnIndex) {
        let txn = self.transactions[txn_index].take().expect("transaction is None.");
        let met_dependencies = self.met_dependencies[txn_index].take().expect("met_dependencies is None.");

        let worker_index = self.ready_workers.recv().expect("All Workers died.");
        self.worker_txs.worker_index.send(WorkerCommand::ExecuteTransaction{transaction, met_dependencies})
    }
}

enum WorkerCommand {
    ExecuteTransaction {
        txn_index: TxnIndex,
        transaction: Transaction,
        met_dependencies: HashMap<StateKey, Option<StateValue>>,
    },
    Finish,
}

fn worker(work_rx: Receiver<WorkerCommand>, worker_ready_tx: Sender<WorkerIndex>, worker_index: WorkerIndex) {
    // TODO(aldenhu): initialize VM without passing in a state view

    loop {
        match work_rx.recv().expect("Sender died.") {
            WorkerCommand::ExecuteTransaction {txn_index, transaction, met_dependencies} => {
                let state_view = DependenciesStateView {met_dependencies};

                // TODO(aldenhu): call the VM

                finalizer.add_vm_output(txn_index, vm_output, transaction);
                worker_ready_tx.send(worker_index).ok();
            }
            WorkerCommand::Finish => {
                break;
            }
        }
    }
}

struct DependenciesStateView {
    met_dependencies: HashMap<StateKey, Option<StateValue>>,
}

impl TStateView for DependenciesStateView {
    type Key = StateKey;

    fn get_state_value(&self, state_key: &Self::Key) -> Result<Option<StateValue>> {
        self.met_dependencies.get(state_key).cloned().ok_or_else(|| anyhow!("Dependency not met."))
    }

    fn is_genesis(&self) -> bool {
        unreachable!()
    }

    fn get_usage(&self) -> anyhow::Result<StateStorageUsage> {
        // TODO(aldenhu): maybe remove get_usage() from StateView
        todo!()
    }
}
