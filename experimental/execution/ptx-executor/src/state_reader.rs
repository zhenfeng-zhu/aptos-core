// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use aptos_state_view::StateView;
use aptos_types::{state_store::state_key::StateKey, transaction::Transaction};
use once_cell::sync::Lazy;
use rayon::Scope;
use std::sync::mpsc::{channel, Receiver, Sender};
use tracing::log::kv::Source;
use aptos_types::state_store::state_value::StateValue;
use crate::common::{BASE_VERSION, TxnIndex};
use crate::executor::{PtxExecutor};

pub(crate) static IO_POOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .thread_name(|index| format!("ptx_state_reader_io_{}", index))
        .build()
        .unwrap()
});

pub(crate) struct PtxStateReader<'a> {
    state_view: &'a dyn StateView,
    executor: &'a PtxExecutor<'a>,
    work_tx: Sender<Command<'a>>,
    work_rx: Receiver<Command<'a>>,
}

impl<'a> PtxStateReader<'a> {
    pub fn new(state_view: &dyn StateView, executor: &PtxExecutor) -> Self {
        let (work_tx, work_rx) = channel();
        Self {
            state_view,
            executor,
            work_tx,
            work_rx,
        }
    }

    pub fn spawn_work_thread(&self, scope: &Scope<'_>) {
        scope.spawn(|| self.work())
    }

    pub fn schedule_read(&self, state_key: &'a StateKey) {
        self.work_tx
            .send(Command::Read(state_key))
            .expect("Work thread died.");
    }

    pub fn finish_block(&self) {
        self.work_tx
            .send(Command::FinishBlock)
            .expect("Work thread died.");
    }

    fn work(&self) {
        IO_POOL.scope(|scope| loop {
            match self.work_rx.recv().expect("Channel closed.") {
                Command::Read(&'a state_key) => scope.spawn(|_scope| {
                    self.executor
                        .inform_state_value(BASE_VERSION, state_key, self.state_view.get(state_key));
                }),
                Command::FinishBlock => {
                    break;
                },
            }
        });
    }
}

enum Command<'a> {
    InformStateValue {
        txn_index: TxnIndex,
        key: &'a StateKey,
        value: StateValue,
    },
    FinishBlock,
}
