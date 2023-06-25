// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use crate::scheduler::PtxScheduler;
use aptos_types::transaction::Transaction;
use rayon::Scope;
use std::sync::mpsc::{channel, Receiver, Sender};

pub(crate) struct PtxAnalyzer<'a> {
    scheduler: &'a PtxScheduler<'a>,
    work_tx: Sender<Command<'a>>,
    work_rx: Receiver<Command<'a>>,
}

impl PtxAnalyzer {
    pub fn new(scheduler: &PtxScheduler) -> Self {
        let (work_tx, work_rx) = channel();
        Self {
            scheduler,
            work_tx,
            work_rx,
        }
    }

    pub fn analyze(&self, transactions: &[Transaction]) {
        for txn in transactions {
            self.work_tx
                .send(Command::Work(txn))
                .expect("Work thread died.");
        }
        self.work_tx
            .send(Command::Finish)
            .expect("Work thread died.");
    }

    pub fn spawn_work_thread(&self, scope: &Scope<'_>) {
        scope.spawn(|| self.work())
    }

    fn work(&self) {
        loop {
            match self.work_rx.recv().expect("Channel closed.") {
                Command::Work(txn) => self.scheduler.add_analyzed_transaction(txn.clone().into()),
                Command::Finish => {
                    self.scheduler.finish_block();
                    break;
                },
            }
        }
    }
}

enum Command<'a> {
    Work(&'a Transaction),
    Finish,
}
