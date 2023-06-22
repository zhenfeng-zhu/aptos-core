// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{backup::backup_handler::DbState, fast_sync_aptos_db::FastSyncStorageWrapper};
use anyhow::Result;
use aptos_crypto::hash::HashValue;
use aptos_types::{
    contract_event::ContractEvent,
    ledger_info::LedgerInfoWithSignatures,
    proof::{SparseMerkleRangeProof, TransactionAccumulatorRangeProof, TransactionInfoWithProof},
    state_store::{state_key::StateKey, state_value::StateValue},
    transaction::{Transaction, TransactionInfo, Version},
    write_set::WriteSet,
};
use std::sync::Arc;

/// `BackupDataAccessor` provides functionalities for data backup.
///  This is supposed to be replace backup handler.
#[derive(Clone)]
pub struct BackupDataAccessor {
    db: Arc<FastSyncStorageWrapper>,
}

impl BackupDataAccessor {
    pub fn new(db: Arc<FastSyncStorageWrapper>) -> Self {
        Self { db }
    }

    /// Gets an iterator that yields a range of transactions.
    pub fn get_transaction_iter(
        &self,
        start_version: Version,
        num_transactions: usize,
    ) -> Result<
        impl Iterator<Item = Result<(Transaction, TransactionInfo, Vec<ContractEvent>, WriteSet)>> + '_,
    > {
        self.db
            .get_transaction_iter(start_version, num_transactions)
    }

    /// Gets the proof for a transaction chunk.
    /// N.B. the `LedgerInfo` returned will always be in the same epoch of the `last_version`.
    pub fn get_transaction_range_proof(
        &self,
        first_version: Version,
        last_version: Version,
    ) -> Result<(TransactionAccumulatorRangeProof, LedgerInfoWithSignatures)> {
        self.db
            .get_transaction_range_proof(first_version, last_version)
    }

    /// Gets an iterator which can yield all accounts in the state tree.
    pub fn get_account_iter(
        &self,
        version: Version,
    ) -> Result<Box<dyn Iterator<Item = Result<(StateKey, StateValue)>> + Send + Sync>> {
        self.db.get_account_iter(version)
    }

    /// Gets the proof that proves a range of accounts.
    pub fn get_account_state_range_proof(
        &self,
        rightmost_key: HashValue,
        version: Version,
    ) -> Result<SparseMerkleRangeProof> {
        self.db
            .get_account_state_range_proof(rightmost_key, version)
    }

    /// Gets the epoch, committed version, and synced version of the DB.
    pub fn get_db_state(&self) -> Result<Option<DbState>> {
        self.db.get_db_state()
    }

    /// Gets the proof of the state root at specified version.
    /// N.B. the `LedgerInfo` returned will always be in the same epoch of the version.
    pub fn get_state_root_proof(
        &self,
        version: Version,
    ) -> Result<(TransactionInfoWithProof, LedgerInfoWithSignatures)> {
        self.db.get_state_root_proof(version)
    }

    pub fn get_epoch_ending_ledger_info_iter(
        &self,
        start_epoch: u64,
        end_epoch: u64,
    ) -> Result<impl Iterator<Item = Result<LedgerInfoWithSignatures>> + '_> {
        self.db
            .get_epoch_ending_ledger_info_iter(start_epoch, end_epoch)
    }
}
