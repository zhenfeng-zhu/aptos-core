// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    backup::backup_handler::DbState,
    event_store::EventStore,
    ledger_store::LedgerStore,
    metrics::{
        BACKUP_EPOCH_ENDING_EPOCH, BACKUP_STATE_SNAPSHOT_LEAF_IDX, BACKUP_STATE_SNAPSHOT_VERSION,
        BACKUP_TXN_VERSION,
    },
    state_store::StateStore,
    transaction_store::TransactionStore,
    AptosDB,
};
use anyhow::{anyhow, ensure, format_err, Context, Result};
use aptos_config::config::{BootstrappingMode, NodeConfig};
use aptos_crypto::HashValue;
use aptos_storage_interface::{
    cached_state_view::ShardedStateCache, state_delta::StateDelta, DbReader, DbWriter,
    ExecutedTrees, FastSyncStatus, Order, StateSnapshotReceiver,
};
use aptos_types::{
    account_config::NewBlockEvent,
    contract_event::{ContractEvent, EventWithVersion},
    epoch_change::EpochChangeProof,
    epoch_state::EpochState,
    event::EventKey,
    ledger_info::LedgerInfoWithSignatures,
    proof::{
        AccumulatorConsistencyProof, SparseMerkleProof, SparseMerkleProofExt,
        SparseMerkleRangeProof, TransactionAccumulatorRangeProof, TransactionAccumulatorSummary,
        TransactionInfoWithProof,
    },
    state_proof::StateProof,
    state_store::{
        state_key::StateKey,
        state_key_prefix::StateKeyPrefix,
        state_storage_usage::StateStorageUsage,
        state_value::{StateValue, StateValueChunkWithProof},
        table::{TableHandle, TableInfo},
        ShardedStateUpdates,
    },
    transaction::{
        AccountTransactionsWithProof, Transaction, TransactionInfo, TransactionListWithProof,
        TransactionOutputListWithProof, TransactionToCommit, TransactionWithProof, Version,
    },
    write_set::WriteSet,
};
use move_core_types::account_address::AccountAddress;
use std::sync::{Arc, RwLock};

const SECONDARY_DB_DIR: &str = "fast_sync_secondary";

/// This is a wrapper around [AptosDB] that is used to bootstrap the node for fast sync mode
pub struct FastSyncStorageWrapper {
    // main is used for normal read/write or genesis data during fast sync
    db_main: AptosDB,
    // secondary is used for restoring fast sync snapshot and all the read/writes afterwards
    db_secondary: Option<AptosDB>,
    // This is for reading the fast_sync status to determine which db to use
    fast_sync_status: Arc<RwLock<FastSyncStatus>>,
}

impl FastSyncStorageWrapper {
    pub fn new_fast_sync_aptos_db(
        config: &NodeConfig,
        status: Arc<RwLock<FastSyncStatus>>,
    ) -> Result<Self> {
        let mut db_dir = config.storage.dir();
        let db_main = AptosDB::open(
            db_dir.as_path(),
            false,
            config.storage.storage_pruner_config,
            config.storage.rocksdb_configs,
            config.storage.enable_indexer,
            config.storage.buffered_state_target_items,
            config.storage.max_num_nodes_per_lru_cache_shard,
        )
        .map_err(|err| anyhow!("fast sync DB failed to open {}", err))?;

        let db_secondary = if config.state_sync.state_sync_driver.bootstrapping_mode
            == BootstrappingMode::DownloadLatestStates
        {
            db_dir.push(SECONDARY_DB_DIR);
            Some(
                AptosDB::open(
                    db_dir.as_path(),
                    false,
                    config.storage.storage_pruner_config,
                    config.storage.rocksdb_configs,
                    config.storage.enable_indexer,
                    config.storage.buffered_state_target_items,
                    config.storage.max_num_nodes_per_lru_cache_shard,
                )
                .map_err(|err| anyhow!("fast sync DB failed to open {}", err))?,
            )
        } else {
            None
        };

        Ok(Self {
            db_main,
            db_secondary,
            fast_sync_status: status,
        })
    }

    pub fn get_fast_sync_status(&self) -> Result<FastSyncStatus> {
        self.fast_sync_status
            .read()
            .map_err(|err| anyhow!("failed to read fast sync status: {}", err))
            .map(|status| *status)
    }

    /// Check if the fast sync finished already
    fn is_fast_sync_bootstrap_finished(&self) -> bool {
        let status = self.get_fast_sync_status().unwrap();
        status == FastSyncStatus::FINISHED
    }

    pub(crate) fn get_transaction_store(&self) -> Result<&TransactionStore> {
        if self.is_fast_sync_bootstrap_finished() {
            Ok(&self
                .db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .transaction_store)
        } else {
            Ok(&self.db_main.transaction_store)
        }
    }

    pub(crate) fn get_ledger_store(&self) -> Result<&LedgerStore> {
        if self.is_fast_sync_bootstrap_finished() {
            Ok(&self
                .db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .ledger_store)
        } else {
            Ok(&self.db_main.ledger_store)
        }
    }

    pub(crate) fn get_event_store(&self) -> Result<&EventStore> {
        if self.is_fast_sync_bootstrap_finished() {
            Ok(&self
                .db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .event_store)
        } else {
            Ok(&self.db_main.event_store)
        }
    }

    pub(crate) fn get_state_store_arc(&self) -> Result<Arc<StateStore>> {
        if self.is_fast_sync_bootstrap_finished() {
            Ok(Arc::clone(
                &self
                    .db_secondary
                    .as_ref()
                    .expect("db_secondary is not initialized")
                    .state_store,
            ))
        } else {
            Ok(Arc::clone(&self.db_main.state_store))
        }
    }

    pub(crate) fn get_state_store(&self) -> Result<&StateStore> {
        if self.is_fast_sync_bootstrap_finished() {
            Ok(&self
                .db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .state_store)
        } else {
            Ok(&self.db_main.state_store)
        }
    }

    /// Provide an iterator to underly data for reading transactions
    pub fn get_transaction_iter(
        &self,
        start_version: Version,
        num_transactions: usize,
    ) -> Result<
        impl Iterator<Item = Result<(Transaction, TransactionInfo, Vec<ContractEvent>, WriteSet)>> + '_,
    > {
        let txn_store = self.get_transaction_store()?;
        let ledger_store = self.get_ledger_store()?;
        let event_store = self.get_event_store()?;

        let txn_iter = txn_store.get_transaction_iter(start_version, num_transactions)?;
        let mut txn_info_iter =
            ledger_store.get_transaction_info_iter(start_version, num_transactions)?;
        let mut event_vec_iter =
            event_store.get_events_by_version_iter(start_version, num_transactions)?;
        let mut write_set_iter = txn_store.get_write_set_iter(start_version, num_transactions)?;

        let zipped = txn_iter.enumerate().map(move |(idx, txn_res)| {
            let version = start_version + idx as u64; // overflow is impossible since it's check upon txn_iter construction.

            let txn = txn_res?;
            let txn_info = txn_info_iter
                .next()
                .ok_or_else(|| anyhow!("TransactionInfo not found when Transaction exists."))
                .context(version)??;
            let event_vec = event_vec_iter
                .next()
                .ok_or_else(|| anyhow!("Events not found when Transaction exists."))
                .context(version)??;
            let write_set = write_set_iter
                .next()
                .ok_or_else(|| anyhow!("WriteSet not found when Transaction exists."))
                .context(version)??;
            BACKUP_TXN_VERSION.set(version as i64);
            Ok((txn, txn_info, event_vec, write_set))
        });
        Ok(zipped)
    }

    /// Gets the proof for a transaction chunk.
    /// N.B. the `LedgerInfo` returned will always be in the same epoch of the `last_version`.
    pub fn get_transaction_range_proof(
        &self,
        first_version: Version,
        last_version: Version,
    ) -> Result<(TransactionAccumulatorRangeProof, LedgerInfoWithSignatures)> {
        ensure!(
            last_version >= first_version,
            "Bad transaction range: [{}, {}]",
            first_version,
            last_version
        );
        let ledger_store = self.get_ledger_store()?;
        let num_transactions = last_version - first_version + 1;
        let epoch = ledger_store.get_epoch(last_version)?;
        let ledger_info = ledger_store.get_latest_ledger_info_in_epoch(epoch)?;
        let accumulator_proof = ledger_store.get_transaction_range_proof(
            Some(first_version),
            num_transactions,
            ledger_info.ledger_info().version(),
        )?;
        Ok((accumulator_proof, ledger_info))
    }

    /// Gets an iterator which can yield all accounts in the state tree.
    pub fn get_account_iter(
        &self,
        version: Version,
    ) -> Result<Box<dyn Iterator<Item = Result<(StateKey, StateValue)>> + Send + Sync>> {
        let iterator = self
            .get_state_store_arc()?
            .get_state_key_and_value_iter(version, HashValue::zero())?
            .enumerate()
            .map(move |(idx, res)| {
                BACKUP_STATE_SNAPSHOT_VERSION.set(version as i64);
                BACKUP_STATE_SNAPSHOT_LEAF_IDX.set(idx as i64);
                res
            });
        Ok(Box::new(iterator))
    }

    /// Gets the proof that proves a range of accounts.
    pub fn get_account_state_range_proof(
        &self,
        rightmost_key: HashValue,
        version: Version,
    ) -> Result<SparseMerkleRangeProof> {
        self.get_state_store()?
            .get_value_range_proof(rightmost_key, version)
    }

    /// Gets the epoch, committed version, and synced version of the DB.
    pub fn get_db_state(&self) -> Result<Option<DbState>> {
        Ok(self
            .get_ledger_store()?
            .get_latest_ledger_info_option()
            .map(|li| DbState {
                epoch: li.ledger_info().epoch(),
                committed_version: li.ledger_info().version(),
            }))
    }

    /// Gets the proof of the state root at specified version.
    /// N.B. the `LedgerInfo` returned will always be in the same epoch of the version.
    pub fn get_state_root_proof(
        &self,
        version: Version,
    ) -> Result<(TransactionInfoWithProof, LedgerInfoWithSignatures)> {
        let ledger_store = self.get_ledger_store()?;
        let epoch = ledger_store.get_epoch(version)?;
        let ledger_info = ledger_store.get_latest_ledger_info_in_epoch(epoch)?;
        let txn_info = ledger_store
            .get_transaction_info_with_proof(version, ledger_info.ledger_info().version())?;

        Ok((txn_info, ledger_info))
    }

    pub fn get_epoch_ending_ledger_info_iter(
        &self,
        start_epoch: u64,
        end_epoch: u64,
    ) -> Result<impl Iterator<Item = Result<LedgerInfoWithSignatures>> + '_> {
        Ok(self
            .get_ledger_store()?
            .get_epoch_ending_ledger_info_iter(start_epoch, end_epoch)?
            .enumerate()
            .map(move |(idx, li)| {
                BACKUP_EPOCH_ENDING_EPOCH.set((start_epoch + idx as u64) as i64);
                li
            }))
    }
}

impl DbWriter for FastSyncStorageWrapper {
    fn get_state_snapshot_receiver(
        &self,
        version: Version,
        expected_root_hash: HashValue,
    ) -> Result<Box<dyn StateSnapshotReceiver<StateKey, StateValue>>> {
        let status = self.get_fast_sync_status()?;
        assert!(status == FastSyncStatus::STARTED);
        self.db_secondary
            .as_ref()
            .expect("db_secondary is None")
            .get_state_snapshot_receiver(version, expected_root_hash)
    }

    fn finalize_state_snapshot(
        &self,
        version: Version,
        output_with_proof: TransactionOutputListWithProof,
        ledger_infos: &[LedgerInfoWithSignatures],
    ) -> Result<()> {
        let status = self.get_fast_sync_status()?;
        assert_eq!(status, FastSyncStatus::STARTED);
        self.db_secondary
            .as_ref()
            .expect("db_secondary is None")
            .finalize_state_snapshot(version, output_with_proof, ledger_infos)?;
        let mut status = self
            .fast_sync_status
            .write()
            .expect("Failed to get write lock of fast sync status");
        *status = FastSyncStatus::FINISHED;
        Ok(())
    }

    fn save_transactions(
        &self,
        txns_to_commit: &[TransactionToCommit],
        first_version: Version,
        base_state_version: Option<Version>,
        ledger_info_with_sigs: Option<&LedgerInfoWithSignatures>,
        sync_commit: bool,
        latest_in_memory_state: StateDelta,
    ) -> Result<()> {
        let status = self.get_fast_sync_status()?;
        if status == FastSyncStatus::STARTED || status == FastSyncStatus::FINISHED {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is None")
                .save_transactions(
                    txns_to_commit,
                    first_version,
                    base_state_version,
                    ledger_info_with_sigs,
                    sync_commit,
                    latest_in_memory_state,
                )
        } else {
            self.db_main.save_transactions(
                txns_to_commit,
                first_version,
                base_state_version,
                ledger_info_with_sigs,
                sync_commit,
                latest_in_memory_state,
            )
        }
    }

    fn save_transaction_block(
        &self,
        txns_to_commit: &[Arc<TransactionToCommit>],
        first_version: Version,
        base_state_version: Option<Version>,
        ledger_info_with_sigs: Option<&LedgerInfoWithSignatures>,
        sync_commit: bool,
        latest_in_memory_state: StateDelta,
        block_state_updates: ShardedStateUpdates,
        sharded_state_cache: &ShardedStateCache,
    ) -> Result<()> {
        let status = self.get_fast_sync_status()?;
        if status == FastSyncStatus::STARTED || status == FastSyncStatus::FINISHED {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is None")
                .save_transaction_block(
                    txns_to_commit,
                    first_version,
                    base_state_version,
                    ledger_info_with_sigs,
                    sync_commit,
                    latest_in_memory_state,
                    block_state_updates,
                    sharded_state_cache,
                )
        } else {
            self.db_main.save_transaction_block(
                txns_to_commit,
                first_version,
                base_state_version,
                ledger_info_with_sigs,
                sync_commit,
                latest_in_memory_state,
                block_state_updates,
                sharded_state_cache,
            )
        }
    }
}

impl DbReader for FastSyncStorageWrapper {
    fn get_epoch_ending_ledger_infos(
        &self,
        start_epoch: u64,
        end_epoch: u64,
    ) -> Result<EpochChangeProof> {
        if self.is_fast_sync_bootstrap_finished() {
            let (ledger_info_with_sigs, more) = self
                .db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_epoch_ending_ledger_infos(start_epoch, end_epoch)?;
            Ok(EpochChangeProof::new(ledger_info_with_sigs, more))
        } else {
            let (ledger_info_with_sigs, more) = self
                .db_main
                .get_epoch_ending_ledger_infos(start_epoch, end_epoch)?;
            Ok(EpochChangeProof::new(ledger_info_with_sigs, more))
        }
    }

    fn get_transactions(
        &self,
        start_version: Version,
        batch_size: u64,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<TransactionListWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transactions(start_version, batch_size, ledger_version, fetch_events)
        } else {
            self.db_main
                .get_transactions(start_version, batch_size, ledger_version, fetch_events)
        }
    }

    fn get_transaction_by_hash(
        &self,
        hash: HashValue,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<Option<TransactionWithProof>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_by_hash(hash, ledger_version, fetch_events)
        } else {
            self.db_main
                .get_transaction_by_hash(hash, ledger_version, fetch_events)
        }
    }

    fn get_transaction_by_version(
        &self,
        version: Version,
        ledger_version: Version,
        fetch_events: bool,
    ) -> Result<TransactionWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_by_version(version, ledger_version, fetch_events)
        } else {
            self.db_main
                .get_transaction_by_version(version, ledger_version, fetch_events)
        }
    }

    fn get_first_txn_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_first_txn_version()
        } else {
            self.db_main.get_first_txn_version()
        }
    }

    fn get_first_viable_txn_version(&self) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_first_viable_txn_version()
        } else {
            self.db_main.get_first_viable_txn_version()
        }
    }

    fn get_first_write_set_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_first_write_set_version()
        } else {
            self.db_main.get_first_write_set_version()
        }
    }

    fn get_transaction_outputs(
        &self,
        start_version: Version,
        limit: u64,
        ledger_version: Version,
    ) -> Result<TransactionOutputListWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_outputs(start_version, limit, ledger_version)
        } else {
            self.db_main
                .get_transaction_outputs(start_version, limit, ledger_version)
        }
    }

    fn get_events(
        &self,
        event_key: &EventKey,
        start: u64,
        order: Order,
        limit: u64,
        ledger_version: Version,
    ) -> Result<Vec<EventWithVersion>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_events(event_key, start, order, limit, ledger_version)
        } else {
            self.db_main
                .get_events(event_key, start, order, limit, ledger_version)
        }
    }

    fn get_transaction_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<Transaction>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_iterator(start_version, limit)
        } else {
            self.db_main.get_transaction_iterator(start_version, limit)
        }
    }

    fn get_transaction_info_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<TransactionInfo>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_info_iterator(start_version, limit)
        } else {
            self.db_main
                .get_transaction_info_iterator(start_version, limit)
        }
    }

    fn get_events_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<Vec<ContractEvent>>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_events_iterator(start_version, limit)
        } else {
            self.db_main.get_events_iterator(start_version, limit)
        }
    }

    fn get_write_set_iterator(
        &self,
        start_version: Version,
        limit: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<WriteSet>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_write_set_iterator(start_version, limit)
        } else {
            self.get_write_set_iterator(start_version, limit)
        }
    }

    fn get_transaction_accumulator_range_proof(
        &self,
        start_version: Version,
        limit: u64,
        ledger_version: Version,
    ) -> Result<TransactionAccumulatorRangeProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_transaction_accumulator_range_proof(start_version, limit, ledger_version)
        } else {
            self.db_main.get_transaction_accumulator_range_proof(
                start_version,
                limit,
                ledger_version,
            )
        }
    }

    fn get_block_timestamp(&self, version: Version) -> Result<u64> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_block_timestamp(version)
        } else {
            self.db_main.get_block_timestamp(version)
        }
    }

    fn get_next_block_event(&self, version: Version) -> Result<(Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_next_block_event(version)
        } else {
            self.db_main.get_next_block_event(version)
        }
    }

    fn get_block_info_by_version(
        &self,
        version: Version,
    ) -> Result<(Version, Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_block_info_by_version(version)
        } else {
            self.db_main.get_block_info_by_version(version)
        }
    }

    fn get_block_info_by_height(&self, height: u64) -> Result<(Version, Version, NewBlockEvent)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_block_info_by_height(height)
        } else {
            self.db_main.get_block_info_by_height(height)
        }
    }

    fn get_last_version_before_timestamp(
        &self,
        _timestamp: u64,
        _ledger_version: Version,
    ) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_last_version_before_timestamp(_timestamp, _ledger_version)
        } else {
            self.db_main
                .get_last_version_before_timestamp(_timestamp, _ledger_version)
        }
    }

    fn get_latest_epoch_state(&self) -> Result<EpochState> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_latest_epoch_state()
        } else {
            self.db_main.get_latest_epoch_state()
        }
    }

    fn get_prefixed_state_value_iterator(
        &self,
        key_prefix: &StateKeyPrefix,
        cursor: Option<&StateKey>,
        version: Version,
    ) -> Result<Box<dyn Iterator<Item = Result<(StateKey, StateValue)>> + '_>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_prefixed_state_value_iterator(key_prefix, cursor, version)
        } else {
            self.db_main
                .get_prefixed_state_value_iterator(key_prefix, cursor, version)
        }
    }

    fn get_latest_ledger_info_option(&self) -> Result<Option<LedgerInfoWithSignatures>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_latest_ledger_info_option()
        } else {
            let res = self.db_main.get_latest_ledger_info_option()?;
            Ok(res)
        }
    }

    fn get_latest_ledger_info(&self) -> Result<LedgerInfoWithSignatures> {
        self.get_latest_ledger_info_option()
            .and_then(|opt| opt.ok_or_else(|| format_err!("Latest LedgerInfo not found.")))
    }

    fn get_latest_version(&self) -> Result<Version> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_latest_version()
        } else {
            self.db_main.get_latest_version()
        }
    }

    fn get_latest_state_checkpoint_version(&self) -> Result<Option<Version>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_latest_state_checkpoint_version()
        } else {
            self.db_main.get_latest_state_checkpoint_version()
        }
    }

    fn get_state_snapshot_before(
        &self,
        next_version: Version,
    ) -> Result<Option<(Version, HashValue)>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_snapshot_before(next_version)
        } else {
            self.db_main.get_state_snapshot_before(next_version)
        }
    }

    fn get_latest_commit_metadata(&self) -> Result<(Version, u64)> {
        let ledger_info_with_sig = self.get_latest_ledger_info()?;
        let ledger_info = ledger_info_with_sig.ledger_info();
        Ok((ledger_info.version(), ledger_info.timestamp_usecs()))
    }

    fn get_account_transaction(
        &self,
        address: AccountAddress,
        seq_num: u64,
        include_events: bool,
        ledger_version: Version,
    ) -> Result<Option<TransactionWithProof>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_account_transaction(address, seq_num, include_events, ledger_version)
        } else {
            self.db_main
                .get_account_transaction(address, seq_num, include_events, ledger_version)
        }
    }

    fn get_account_transactions(
        &self,
        address: AccountAddress,
        seq_num: u64,
        limit: u64,
        include_events: bool,
        ledger_version: Version,
    ) -> Result<AccountTransactionsWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_account_transactions(address, seq_num, limit, include_events, ledger_version)
        } else {
            self.db_main.get_account_transactions(
                address,
                seq_num,
                limit,
                include_events,
                ledger_version,
            )
        }
    }

    fn get_state_proof_with_ledger_info(
        &self,
        known_version: u64,
        ledger_info: LedgerInfoWithSignatures,
    ) -> Result<StateProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_proof_with_ledger_info(known_version, ledger_info)
        } else {
            self.db_main
                .get_state_proof_with_ledger_info(known_version, ledger_info)
        }
    }

    fn get_state_proof(&self, known_version: u64) -> Result<StateProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_proof(known_version)
        } else {
            self.db_main.get_state_proof(known_version)
        }
    }

    fn get_state_value_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<Option<StateValue>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_value_by_version(state_key, version)
        } else {
            self.db_main.get_state_value_by_version(state_key, version)
        }
    }

    fn get_state_value_with_version_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<Option<(Version, StateValue)>> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_value_with_version_by_version(state_key, version)
        } else {
            self.db_main
                .get_state_value_with_version_by_version(state_key, version)
        }
    }

    fn get_state_proof_by_version_ext(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<SparseMerkleProofExt> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_proof_by_version_ext(state_key, version)
        } else {
            self.db_main
                .get_state_proof_by_version_ext(state_key, version)
        }
    }

    fn get_state_value_with_proof_by_version_ext(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<(Option<StateValue>, SparseMerkleProofExt)> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_value_with_proof_by_version_ext(state_key, version)
        } else {
            self.db_main
                .get_state_value_with_proof_by_version_ext(state_key, version)
        }
    }

    fn get_state_value_with_proof_by_version(
        &self,
        state_key: &StateKey,
        version: Version,
    ) -> Result<(Option<StateValue>, SparseMerkleProof)> {
        self.get_state_value_with_proof_by_version_ext(state_key, version)
            .map(|(value, proof_ext)| (value, proof_ext.into()))
    }

    fn get_latest_executed_trees(&self) -> Result<ExecutedTrees> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_latest_executed_trees()
        } else {
            self.db_main.get_latest_executed_trees()
        }
    }

    fn get_epoch_ending_ledger_info(&self, known_version: u64) -> Result<LedgerInfoWithSignatures> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_epoch_ending_ledger_info(known_version)
        } else {
            self.db_main.get_epoch_ending_ledger_info(known_version)
        }
    }

    fn get_accumulator_root_hash(&self, _version: Version) -> Result<HashValue> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_accumulator_root_hash(_version)
        } else {
            self.db_main.get_accumulator_root_hash(_version)
        }
    }

    fn get_accumulator_consistency_proof(
        &self,
        _client_known_version: Option<Version>,
        _ledger_version: Version,
    ) -> Result<AccumulatorConsistencyProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_accumulator_consistency_proof(_client_known_version, _ledger_version)
        } else {
            self.db_main
                .get_accumulator_consistency_proof(_client_known_version, _ledger_version)
        }
    }

    fn get_accumulator_summary(
        &self,
        ledger_version: Version,
    ) -> Result<TransactionAccumulatorSummary> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_accumulator_summary(ledger_version)
        } else {
            self.db_main.get_accumulator_summary(ledger_version)
        }
    }

    fn get_state_leaf_count(&self, version: Version) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_leaf_count(version)
        } else {
            self.db_main.get_state_leaf_count(version)
        }
    }

    fn get_state_value_chunk_with_proof(
        &self,
        version: Version,
        start_idx: usize,
        chunk_size: usize,
    ) -> Result<StateValueChunkWithProof> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_value_chunk_with_proof(version, start_idx, chunk_size)
        } else {
            self.db_main
                .get_state_value_chunk_with_proof(version, start_idx, chunk_size)
        }
    }

    fn is_state_merkle_pruner_enabled(&self) -> Result<bool> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .is_state_merkle_pruner_enabled()
        } else {
            self.db_main.is_state_merkle_pruner_enabled()
        }
    }

    fn get_epoch_snapshot_prune_window(&self) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_epoch_snapshot_prune_window()
        } else {
            self.db_main.get_epoch_snapshot_prune_window()
        }
    }

    fn is_ledger_pruner_enabled(&self) -> Result<bool> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .is_ledger_pruner_enabled()
        } else {
            self.db_main.is_ledger_pruner_enabled()
        }
    }

    fn get_ledger_prune_window(&self) -> Result<usize> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_ledger_prune_window()
        } else {
            self.get_ledger_prune_window()
        }
    }

    fn get_table_info(&self, handle: TableHandle) -> Result<TableInfo> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_table_info(handle)
        } else {
            self.db_main.get_table_info(handle)
        }
    }

    fn indexer_enabled(&self) -> bool {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .indexer_enabled()
        } else {
            self.db_main.indexer_enabled()
        }
    }

    fn get_state_storage_usage(&self, version: Option<Version>) -> Result<StateStorageUsage> {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_secondary
                .as_ref()
                .expect("db_secondary is not initialized")
                .get_state_storage_usage(version)
        } else {
            self.db_main.get_state_storage_usage(version)
        }
    }
}
