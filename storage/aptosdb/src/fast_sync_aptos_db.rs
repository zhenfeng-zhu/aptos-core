// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    backup::backup_handler::DbState,
    metrics::{
        BACKUP_EPOCH_ENDING_EPOCH, BACKUP_STATE_SNAPSHOT_LEAF_IDX, BACKUP_STATE_SNAPSHOT_VERSION,
        BACKUP_TXN_VERSION,
    },
    AptosDB,
};
use anyhow::{anyhow, ensure, Context, Result};
use aptos_config::config::{BootstrappingMode, NodeConfig};
use aptos_crypto::HashValue;
use aptos_storage_interface::{
    cached_state_view::ShardedStateCache, state_delta::StateDelta, DbReader, DbWriter,
    ExecutedTrees, Order, StateSnapshotReceiver,
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

pub const SECONDARY_DB_DIR: &str = "fast_sync_secondary";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FastSyncStatus {
    UNKNOWN,
    STARTED,
    FINISHED,
}

/// This is a wrapper around [AptosDB] that is used to bootstrap the node for fast sync mode
pub struct FastSyncStorageWrapper {
    // Used for storing genesis data during fast sync
    temporary_db_with_genesis: AptosDB,
    // Used for restoring fast sync snapshot and all the read/writes afterwards
    db_for_fast_sync: Option<AptosDB>,
    // This is for reading the fast_sync status to determine which db to use
    fast_sync_status: Arc<RwLock<FastSyncStatus>>,
}

impl FastSyncStorageWrapper {
    /// If the db is empty and configured to do fast sync, we return a FastSyncStorageWrapper
    /// Otherwise, we returns AptosDB directly and the FastSyncStorageWrapper is None
    pub fn initialize_dbs(
        config: &NodeConfig,
    ) -> Result<(Option<AptosDB>, Option<FastSyncStorageWrapper>)> {
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

        // when the db is empty and configured to do fast sync, we will create a second DB
        if config.state_sync.state_sync_driver.bootstrapping_mode
            == BootstrappingMode::DownloadLatestStates
            && (db_main.ledger_store.get_latest_version().map_or(0, |v| v) == 0)
        {
            db_dir.push(SECONDARY_DB_DIR);
            let secondary_db = AptosDB::open(
                db_dir.as_path(),
                false,
                config.storage.storage_pruner_config,
                config.storage.rocksdb_configs,
                config.storage.enable_indexer,
                config.storage.buffered_state_target_items,
                config.storage.max_num_nodes_per_lru_cache_shard,
            )
            .map_err(|err| anyhow!("Secondary DB failed to open {}", err))?;

            Ok((
                None,
                Some(FastSyncStorageWrapper {
                    temporary_db_with_genesis: secondary_db,
                    db_for_fast_sync: Some(db_main),
                    fast_sync_status: Arc::new(RwLock::new(FastSyncStatus::UNKNOWN)),
                }),
            ))
        } else {
            Ok((Some(db_main), None))
        }
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

    /// Check if the fast sync started already
    fn is_fast_sync_bootstrap_started(&self) -> bool {
        let status = self.get_fast_sync_status().unwrap();
        status == FastSyncStatus::STARTED
    }

    pub(crate) fn get_aptos_db_read_ref(&self) -> &AptosDB {
        if self.is_fast_sync_bootstrap_finished() {
            self.db_for_fast_sync
                .as_ref()
                .expect("db_secondary is not initialized")
        } else {
            &self.temporary_db_with_genesis
        }
    }

    pub(crate) fn get_aptos_db_write_ref(&self) -> &AptosDB {
        if self.is_fast_sync_bootstrap_started() || self.is_fast_sync_bootstrap_finished() {
            self.db_for_fast_sync
                .as_ref()
                .expect("db_secondary is not initialized")
        } else {
            &self.temporary_db_with_genesis
        }
    }

    /// Provide an iterator to underlying data for reading transactions
    pub fn get_transaction_iter(
        &self,
        start_version: Version,
        num_transactions: usize,
    ) -> Result<
        impl Iterator<Item = Result<(Transaction, TransactionInfo, Vec<ContractEvent>, WriteSet)>> + '_,
    > {
        let txn_iter = self
            .get_aptos_db_read_ref()
            .transaction_store
            .get_transaction_iter(start_version, num_transactions)?;
        let mut txn_info_iter = self
            .get_aptos_db_read_ref()
            .ledger_store
            .get_transaction_info_iter(start_version, num_transactions)?;
        let mut event_vec_iter = self
            .get_aptos_db_read_ref()
            .event_store
            .get_events_by_version_iter(start_version, num_transactions)?;
        let mut write_set_iter = self
            .get_aptos_db_read_ref()
            .transaction_store
            .get_write_set_iter(start_version, num_transactions)?;

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
        let ledger_store = self.get_aptos_db_read_ref().ledger_store.clone();
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
            .get_aptos_db_read_ref()
            .state_store
            .clone()
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
        self.get_aptos_db_read_ref()
            .state_store
            .clone()
            .get_value_range_proof(rightmost_key, version)
    }

    /// Gets the epoch, committed version, and synced version of the DB.
    pub fn get_db_state(&self) -> Result<Option<DbState>> {
        Ok(self
            .get_aptos_db_read_ref()
            .ledger_store
            .clone()
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
        let ledger_store = self.get_aptos_db_read_ref().ledger_store.clone();
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
            .get_aptos_db_read_ref()
            .ledger_store
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
        *self
            .fast_sync_status
            .write()
            .expect("Failed to get write lock of fast sync status") = FastSyncStatus::STARTED;
        self.get_aptos_db_write_ref()
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
        self.get_aptos_db_write_ref().finalize_state_snapshot(
            version,
            output_with_proof,
            ledger_infos,
        )?;
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
        self.get_aptos_db_write_ref().save_transactions(
            txns_to_commit,
            first_version,
            base_state_version,
            ledger_info_with_sigs,
            sync_commit,
            latest_in_memory_state,
        )
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
        self.get_aptos_db_write_ref().save_transaction_block(
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

macro_rules! aptos_db_read_fn {
    ($(fn $name:ident(&self $(, $arg: ident : $ty: ty $(,)?)*) -> $return_type:ty,)+) => {
        $(fn $name(&self, $($arg: $ty),*) -> $return_type {
            self.get_aptos_db_read_ref().$name($($arg),*)
        })+
    };
}

impl DbReader for FastSyncStorageWrapper {
    aptos_db_read_fn!(
        fn get_transactions(
            &self,
            start_version: Version,
            batch_size: u64,
            ledger_version: Version,
            fetch_events: bool,
        ) -> Result<TransactionListWithProof>,

        fn get_transaction_by_hash(
            &self,
            hash: HashValue,
            ledger_version: Version,
            fetch_events: bool,
        ) -> Result<Option<TransactionWithProof>>,

        fn get_transaction_by_version(
            &self,
            version: Version,
            ledger_version: Version,
            fetch_events: bool,
        ) -> Result<TransactionWithProof>,

        fn get_first_txn_version(&self) -> Result<Option<Version>>,

        fn get_first_viable_txn_version(&self) -> Result<Version>,

        fn get_first_write_set_version(&self) -> Result<Option<Version>>,

        fn get_transaction_outputs(
            &self,
            start_version: Version,
            limit: u64,
            ledger_version: Version,
        ) -> Result<TransactionOutputListWithProof>,

        fn get_events(
            &self,
            event_key: &EventKey,
            start: u64,
            order: Order,
            limit: u64,
            ledger_version: Version,
            ) -> Result<Vec<EventWithVersion>>,

        fn get_transaction_iterator(
            &self,
            start_version: Version,
            limit: u64,
            ) -> Result<Box<dyn Iterator<Item = Result<Transaction>> + '_>>,

        fn get_transaction_info_iterator(
            &self,
            start_version: Version,
            limit: u64,
        ) -> Result<Box<dyn Iterator<Item = Result<TransactionInfo>> + '_>>,

        fn get_events_iterator(
            &self,
            start_version: Version,
            limit: u64,
        ) -> Result<Box<dyn Iterator<Item = Result<Vec<ContractEvent>>> + '_>>,

        fn get_write_set_iterator(
            &self,
            start_version: Version,
            limit: u64,
        ) -> Result<Box<dyn Iterator<Item = Result<WriteSet>> + '_>>,

        fn get_transaction_accumulator_range_proof(
            &self,
            start_version: Version,
            limit: u64,
            ledger_version: Version,
        ) -> Result<TransactionAccumulatorRangeProof>,

        fn get_block_timestamp(&self, version: Version) -> Result<u64>,

        fn get_next_block_event(&self, version: Version) -> Result<(Version, NewBlockEvent)>,

        fn get_block_info_by_version(
            &self,
            version: Version,
        ) -> Result<(Version, Version, NewBlockEvent)>,

        fn get_block_info_by_height(&self, height: u64) -> Result<(Version, Version, NewBlockEvent)>,

        fn get_last_version_before_timestamp(
            &self,
            _timestamp: u64,
            _ledger_version: Version,
        ) -> Result<Version>,

        fn get_latest_epoch_state(&self) -> Result<EpochState>,

        fn get_prefixed_state_value_iterator(
            &self,
            key_prefix: &StateKeyPrefix,
            cursor: Option<&StateKey>,
            version: Version,
        ) -> Result<Box<dyn Iterator<Item = Result<(StateKey, StateValue)>> + '_>>,

        fn get_latest_ledger_info_option(&self) -> Result<Option<LedgerInfoWithSignatures>>,

        fn get_latest_ledger_info(&self) -> Result<LedgerInfoWithSignatures>,

        fn get_latest_version(&self) -> Result<Version>,

        fn get_latest_state_checkpoint_version(&self) -> Result<Option<Version>>,

        fn get_state_snapshot_before(
            &self,
            next_version: Version,
        ) -> Result<Option<(Version, HashValue)>>,

        fn get_latest_commit_metadata(&self) -> Result<(Version, u64)>,

        fn get_account_transaction(
            &self,
            address: AccountAddress,
            seq_num: u64,
            include_events: bool,
            ledger_version: Version,
        ) -> Result<Option<TransactionWithProof>>,

        fn get_account_transactions(
            &self,
            address: AccountAddress,
            seq_num: u64,
            limit: u64,
            include_events: bool,
            ledger_version: Version,
        ) -> Result<AccountTransactionsWithProof>,

        fn get_state_proof_with_ledger_info(
            &self,
            known_version: u64,
            ledger_info: LedgerInfoWithSignatures,
        ) -> Result<StateProof>,

        fn get_state_proof(&self, known_version: u64) -> Result<StateProof>,

        fn get_state_value_by_version(
            &self,
            state_key: &StateKey,
            version: Version,
        ) -> Result<Option<StateValue>>,

        fn get_state_value_with_version_by_version(
            &self,
            state_key: &StateKey,
            version: Version,
        ) -> Result<Option<(Version, StateValue)>>,

        fn get_state_proof_by_version_ext(
            &self,
            state_key: &StateKey,
            version: Version,
        ) -> Result<SparseMerkleProofExt>,

        fn get_state_value_with_proof_by_version_ext(
            &self,
            state_key: &StateKey,
            version: Version,
        ) -> Result<(Option<StateValue>, SparseMerkleProofExt)>,

        fn get_state_value_with_proof_by_version(
            &self,
            state_key: &StateKey,
            version: Version,
        ) -> Result<(Option<StateValue>, SparseMerkleProof)>,

        fn get_latest_executed_trees(&self) -> Result<ExecutedTrees>,

        fn get_epoch_ending_ledger_info(&self, known_version: u64) -> Result<LedgerInfoWithSignatures>,

        fn get_accumulator_root_hash(&self, _version: Version) -> Result<HashValue>,

        fn get_accumulator_consistency_proof(
            &self,
            _client_known_version: Option<Version>,
            _ledger_version: Version,
        ) -> Result<AccumulatorConsistencyProof>,

        fn get_accumulator_summary(
            &self,
            ledger_version: Version,
        ) -> Result<TransactionAccumulatorSummary>,

        fn get_state_leaf_count(&self, version: Version) -> Result<usize>,

        fn get_state_value_chunk_with_proof(
            &self,
            version: Version,
            start_idx: usize,
            chunk_size: usize,
        ) -> Result<StateValueChunkWithProof>,

        fn is_state_merkle_pruner_enabled(&self) -> Result<bool>,

        fn get_epoch_snapshot_prune_window(&self) -> Result<usize>,

        fn is_ledger_pruner_enabled(&self) -> Result<bool>,

        fn get_ledger_prune_window(&self) -> Result<usize>,

        fn get_table_info(&self, handle: TableHandle) -> Result<TableInfo>,

        fn indexer_enabled(&self) -> bool,

        fn get_state_storage_usage(&self, version: Option<Version>) -> Result<StateStorageUsage>,
    );

    fn get_epoch_ending_ledger_infos(
        &self,
        start_epoch: u64,
        end_epoch: u64,
    ) -> Result<EpochChangeProof> {
        let (ledger_info, flag) = self
            .get_aptos_db_read_ref()
            .get_epoch_ending_ledger_infos(start_epoch, end_epoch)?;
        Ok(EpochChangeProof::new(ledger_info, flag))
    }
}
