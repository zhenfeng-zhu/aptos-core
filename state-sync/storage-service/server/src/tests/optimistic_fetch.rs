// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    moderator::RequestModerator,
    network::ResponseSender,
    optimistic_fetch,
    optimistic_fetch::OptimisticFetchRequest,
    storage::StorageReader,
    tests::{mock, utils},
};
use aptos_bounded_executor::BoundedExecutor;
use aptos_config::{config::StorageServiceConfig, network_id::PeerNetworkId};
use aptos_infallible::Mutex;
use aptos_storage_service_types::{
    requests::{
        DataRequest, NewTransactionOutputsWithProofRequest,
        NewTransactionsOrOutputsWithProofRequest, NewTransactionsWithProofRequest,
        StorageServiceRequest,
    },
    responses::{CompleteDataRange, StorageServerSummary},
};
use aptos_time_service::TimeService;
use aptos_types::{epoch_change::EpochChangeProof, ledger_info::LedgerInfoWithSignatures};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::channel::oneshot;
use lru::LruCache;
use rand::{rngs::OsRng, Rng};
use std::{sync::Arc, time::Duration};
use tokio::runtime::Handle;

#[tokio::test]
async fn test_peers_with_ready_optimistic_fetches() {
    // Create a mock time service
    let time_service = TimeService::mock();

    // Create two peers and optimistic fetch requests
    let peer_network_1 = PeerNetworkId::random();
    let peer_network_2 = PeerNetworkId::random();
    let optimistic_fetch_1 =
        create_optimistic_fetch_request(time_service.clone(), Some(1), Some(1));
    let optimistic_fetch_2 =
        create_optimistic_fetch_request(time_service.clone(), Some(10), Some(1));

    // Insert the optimistic fetches into the pending map
    let optimistic_fetches = Arc::new(DashMap::new());
    optimistic_fetches.insert(peer_network_1, optimistic_fetch_1);
    optimistic_fetches.insert(peer_network_2, optimistic_fetch_2);

    // Create epoch ending test data
    let epoch_ending_ledger_info = utils::create_epoch_ending_ledger_info(1, 5);
    let epoch_change_proof = EpochChangeProof {
        ledger_info_with_sigs: vec![epoch_ending_ledger_info],
        more: false,
    };

    // Create the mock db reader
    let mut db_reader = mock::create_mock_db_reader();
    utils::expect_get_epoch_ending_ledger_infos(&mut db_reader, 1, 2, epoch_change_proof);

    // Create the storage reader
    let storage_service_config = StorageServiceConfig::default();
    let storage_reader = StorageReader::new(storage_service_config, Arc::new(db_reader));

    // Create test data with an empty storage server summary
    let bounded_executor = BoundedExecutor::new(100, Handle::current());
    let cached_storage_server_summary =
        Arc::new(ArcSwap::from(Arc::new(StorageServerSummary::default())));
    let lru_response_cache = Arc::new(Mutex::new(LruCache::new(0)));
    let request_moderator = Arc::new(RequestModerator::new(
        cached_storage_server_summary.clone(),
        mock::create_peers_and_metadata(vec![]),
        storage_service_config,
        time_service.clone(),
    ));

    // Verify that there are no peers with ready optimistic fetches
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor.clone(),
            storage_service_config,
            cached_storage_server_summary.clone(),
            optimistic_fetches.clone(),
            lru_response_cache.clone(),
            request_moderator.clone(),
            storage_reader.clone(),
            time_service.clone(),
        )
        .await
        .unwrap();
    assert!(peers_with_ready_optimistic_fetches.is_empty());

    // Update the storage server summary so that there is new data for optimistic fetch 1
    let synced_ledger_info =
        update_storage_server_summary(cached_storage_server_summary.clone(), 2, 1);

    // Verify that optimistic fetch 1 is ready
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor.clone(),
            storage_service_config,
            cached_storage_server_summary.clone(),
            optimistic_fetches.clone(),
            lru_response_cache.clone(),
            request_moderator.clone(),
            storage_reader.clone(),
            time_service.clone(),
        )
        .await
        .unwrap();
    assert_eq!(peers_with_ready_optimistic_fetches, vec![(
        peer_network_1,
        synced_ledger_info
    )]);

    // Manually remove optimistic fetch 1 from the map
    optimistic_fetches.remove(&peer_network_1);

    // Update the storage server summary so that there is new data for optimistic fetch 2,
    // but the optimistic fetch is invalid because it doesn't respect an epoch boundary.
    let _ = update_storage_server_summary(cached_storage_server_summary.clone(), 100, 2);

    // Verify that optimistic fetch 2 is not returned because it was invalid
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor,
            storage_service_config,
            cached_storage_server_summary,
            optimistic_fetches,
            lru_response_cache,
            request_moderator,
            storage_reader,
            time_service,
        )
        .await
        .unwrap();
    assert_eq!(peers_with_ready_optimistic_fetches, vec![]);

    // Verify that optimistic fetches no longer contains peer 2
    assert!(peers_with_ready_optimistic_fetches.is_empty());
}

#[tokio::test]
async fn test_remove_expired_optimistic_fetches() {
    // Create a storage service config
    let max_optimistic_fetch_period_ms = 100;
    let storage_service_config = StorageServiceConfig {
        max_optimistic_fetch_period_ms,
        ..Default::default()
    };

    // Create the mock storage reader and time service
    let db_reader = mock::create_mock_db_reader();
    let storage = StorageReader::new(storage_service_config, Arc::new(db_reader));
    let time_service = TimeService::mock();

    // Create the test components
    let bounded_executor = BoundedExecutor::new(100, Handle::current());
    let cached_storage_server_summary =
        Arc::new(ArcSwap::from(Arc::new(StorageServerSummary::default())));
    let lru_response_cache = Arc::new(Mutex::new(LruCache::new(0)));
    let request_moderator = Arc::new(RequestModerator::new(
        cached_storage_server_summary.clone(),
        mock::create_peers_and_metadata(vec![]),
        storage_service_config,
        time_service.clone(),
    ));

    // Create the first batch of test optimistic fetches
    let num_optimistic_fetches_in_batch = 10;
    let optimistic_fetches = Arc::new(DashMap::new());
    for _ in 0..num_optimistic_fetches_in_batch {
        let optimistic_fetch =
            create_optimistic_fetch_request(time_service.clone(), Some(9), Some(9));
        optimistic_fetches.insert(PeerNetworkId::random(), optimistic_fetch);
    }

    // Verify the number of active optimistic fetches
    assert_eq!(optimistic_fetches.len(), num_optimistic_fetches_in_batch);

    // Elapse a small amount of time (not enough to expire the optimistic fetches)
    time_service
        .clone()
        .into_mock()
        .advance_async(Duration::from_millis(max_optimistic_fetch_period_ms / 2))
        .await;

    // Update the storage server summary so that there is new data
    let _ = update_storage_server_summary(cached_storage_server_summary.clone(), 1, 1);

    // Remove the expired optimistic fetches and verify none were removed
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor.clone(),
            storage_service_config,
            cached_storage_server_summary.clone(),
            optimistic_fetches.clone(),
            lru_response_cache.clone(),
            request_moderator.clone(),
            storage.clone(),
            time_service.clone(),
        )
        .await
        .unwrap();
    assert!(peers_with_ready_optimistic_fetches.is_empty());
    assert_eq!(optimistic_fetches.len(), num_optimistic_fetches_in_batch);

    // Create another batch of optimistic fetches
    for _ in 0..num_optimistic_fetches_in_batch {
        let optimistic_fetch =
            create_optimistic_fetch_request(time_service.clone(), Some(9), Some(9));
        optimistic_fetches.insert(PeerNetworkId::random(), optimistic_fetch);
    }

    // Verify the new number of active optimistic fetches
    assert_eq!(
        optimistic_fetches.len(),
        num_optimistic_fetches_in_batch * 2
    );

    // Elapse enough time to expire the first batch of optimistic fetches
    time_service
        .clone()
        .into_mock()
        .advance_async(Duration::from_millis(max_optimistic_fetch_period_ms))
        .await;

    // Remove the expired optimistic fetches and verify the first batch was removed
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor.clone(),
            storage_service_config,
            cached_storage_server_summary.clone(),
            optimistic_fetches.clone(),
            lru_response_cache.clone(),
            request_moderator.clone(),
            storage.clone(),
            time_service.clone(),
        )
        .await
        .unwrap();
    assert!(peers_with_ready_optimistic_fetches.is_empty());
    assert_eq!(optimistic_fetches.len(), num_optimistic_fetches_in_batch);

    // Elapse enough time to expire the second batch of optimistic fetches
    time_service
        .clone()
        .into_mock()
        .advance_async(Duration::from_millis(max_optimistic_fetch_period_ms + 1))
        .await;

    // Remove the expired optimistic fetches and verify the second batch was removed
    let peers_with_ready_optimistic_fetches =
        optimistic_fetch::get_peers_with_ready_optimistic_fetches(
            bounded_executor,
            storage_service_config,
            cached_storage_server_summary.clone(),
            optimistic_fetches.clone(),
            lru_response_cache,
            request_moderator,
            storage.clone(),
            time_service.clone(),
        )
        .await
        .unwrap();
    assert!(peers_with_ready_optimistic_fetches.is_empty());
    assert!(optimistic_fetches.is_empty());
}

/// Creates a random request for optimistic fetch data
fn create_optimistic_fetch_data_request(
    known_version: Option<u64>,
    known_epoch: Option<u64>,
) -> DataRequest {
    let known_version = known_version.unwrap_or_default();
    let known_epoch = known_epoch.unwrap_or_default();

    // Generate the random data request
    let mut rng = OsRng;
    let random_number: u8 = rng.gen();
    match random_number % 3 {
        0 => DataRequest::GetNewTransactionsWithProof(NewTransactionsWithProofRequest {
            known_version,
            known_epoch,
            include_events: false,
        }),
        1 => {
            DataRequest::GetNewTransactionOutputsWithProof(NewTransactionOutputsWithProofRequest {
                known_version,
                known_epoch,
            })
        },
        2 => DataRequest::GetNewTransactionsOrOutputsWithProof(
            NewTransactionsOrOutputsWithProofRequest {
                known_version,
                known_epoch,
                include_events: true,
                max_num_output_reductions: 1,
            },
        ),
        num => panic!("This shouldn't be possible! Got num: {:?}", num),
    }
}

/// Creates a random optimistic fetch request
fn create_optimistic_fetch_request(
    time_service: TimeService,
    known_version: Option<u64>,
    known_epoch: Option<u64>,
) -> OptimisticFetchRequest {
    // Create a storage service request
    let data_request = create_optimistic_fetch_data_request(known_version, known_epoch);
    let storage_service_request = StorageServiceRequest::new(data_request, true);

    // Create the response sender
    let (callback, _) = oneshot::channel();
    let response_sender = ResponseSender::new(callback);

    // Create and return the optimistic fetch request
    OptimisticFetchRequest::new(storage_service_request, response_sender, time_service)
}

/// Updates the storage server summary with new data and returns the synced ledger info
fn update_storage_server_summary(
    cached_storage_server_summary: Arc<ArcSwap<StorageServerSummary>>,
    highest_synced_version: u64,
    highest_synced_epoch: u64,
) -> LedgerInfoWithSignatures {
    // Create the storage server summary and synced ledger info
    let mut storage_server_summary = StorageServerSummary::default();
    let highest_synced_ledger_info =
        utils::create_test_ledger_info_with_sigs(highest_synced_epoch, highest_synced_version);

    // Update the epoch ending ledger infos and synced ledger info
    storage_server_summary
        .data_summary
        .epoch_ending_ledger_infos = Some(CompleteDataRange::new(0, highest_synced_epoch).unwrap());
    storage_server_summary.data_summary.synced_ledger_info =
        Some(highest_synced_ledger_info.clone());

    // Update the cached storage server summary
    cached_storage_server_summary.store(Arc::new(storage_server_summary));

    highest_synced_ledger_info
}
