// Copyright (c) Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    dag::{
        anchor_election::RoundRobinAnchorElection,
        bullshark::Bullshark,
        dag::Dag,
        reliable_broadcast::{ReliableBroadcast, ReliableBroadcastCommand},
    },
    network::{DagSender, NetworkSender},
    payload_manager::PayloadManager,
    round_manager::VerifiedEvent,
    state_replication::{PayloadClient, StateComputer},
};
use aptos_channels::aptos_channel;
use aptos_config::config::DagConfig;
use aptos_consensus_types::{
    common::{Author, Round},
    node::{CertifiedNode, CertifiedNodeAck, CertifiedNodeRequest, Node, NodeMetaData},
};
use aptos_infallible::Mutex;
use aptos_logger::spawn_named;
use aptos_types::{
    validator_signer::ValidatorSigner, validator_verifier::ValidatorVerifier, PeerId,
};
use futures::StreamExt;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::mpsc::Sender, time};

pub struct DagDriver {
    epoch: u64,
    round: Round,
    author: Author,
    config: DagConfig,
    payload_client: Arc<dyn PayloadClient>,
    timeout: bool,
    network_sender: NetworkSender,
    // TODO: Should we clean more often than once an epoch?
    dag: Dag,
    bullshark: Arc<Mutex<Bullshark>>,
    rb_tx: Sender<ReliableBroadcastCommand>,
    network_msg_rx: aptos_channel::Receiver<PeerId, VerifiedEvent>,
}

impl DagDriver {
    pub fn new(
        epoch: u64,
        author: Author,
        config: DagConfig,
        payload_client: Arc<dyn PayloadClient>,
        network_sender: NetworkSender,
        verifier: ValidatorVerifier,
        validator_signer: Arc<ValidatorSigner>,
        rb_network_msg_rx: aptos_channel::Receiver<PeerId, VerifiedEvent>,
        network_msg_rx: aptos_channel::Receiver<PeerId, VerifiedEvent>,
        payload_manager: Arc<PayloadManager>,
        state_computer: Arc<dyn StateComputer>,
    ) -> Self {
        // let (dag_bullshark_tx, dag_bullshark_rx) = tokio::sync::mpsc::channel(config.channel_size);
        let (rb_tx, rb_rx) = tokio::sync::mpsc::channel(config.channel_size);

        let rb = ReliableBroadcast::new(
            epoch,
            network_sender.clone(),
            verifier.clone(),
            validator_signer,
        );

        let proposer_election = Arc::new(RoundRobinAnchorElection::new(&verifier));
        let bullshark = Arc::new(Mutex::new(Bullshark::new(
            state_computer,
            proposer_election.clone(),
            verifier.clone(),
        )));

        spawn_named!("reliable_broadcast", rb.start(rb_network_msg_rx, rb_rx));
        // spawn_named!("bullshark", bullshark.start(dag_bullshark_rx));

        Self {
            epoch,
            round: 0,
            author,
            config,
            payload_client,
            timeout: false,
            network_sender,
            dag: Dag::new(
                epoch,
                bullshark.clone(),
                verifier.clone(),
                proposer_election,
                payload_manager,
            ),
            bullshark,
            rb_tx,
            network_msg_rx,
        }
    }

    async fn remote_fetch_missing_nodes(&self) {
        for (node_meta_data, nodes_to_request) in self.dag.missing_nodes_metadata() {
            let request = CertifiedNodeRequest::new(node_meta_data, self.author);
            self.network_sender
                .send_certified_node_request(request, nodes_to_request)
                .await;
        }
    }

    async fn handle_node_request(&mut self, node_request: CertifiedNodeRequest) {
        if let Some(certified_node) = self.dag.get_node(&node_request) {
            self.network_sender
                .send_certified_node(
                    certified_node.clone(),
                    Some(vec![node_request.requester()]),
                    false,
                )
                .await
        }
    }

    async fn create_node(&mut self, parents: HashSet<NodeMetaData>) -> Node {
        let payload_filter = self.bullshark.lock().pending_payload();
        let payload = self
            .payload_client
            .pull_payload_for_dag(
                self.round,
                self.config.max_node_txns,
                self.config.max_node_bytes,
                payload_filter,
            )
            .await
            .expect("DAG: fail to retrieve payload");
        Node::new(self.epoch, self.round, self.author, payload, parents)
    }

    async fn try_advance_round(&mut self) -> Option<Node> {
        if let Some(parents) = self.dag.try_advance_round(self.timeout) {
            self.round += 1;
            Some(self.create_node(parents).await)
        } else {
            None
        }
    }

    async fn handle_certified_node(&mut self, certified_node: CertifiedNode, ack_required: bool) {
        let digest = certified_node.digest();
        let source = certified_node.source();
        self.dag.try_add_node(certified_node).await;

        if ack_required {
            let ack = CertifiedNodeAck::new(self.epoch, digest, self.author);
            self.network_sender
                .send_certified_node_ack(ack, vec![source])
                .await
        }
    }

    pub(crate) async fn start(mut self) {
        let node = self.create_node(HashSet::new()).await;
        self.rb_tx
            .send(ReliableBroadcastCommand::BroadcastRequest(node))
            .await
            .expect("dag: reliable broadcast receiver dropped");

        let mut interval_missing_nodes = time::interval(Duration::from_millis(500)); // time out should be slightly more than one network round trip.
        let mut interval_timeout = time::interval(Duration::from_millis(1000)); // similar to leader timeout in our consensus
        loop {
            // TODO: shutdown
            tokio::select! {
                biased;

                _ = interval_missing_nodes.tick() => {
                self.remote_fetch_missing_nodes().await
            },

                _ = interval_timeout.tick() => {
                    if self.timeout == false {
                        self.timeout = true;
                        if let Some(node) = self.try_advance_round().await {
                            self.rb_tx.send(ReliableBroadcastCommand::BroadcastRequest(node)).await.expect("dag: reliable broadcast receiver dropped");
                            self.timeout = false;
                            interval_timeout.reset();
                        }

                    }
                }

            Some(msg) = self.network_msg_rx.next() => {
                    match msg {

                        VerifiedEvent::CertifiedNodeMsg(certified_node, ack_required) => {

                            self.handle_certified_node(*certified_node, ack_required).await;
                            if let Some(node) = self.try_advance_round().await {
                                self.rb_tx.send(ReliableBroadcastCommand::BroadcastRequest(node)).await.expect("dag: reliable broadcast receiver dropped");
                                self.timeout = false;
                                interval_timeout.reset();
                            }

                        },

                        VerifiedEvent::CertifiedNodeRequestMsg(node_request) => {
                            self.handle_node_request(*node_request).await;
                    }
                    _ => unreachable!("reliable broadcast got wrong messsgae"),
                    }
                },
            }
        }
    }
}
