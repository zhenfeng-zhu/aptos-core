// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! TODO(aldenhu): doc

use aptos_types::state_store::state_key::StateKey;

pub(crate) type TxnIndex = usize;

pub(crate) const BASE_VERSION: TxnIndex = TxnIndex::MAX;

pub(crate) type VersionedKey = (StateKey, TxnIndex);
