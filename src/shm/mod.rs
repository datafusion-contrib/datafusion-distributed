// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared-memory transport.
//!
//! A non-gRPC [`ChannelResolver`] for co-located execution, where "workers" are tasks or
//! parallel processes sharing one machine and communicating over a shared-memory mesh rather
//! than gRPC. The transport-mechanism pieces (the MPSC ring, framing, routing, cooperative
//! drain) live here as a reusable library; an embedder supplies the two platform primitives via
//! small extension points: how to allocate the shared buffer, and how to wake a blocked consumer.
//!
//! The point of hosting it in this crate is testing: the in-process instantiation runs real
//! distributed queries through the transport in this crate's CI, so an upstream rebase that
//! breaks the channel-protocol contract fails here, before any downstream embedder rebuilds.
//!
//! [`ChannelResolver`]: crate::ChannelResolver
//!
//! Two assumptions an embedder signs up for:
//! - Execution is cooperative on a current-thread runtime: consumers spin on
//!   `try_pop` + `yield_now` and producers drain their own inbound while blocked, instead of
//!   parking on the `Wakeup` extension point. On a multi-thread runtime each stream burns a core while
//!   idle.
//! - Inbound frames demux into unbounded per-channel buffers, so a consumer that falls behind
//!   buffers the in-flight intermediate result in process memory. The rings in shared memory
//!   stay bounded; the overflow lives on the consumer's heap.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

mod dsm;
mod mesh;
mod mpsc_ring;
mod runtime;
// Deferred: the self-hosting default transport was built on the removed `WorkerTransport`/
// `WorkerDispatch` dispatch umbrella, which the `ChannelResolver` model has no analog for; its
// no-gRPC-default role is now served by `InProcessChannelResolver` and its ring-exercising role by
// the `in_process` test, so it stays gated out until reimplemented on `coordinator_channel`.
#[cfg(any())]
mod self_hosted;
mod setup;
mod sink;
mod transport;

// Curated public surface an embedder consumes. The embedder allocates the shared buffer and
// supplies the two extension points (`Wakeup`, `Interrupt`); everything else is built here.
pub use mpsc_ring::{NO_RECEIVER_TOKEN, Wakeup};
pub use runtime::{InProcessWorkerResolver, MppMesh, ShmChannelResolver, proc_for_task};
pub use setup::{
    LeaderAttach, WorkerAttach, collect_task_metrics, dsm_region_bytes, install_work_unit_channels,
    leader_setup, region_total, run_worker_fragment, worker_setup,
};
pub use sink::{PartitionSink, WorkerSink};
pub use transport::{
    CooperativeDrainSet, Interrupt, MppFrameHeader, MppPartitionSink, MppSender, NoInterrupt,
    SendBatchStats, SetPlanFrame,
};

/// Out-of-DSM liveness flag shared by the ring handles from one attach. The embedder flips it to
/// `false` from its dsm-detach callback while the segment is still mapped, so a handle dropped
/// afterward (e.g. by a memory-context reset) no-ops instead of dereferencing freed memory.
pub type AliveFlag = Arc<AtomicBool>;

// In-process instantiation + the end-to-end test that runs a real distributed query through the
// transport with no Postgres. Test-only: it's how an upstream rebase that breaks the transport
// contract fails in this crate's CI.
#[cfg(test)]
mod in_process;
