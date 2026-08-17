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
//! than gRPC. The transport-mechanism pieces (the MPSC ring, framing, routing, async inbound
//! reactor, IPC signaling mesh) live here as a reusable library; an embedder supplies the platform
//! primitives via small extension points: how to allocate the shared buffer, and optional wakeup hooks.
//!
//! The shared-memory transport implements a demand-driven, pull-based RPC model: downstream
//! consumers request partition execution on demand via [`ExecuteTaskFrame`], matching the
//! canonical gRPC task execution model over shared-memory channels.
//!
//! The point of hosting it in this crate is testing: the in-process instantiation runs real
//! distributed queries through the transport in this crate's CI, so an upstream rebase that
//! breaks the channel-protocol contract fails here, before any downstream embedder rebuilds.
//!
//! [`ChannelResolver`]: crate::ChannelResolver
//! [`ExecuteTaskFrame`]: transport::ExecuteTaskFrame
//!
//! Two characteristics of the transport:
//! - Execution is fully asynchronous: producers and consumers signal readiness across processes
//!   via IPC sockets or event notifications rather than busy-waiting.
//! - Inbound frames demux into per-channel buffers, so intermediate results flow through
//!   reactively managed queues while the rings in shared memory stay bounded.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

mod dsm;
mod ipc;
mod mesh;
mod mpsc_ring;
mod notifier;
mod runtime;
// Deferred: the self-hosting default transport was built on the removed `WorkerTransport`/
// `WorkerDispatch` dispatch umbrella, which the `ChannelResolver` model has no analog for; its
// no-gRPC-default role is now served by `LocalWorkerContext` and its ring-exercising role by
// the `in_process` test, so it stays gated out until reimplemented on `coordinator_channel`.
#[cfg(any())]
mod self_hosted;
mod setup;
mod sink;
mod transport;

// Curated public surface an embedder consumes. The embedder allocates the shared buffer and
// supplies the two extension points (`Wakeup`, `Interrupt`); everything else is built here.
pub use ipc::{IpcMeshNotifier, QuerySocketScope};
pub use mpsc_ring::{NO_RECEIVER_TOKEN, Wakeup};
pub use runtime::{InProcessWorkerResolver, MppMesh, ShmChannelResolver, proc_for_task};
pub use setup::{
    LeaderSession, WorkerSession, collect_task_metrics, dsm_n_procs, dsm_region_bytes,
    install_work_unit_channels, leader_setup, leader_setup_ipc, leader_setup_with_notifier,
    region_total, run_execute_task_loop, run_worker_fragment, worker_setup, worker_setup_ipc,
    worker_setup_with_notifier,
};
pub use sink::{PartitionSink, WorkerSink};
pub use transport::{
    ExecuteTaskFrame, ExecuteTaskRx, Interrupt, LocalDrainPartitionSink, MppDataStreamKey,
    MppFrameHeader, MppPartitionSink, MppSender, NoInterrupt, SendBatchStats, SetPlanFrame,
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
