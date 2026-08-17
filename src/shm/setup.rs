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

//! Mesh construction over a caller-supplied shared buffer, the extension point between an embedder's buffer
//! allocation and the transport.
//!
//! The embedder allocates the shared region (a PG `dsm_segment`, or a heap buffer in-process),
//! sizes it with [`dsm_region_bytes`], then calls [`leader_setup`] on the proc that initializes the
//! rings and [`worker_setup`] on each producer proc. Both hand back an [`MppMesh`] the embedder
//! installs on its DataFusion session; `worker_setup` also returns the outbound senders and the
//! plan bytes copied out of the region. [`run_worker_fragment`] is the producer push loop.
//!
//! The two platform primitives the embedder supplies are the [`Wakeup`] (how to wake a blocked
//! consumer) and the [`Interrupt`] (how to check for cancellation); everything here is otherwise
//! pure Rust over the shared buffer.

use std::ffi::c_void;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use datafusion::common::{DataFusionError, HashMap, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::StreamExt;
use tokio_util::sync::CancellationToken;

use std::path::Path;
use std::time::Duration;

use super::dsm::{
    compute_dsm_layout, leader_init, peer_proc_for_index, read_n_procs, read_region_total,
    worker_attach,
};
use super::ipc::IpcMeshNotifier;
use super::mesh::{DsmInboxReceiver, DsmInboxSender};
use super::mpsc_ring::{DsmMpscSender, NO_RECEIVER_TOKEN, Wakeup};
use super::notifier::{RingReceiverNotifier, RingSenderNotifier};
use super::runtime::MppMesh;
use super::transport::{
    BatchChannelSender, DrainHandle, ExecuteTaskRx, IncomingExecuteTaskRequest, Interrupt,
    MppDataStreamKey, MppFrameHeader, MppReceiver, MppSender,
};
use crate::proto as pb;
use crate::work_unit_feed::RemoteWorkUnitFeedRegistry;
use crate::{DistributedTaskContext, PartitionSink, collect_plan_metrics_protos};

/// Total bytes the shared region needs for `n_procs` inboxes plus `plan_len` plan bytes, with
/// `queue_bytes` per inbox. The embedder reserves exactly this much before [`leader_setup`].
pub fn dsm_region_bytes(n_procs: u32, queue_bytes: usize, plan_len: usize) -> Result<usize> {
    compute_dsm_layout(n_procs, queue_bytes, plan_len)
        .map(|l| l.region_total)
        .map_err(|e| DataFusionError::Internal(format!("mpp: dsm_region_bytes: {e}")))
}

/// Read `region_total` out of the header a leader wrote, so a worker that just mapped the region
/// can size its [`worker_setup`] call without knowing the header layout.
///
/// A caller that derives the size from the header forfeits [`worker_setup`]'s size validation
/// (it would compare the header against itself). Pass the mapped size from the embedder's own
/// bookkeeping where it is available.
///
/// # Safety
/// - `base` must point at the start of a region a leader initialized via [`leader_setup`].
/// - `base` must be at least 8-byte aligned (the header holds `u64` fields).
pub unsafe fn region_total(base: *const c_void) -> usize {
    unsafe { read_region_total(base) as usize }
}

/// Read `n_procs` out of the header a leader wrote, so a worker that just mapped the region
/// can determine the total process count to initialize its signaling notifier without
/// needing out-of-band sidechannels.
///
/// # Safety
/// - `base` must point at the start of a region a leader initialized via [`leader_setup`].
/// - `base` must be at least 8-byte aligned (the header holds `u64` fields).
pub unsafe fn dsm_n_procs(base: *const c_void) -> u32 {
    unsafe { read_n_procs(base) }
}

/// Wrap each peer-indexed `DsmMpscSender` into an outbound `MppSender` keyed by destination proc.
/// The dispatcher `clone_with_header`s these per output partition before sending, so the
/// placeholder header is never observed on the wire. Slot `this_proc` stays `None` because
/// local communication routes in-process via `LocalWorkerContext`.
///
/// Returns `(data, cancel)`. `data` is the producer's output senders. `cancel` is a control-plane
/// sibling onto each peer inbox, used by [`MppMesh::cancel_stream`]: a consumer reaches its producer
/// without counting as one of that producer's data senders, so a held `Cancel` sender never masks
/// the producer-gone `detached` signal.
#[derive(Clone, Copy)]
struct SendSyncRing(std::ptr::NonNull<super::mpsc_ring::DsmMpscRingHeader>);
unsafe impl Send for SendSyncRing {}
unsafe impl Sync for SendSyncRing {}

fn build_outbound_senders(
    this_proc: u32,
    total_procs: u32,
    peer_senders: Vec<DsmMpscSender>,
    ipc_notifier: &Arc<IpcMeshNotifier>,
) -> (Vec<Option<MppSender>>, Vec<Option<MppSender>>) {
    let mut senders: Vec<Option<MppSender>> = (0..total_procs).map(|_| None).collect();
    let mut cancel: Vec<Option<MppSender>> = (0..total_procs).map(|_| None).collect();
    let mut rings_map = HashMap::new();
    for (peer_idx, dsm_send) in peer_senders.iter().enumerate() {
        let target_proc = peer_proc_for_index(this_proc, peer_idx as u32);
        rings_map.insert(
            target_proc,
            (SendSyncRing(dsm_send.ring_header()), dsm_send.alive()),
        );
    }
    let rings_map_arc = Arc::new(rings_map);
    ipc_notifier.set_detachment_checker(Arc::new(move |proc_id| {
        if let Some((ring, alive)) = rings_map_arc.get(&proc_id) {
            if !alive.load(Ordering::Acquire) {
                return true;
            }
            let header = unsafe { ring.0.as_ref() };
            header.detached.load(Ordering::Acquire)
        } else {
            false
        }
    }));

    for (peer_idx, dsm_send) in peer_senders.into_iter().enumerate() {
        let target_proc = peer_proc_for_index(this_proc, peer_idx as u32);
        // Ensure peer index does not map onto this proc.
        debug_assert!(
            target_proc != this_proc,
            "peer index {peer_idx} mapped to the self proc {this_proc}"
        );
        debug_assert!(
            target_proc < total_procs,
            "peer index {peer_idx} mapped to proc {target_proc} >= total {total_procs}"
        );
        let ring = dsm_send.ring_header();
        let alive = dsm_send.alive();
        let tx_notif = Arc::new(RingSenderNotifier::new(
            Arc::clone(ipc_notifier),
            target_proc,
            ring,
            alive,
        ));
        let control: Arc<dyn BatchChannelSender> = Arc::new(DsmInboxSender::new(
            dsm_send.to_control(),
            Arc::clone(&tx_notif),
        ));
        cancel[target_proc as usize] = Some(MppSender::with_header(
            control,
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), this_proc),
        ));
        let shared: Arc<dyn BatchChannelSender> = Arc::new(DsmInboxSender::new(dsm_send, tx_notif));
        senders[target_proc as usize] = Some(MppSender::with_header(
            shared,
            // Stamp `sender_proc = this_proc` so a stray frame that escapes the dispatcher's
            // `clone_with_header` overwrite still identifies its origin on the drain side.
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), this_proc),
        ));
    }
    (senders, cancel)
}

/// What [`leader_setup`] hands back to the embedder: the active leader session on the mesh.
///
/// Dropping this handle drops `_outbound_senders`, notifying peer worker inboxes that the leader
/// has detached. Fields `mesh` and `plan_bytes` are accessible, while `_outbound_senders` is private
/// to prevent premature destructuring.
pub struct LeaderSession {
    /// The leader's mesh, installed on its DataFusion session.
    pub mesh: Arc<MppMesh>,
    /// Outbound senders keyed by destination proc index, for the control plane: work-unit
    /// frames flow leader -> worker through them. Private to enforce session scope retention.
    _outbound_senders: Vec<Option<MppSender>>,
}

impl LeaderSession {
    /// Send in-band cancel frames to all worker inboxes to stop data production.
    pub fn cancel_workers(&self) {
        let cancel_key = MppDataStreamKey::new(u32::MAX, u32::MAX, u32::MAX);
        for sender in self._outbound_senders.iter().flatten() {
            sender.try_send_cancel(cancel_key);
        }
    }

    /// Mark the leader mesh as detached and notify all peer worker inboxes via in-band DSM cancel frame.
    pub fn mark_detached(&mut self) {
        self.cancel_workers();
        self._outbound_senders.clear();
        self.mesh.mark_detached();
    }
}

/// Initialize the shared region as the leader (`proc 0`) and return its session handle.
///
/// Writes the region header, copies `plan_bytes` in, initializes the `n_procs` inboxes, and
/// attaches the leader as receiver to its own inbox. `receiver_token` is registered so producers
/// resolve this proc's [`Wakeup`]; `interrupt` is consulted at the transport's block points.
///
/// # Safety
/// - `base` must point at an uninitialized region of at least `dsm_region_bytes(n_procs,
///   queue_bytes, plan_bytes.len())` bytes.
/// - `base` must be at least 8-byte (MAXALIGN) aligned; the ring headers hold atomics.
/// - The region must not be concurrently accessed until this returns.
#[allow(clippy::too_many_arguments)]
pub unsafe fn leader_setup(
    base: *mut c_void,
    n_procs: u32,
    queue_bytes: usize,
    plan_bytes: &[u8],
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
    attach_senders: bool,
    ipc_notifier: Arc<IpcMeshNotifier>,
) -> Result<LeaderSession> {
    if receiver_token == NO_RECEIVER_TOKEN {
        return Err(DataFusionError::Internal(
            "mpp: leader_setup: receiver_token is the NO_RECEIVER_TOKEN sentinel; wakeups \
             for this proc would be silently disabled"
                .into(),
        ));
    }
    let layout = compute_dsm_layout(n_procs, queue_bytes, plan_bytes.len())
        .map_err(|e| DataFusionError::Internal(format!("mpp: leader_setup compute layout: {e}")))?;
    let attach = unsafe {
        leader_init(
            base,
            &layout,
            plan_bytes,
            Arc::clone(&wakeup),
            attach_senders,
        )
    }
    .map_err(DataFusionError::Internal)?;

    let rx_ring = attach.inbound_receiver.ring_header();
    let rx_notif = Arc::new(RingReceiverNotifier::new(
        Arc::clone(&ipc_notifier),
        rx_ring,
        Arc::clone(&attach.alive),
    ));
    let inbox = DsmInboxReceiver::new(attach.inbound_receiver);
    inbox.set_receiver(receiver_token);
    let inbound = Arc::new(DrainHandle::cooperative(vec![MppReceiver::new(Box::new(
        inbox,
    ))]));
    let reactor_task = inbound.start_inbound_reactor(Some(Arc::clone(&rx_notif)));
    // The leader hosts no producer fragments, but its senders carry the control plane:
    // work-unit frames (and later dynamic filters) flow leader -> worker through them. Empty
    // when the embedder did not opt in: a ring latches `detached` once its sender count hits
    // zero, so senders that might drop before every worker attached must never exist.
    let (outbound_senders, cancel_senders) =
        build_outbound_senders(0, n_procs, attach.outbound_senders, &ipc_notifier);
    let mut mesh = MppMesh::new(0, n_procs, inbound, interrupt, attach.alive);
    mesh.set_rx_notifier(Some(rx_notif));
    mesh.set_reactor(reactor_task);
    mesh.set_cancel_senders(Arc::new(std::sync::Mutex::new(cancel_senders)));
    Ok(LeaderSession {
        mesh: Arc::new(mesh),
        _outbound_senders: outbound_senders,
    })
}

/// Forwarder for [`leader_setup`].
///
/// # Safety
/// - Same requirements as [`leader_setup`].
#[allow(clippy::too_many_arguments)]
pub unsafe fn leader_setup_with_notifier(
    base: *mut c_void,
    n_procs: u32,
    queue_bytes: usize,
    plan_bytes: &[u8],
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
    attach_senders: bool,
    ipc_notifier: Arc<IpcMeshNotifier>,
) -> Result<LeaderSession> {
    unsafe {
        leader_setup(
            base,
            n_procs,
            queue_bytes,
            plan_bytes,
            wakeup,
            receiver_token,
            interrupt,
            attach_senders,
            ipc_notifier,
        )
    }
}

/// Initialize DSM region and IPC signaling mesh for the leader in a single async step.
#[allow(clippy::too_many_arguments)]
pub async fn leader_setup_ipc(
    base: *mut c_void,
    n_procs: u32,
    queue_bytes: usize,
    plan_bytes: &[u8],
    socket_dir: &Path,
    query_id: &str,
    wakeup: Arc<dyn Wakeup>,
    interrupt: Arc<dyn Interrupt>,
) -> Result<LeaderSession> {
    let (notifier, listener) = IpcMeshNotifier::bind(socket_dir, query_id, 0, n_procs).await?;
    notifier.start_listener_loop(listener);
    let session = unsafe {
        leader_setup(
            base,
            n_procs,
            queue_bytes,
            plan_bytes,
            wakeup,
            0,
            interrupt,
            true,
            notifier.clone(),
        )?
    };
    notifier
        .connect_peers(socket_dir, query_id, Duration::from_secs(10))
        .await?;
    Ok(session)
}

/// Build one task's work-unit feed channels, install the receiving ends on `cfg` (where the
/// deserialized plan's remote feed leaves look them up), and register the sending ends on
/// `mesh`'s drain so inbound `WorkUnit` frames fill them. `feeds` lists the task's declared
/// feeds as `(feed id, partitions)`, the same pairs the plan's `WorkUnitFeedDeclaration`s carry.
pub fn install_work_unit_channels(
    cfg: &mut datafusion::prelude::SessionConfig,
    mesh: &MppMesh,
    stage_id: u32,
    task_number: u32,
    feeds: &[(uuid::Uuid, usize)],
) {
    let mut channels = RemoteWorkUnitFeedRegistry::default();
    for (id, partitions) in feeds {
        channels.add(*id, *partitions);
    }
    cfg.set_extension(Arc::new(channels.receivers));
    mesh.register_work_unit_senders(stage_id, task_number, channels.senders);
}

/// Build the [`TaskMetrics`] payload for one executed fragment, for embedders that run
/// fragments outside the worker task registry (pg parallel workers). Pair it with
/// [`super::transport::MppSender::send_task_metrics_best_effort`] after the fragment's streams
/// complete; the leader-side rewrite consumes the same pre-order the in-registry path produces.
/// The task-level stamps (plan added/executed/finished) carry no data on this path, but the set
/// must be present: `decode_task_metrics` rejects a missing field, and one failed decode starves
/// the whole store.
///
/// [`TaskMetrics`]: crate::proto::TaskMetrics
pub fn collect_task_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    task_index: usize,
    task_count: usize,
) -> pb::TaskMetrics {
    pb::TaskMetrics {
        pre_order_plan_metrics: collect_plan_metrics_protos(
            plan,
            DistributedTaskContext {
                task_index,
                task_count,
            },
        ),
        task_metrics: Some(pb::MetricsSet::default()),
    }
}

/// What [`worker_setup`] hands back to the embedder: the active worker session on the mesh.
///
/// Dropping this handle drops `_outbound_senders`, notifying peer inboxes that this worker
/// has detached. Private `_outbound_senders` prevents premature destructuring.
pub struct WorkerSession {
    /// The worker's mesh, installed on its DataFusion session.
    pub mesh: Arc<MppMesh>,
    /// The plan bytes the leader wrote into the region, copied out for this worker.
    pub plan_bytes: Vec<u8>,
    /// Outbound senders keyed by destination proc index. Private to enforce session scope retention.
    _outbound_senders: Vec<Option<MppSender>>,
}

impl LeaderSession {
    pub fn outbound_senders(&self) -> &[Option<MppSender>] {
        &self._outbound_senders
    }
}

impl WorkerSession {
    pub fn outbound_senders(&self) -> &[Option<MppSender>] {
        &self._outbound_senders
    }
}

/// Attach to the leader-initialized region as worker `proc_idx` (`>= 1`).
///
/// # Safety
/// - `base`/`region_total` must match the region the leader initialized via [`leader_setup`].
/// - `base` must be at least 8-byte (MAXALIGN) aligned; the ring headers hold atomics.
pub unsafe fn worker_setup(
    base: *mut c_void,
    region_total: usize,
    proc_idx: u32,
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
    ipc_notifier: Arc<IpcMeshNotifier>,
) -> Result<WorkerSession> {
    if receiver_token == NO_RECEIVER_TOKEN {
        return Err(DataFusionError::Internal(
            "mpp: worker_setup: receiver_token is the NO_RECEIVER_TOKEN sentinel; wakeups \
             for this proc would be silently disabled"
                .into(),
        ));
    }
    let (header, plan_bytes, attach) =
        unsafe { worker_attach(base, region_total as u64, proc_idx, Arc::clone(&wakeup)) }
            .map_err(DataFusionError::Internal)?;
    let total_procs = header.n_procs;

    let (outbound, cancel) = build_outbound_senders(
        proc_idx,
        total_procs,
        attach.outbound_senders,
        &ipc_notifier,
    );

    let rx_ring = attach.inbound_receiver.ring_header();
    let rx_notif = Arc::new(RingReceiverNotifier::new(
        Arc::clone(&ipc_notifier),
        rx_ring,
        Arc::clone(&attach.alive),
    ));
    let inbox = DsmInboxReceiver::new(attach.inbound_receiver);
    inbox.set_receiver(receiver_token);
    let inbound = Arc::new(DrainHandle::cooperative(vec![MppReceiver::new(Box::new(
        inbox,
    ))]));
    let reactor_task = inbound.start_inbound_reactor(Some(Arc::clone(&rx_notif)));
    let mut mesh = MppMesh::new(proc_idx, total_procs, inbound, interrupt, attach.alive);
    mesh.set_rx_notifier(Some(rx_notif));
    mesh.set_reactor(reactor_task);
    // A worker consumes shuffle inputs, so it can be the consumer that stops a stream early. Give
    // its mesh the control-plane cancel senders; they drop with the mesh at the end of the worker's
    // run, well before the DSM unmaps, so no explicit release is needed.
    mesh.set_cancel_senders(Arc::new(std::sync::Mutex::new(cancel)));
    Ok(WorkerSession {
        mesh: Arc::new(mesh),
        plan_bytes,
        _outbound_senders: outbound,
    })
}

/// Forwarder for [`worker_setup`].
///
/// # Safety
/// - Same requirements as [`worker_setup`].
pub unsafe fn worker_setup_with_notifier(
    base: *mut c_void,
    region_total: usize,
    proc_idx: u32,
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
    ipc_notifier: Arc<IpcMeshNotifier>,
) -> Result<WorkerSession> {
    unsafe {
        worker_setup(
            base,
            region_total,
            proc_idx,
            wakeup,
            receiver_token,
            interrupt,
            ipc_notifier,
        )
    }
}

/// Attach to DSM region and IPC signaling mesh for a worker in a single async step.
/// Automatically discovers `n_procs` and `region_total` from `base`.
pub async fn worker_setup_ipc(
    base: *mut c_void,
    proc_idx: u32,
    socket_dir: &Path,
    query_id: &str,
    wakeup: Arc<dyn Wakeup>,
    receiver_token: u64,
    interrupt: Arc<dyn Interrupt>,
) -> Result<WorkerSession> {
    let total_bytes = unsafe { region_total(base) };
    let n_procs = unsafe { dsm_n_procs(base) };
    let (notifier, listener) =
        IpcMeshNotifier::bind(socket_dir, query_id, proc_idx, n_procs).await?;
    notifier.start_listener_loop(listener);
    let session = unsafe {
        worker_setup(
            base,
            total_bytes,
            proc_idx,
            wakeup,
            receiver_token,
            interrupt,
            notifier.clone(),
        )?
    };
    notifier
        .connect_peers(socket_dir, query_id, Duration::from_secs(10))
        .await?;
    Ok(session)
}

/// Run a producer fragment plan to exhaustion, pushing every output batch into the matching
/// per-partition [`PartitionSink`]. The output partition count of `plan` must equal `sinks.len()`;
/// `sinks[partition]` is the send end the caller routed for that output partition.
///
/// Each partition's [`PartitionSink::finish`] sends a per-channel EOF when its stream ends,
/// regardless of how it ended: the shared queue is multiplexed across fragments, so dropping a sink
/// doesn't end the channel, only the EOF frame does.
pub async fn run_worker_fragment(
    plan: Arc<dyn ExecutionPlan>,
    sinks: Vec<Box<dyn PartitionSink>>,
    ctx: Arc<TaskContext>,
    partition_range: std::ops::Range<usize>,
) -> Result<()> {
    if partition_range.end.saturating_sub(partition_range.start) != sinks.len() {
        return Err(DataFusionError::Internal(format!(
            "run_worker_fragment: partition range {}..{} requires {} sinks but got {}",
            partition_range.start,
            partition_range.end,
            partition_range.end.saturating_sub(partition_range.start),
            sinks.len()
        )));
    }
    let mut futures = Vec::with_capacity(sinks.len());
    for (partition, mut sink) in partition_range.zip(sinks.into_iter()) {
        let plan = Arc::clone(&plan);
        let ctx = Arc::clone(&ctx);
        futures.push(async move {
            let stream_result: Result<()> = async {
                let mut stream = plan.execute(partition, ctx)?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if let Err(e) = sink.send(&batch).await {
                        let msg = e.to_string();
                        if sink.cancelled() || msg.contains("detached") || msg.contains("Detached")
                        {
                            break;
                        }
                        return Err(e);
                    }
                    // Consumer abandoned this stream. Stop pulling: dropping `stream` ends the
                    // upstream scan and cascades the cancel to its own producers.
                    if sink.cancelled() {
                        break;
                    }
                }
                Ok(())
            }
            .await;
            let is_cancelled = sink.cancelled();
            let eof_result = match sink.finish().await {
                Ok(()) => Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    if is_cancelled || msg.contains("detached") || msg.contains("Detached") {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            };
            // Surface the stream error first, then any EOF-send error, so neither disappears.
            stream_result.and(eof_result)
        });
    }
    // `join_all`, not `try_join_all`: fail-fast would cancel sibling partitions mid-await before
    // they reach `finish`, leaving the consumer's channel buffer stuck.
    let results = futures::future::join_all(futures).await;
    for r in results {
        r?;
    }
    Ok(())
}

/// Run the worker request loop for receiving [`ExecuteTaskFrame`] requests, validating partition
/// ranges, and delegating execution of each requested range to `spawn_range`.
///
/// # Lifecycle & Guarantees
/// - **On-Demand Range Execution:** Listens on the mesh for incoming [`ExecuteTaskFrame`] messages,
///   extracting the requested partition range (`start..end`) and headers.
/// - **Partition Validation & Single-Claim Accounting:** Ensures that `start <= end` and `end <= n_partitions`,
///   and verifies that no partition within `start..end` has been claimed by a previous frame. Returns an
///   [`DataFusionError::Execution`] error if bounds are exceeded or duplicate partition claims occur.
/// - **Cancellation & Early Termination Unwinding:** Selects on `token.cancelled()`. If the query is
///   cancelled or terminates early (e.g. satisfied by a `LIMIT` clause downstream), the request loop
///   breaks out immediately and drops active sub-futures.
/// - **Async Inbound Ring Draining:** The mesh's background inbound reactor automatically
///   demuxes incoming frames and handles reactive notifications while awaiting frame arrivals.
/// - **Completion:** Exits cleanly once all `n_partitions` have been requested (or the channel closes) AND all
///   spawned stream futures in `spawn_range` have completed to EOF.
///
/// * `mesh`: The MPP mesh used to coordinate routing, draining, and error handling.
/// * `stage_id`: The stage ID this task belongs to.
/// * `task_number`: The task index within the stage.
/// * `n_partitions`: Total number of output partitions expected across all request frames for this task.
/// * `token`: Cancellation token used to unwind the request loop on query cancellation or early teardown.
/// * `spawn_range`: Closure invoked for each valid, unclaimed partition range `start..end`. Must return a future executing the partition streams for that range.
pub async fn run_execute_task_loop<F, Fut>(
    mesh: &Arc<MppMesh>,
    stage_id: u32,
    task_number: u32,
    n_partitions: usize,
    token: CancellationToken,
    spawn_range: F,
) -> Result<()>
where
    F: FnMut(pb::ExecuteTaskRequest, http::HeaderMap, std::ops::Range<usize>) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let rx = mesh.take_execute_task_rx(stage_id, task_number)?;
    let (res, failed_sender_proc) =
        run_execute_task_loop_inner(rx, n_partitions, token, spawn_range).await;
    if let Err(e) = &res {
        let err_msg = e.to_string();
        if !err_msg.contains("detached") && !err_msg.contains("Detached") {
            if let Some(sender) = mesh.error_sender(0, stage_id, task_number) {
                let _ = sender.send_task_error_best_effort(&err_msg).await;
            }
            if let Some(target_proc) = failed_sender_proc
                && target_proc != 0
                && let Some(sender) = mesh.error_sender(target_proc, stage_id, task_number)
            {
                let _ = sender.send_task_error_best_effort(&err_msg).await;
            }
        }
    }
    res
}

async fn run_execute_task_loop_inner<F, Fut>(
    rx: ExecuteTaskRx,
    n_partitions: usize,
    token: CancellationToken,
    mut spawn_range: F,
) -> (Result<()>, Option<u32>)
where
    F: FnMut(pb::ExecuteTaskRequest, http::HeaderMap, std::ops::Range<usize>) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    if n_partitions == 0 {
        return (Ok(()), None);
    }

    let mut sub_futures = futures::stream::FuturesUnordered::new();
    let mut partitions_requested = 0;
    let mut rx_opt = Some(rx);
    let mut claimed = vec![false; n_partitions];

    loop {
        // Stop when frame receiving has finished (all partitions requested or channel closed)
        // and all active partition stream futures have completed.
        if rx_opt.is_none() && sub_futures.is_empty() {
            break;
        }

        tokio::select! {
            // Unwind immediately on early query cancellation or completion.
            _ = token.cancelled() => break,

            frame_res = async { rx_opt.as_mut().unwrap().recv().await }, if rx_opt.is_some() => {
                match frame_res {
                    Some(Ok(IncomingExecuteTaskRequest { sender_proc, frame })) => {
                        let (request, headers) = match frame.into_parts() {
                            Ok(parts) => parts,
                            Err(e) => return (Err(e), Some(sender_proc)),
                        };
                        let start = request.target_partition_start as usize;
                        let end = request.target_partition_end as usize;

                        if start > end || end > n_partitions {
                            return (Err(DataFusionError::Execution(format!(
                                "shm transport: invalid partition range {start}..{end} for total partitions {n_partitions}"
                            ))), Some(sender_proc));
                        }

                        for (i, claimed_slot) in claimed[start..end].iter_mut().enumerate() {
                            if *claimed_slot {
                                let q = start + i;
                                return (Err(DataFusionError::Execution(format!(
                                    "shm transport: requested partition range {start}..{end} includes already claimed partition {q}"
                                ))), Some(sender_proc));
                            }
                            *claimed_slot = true;
                        }

                        let range_fut = spawn_range(request, headers, start..end);
                        sub_futures.push(async move {
                            use futures::FutureExt;
                            let res = std::panic::AssertUnwindSafe(range_fut)
                                .catch_unwind()
                                .await;
                            match res {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(e)) => Err((sender_proc, e)),
                                Err(payload) => {
                                    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                        s.to_string()
                                    } else if let Some(s) = payload.downcast_ref::<String>() {
                                        s.clone()
                                    } else {
                                        "Box<dyn Any>".to_string()
                                    };
                                    Err((sender_proc, DataFusionError::Execution(msg)))
                                }
                            }
                        });

                        let len = end - start;
                        partitions_requested += len;
                        if partitions_requested >= n_partitions {
                            rx_opt = None;
                        }
                    }
                    Some(Err(e)) => {
                        let msg = e.to_string();
                        if msg.contains("detached") || msg.contains("Detached") {
                            rx_opt = None;
                        } else {
                            return (Err(e), None);
                        }
                    }
                    None => {
                        rx_opt = None;
                    }
                }
            }
            next_res = sub_futures.next(), if !sub_futures.is_empty() => {
                match next_res {
                    Some(Ok(())) => {}
                    Some(Err((sender_proc, e))) => {
                        let msg = e.to_string();
                        if msg.contains("detached") || msg.contains("Detached") {
                            // Detached sink or consumer is clean early termination
                        } else {
                            return (Err(e), Some(sender_proc));
                        }
                    }
                    None => unreachable!(),
                }
            }
        }
    }
    (Ok(()), None)
}

#[cfg(test)]
mod tests {
    use super::super::transport::ExecuteTaskFrame;
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn run_execute_task_loop_multi_frame_sub_ranges() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let token = CancellationToken::new();
        let total_partitions = 4;
        let ranges_processed = Arc::new(AtomicUsize::new(0));
        let total_partition_len_processed = Arc::new(AtomicUsize::new(0));

        // Frame 1: sub-range 0..2
        let frame1 = ExecuteTaskFrame::from_parts(
            pb::ExecuteTaskRequest {
                task_key: Some(pb::TaskKey {
                    query_id: vec![],
                    stage_id: 1,
                    task_number: 0,
                }),
                target_partition_start: 0,
                target_partition_end: 2,
                producer_head: None,
            },
            &http::HeaderMap::new(),
        )
        .unwrap();

        // Frame 2: sub-range 2..4
        let frame2 = ExecuteTaskFrame::from_parts(
            pb::ExecuteTaskRequest {
                task_key: Some(pb::TaskKey {
                    query_id: vec![],
                    stage_id: 1,
                    task_number: 0,
                }),
                target_partition_start: 2,
                target_partition_end: 4,
                producer_head: None,
            },
            &http::HeaderMap::new(),
        )
        .unwrap();

        tx.send(Ok(IncomingExecuteTaskRequest {
            sender_proc: 0,
            frame: frame1,
        }))
        .unwrap();
        tx.send(Ok(IncomingExecuteTaskRequest {
            sender_proc: 0,
            frame: frame2,
        }))
        .unwrap();

        let ranges_counter = Arc::clone(&ranges_processed);
        let len_counter = Arc::clone(&total_partition_len_processed);

        let (res, _) = run_execute_task_loop_inner(
            rx,
            total_partitions,
            token,
            move |_request, _headers, range| {
                let r_counter = Arc::clone(&ranges_counter);
                let l_counter = Arc::clone(&len_counter);
                let len = range.len();
                async move {
                    r_counter.fetch_add(1, Ordering::SeqCst);
                    l_counter.fetch_add(len, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

        assert!(res.is_ok());
        assert_eq!(ranges_processed.load(Ordering::SeqCst), 2);
        assert_eq!(total_partition_len_processed.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn run_execute_task_loop_cancellation_returns_ok() {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let token = CancellationToken::new();
        token.cancel(); // Pre-cancelled token to simulate early query termination / LIMIT.

        let (res, _) =
            run_execute_task_loop_inner(rx, 4, token, |_request, _headers, _range| async {
                Ok(())
            })
            .await;

        assert!(
            res.is_ok(),
            "cancellation token unwinding must return Ok(()) to avoid false TaskError dispatch"
        );
    }
}
