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

//! Runtime glue between the leader's DataFusion execution and the DSM MPSC mesh.
//!
//! [`MppMesh`] is the runtime handle the leader builds at DSM-init time. It holds the
//! single [`super::transport::DrainHandle`] (the
//! `inbound_receiver`) that consolidates this proc's DSM inbox and self-loop, and gets
//! installed on the leader's `SessionConfig` extensions before plan execution.
//!
//! [`ShmChannelResolver`] implements [`ChannelResolver`], consulted by
//! `NetworkShuffleExec` / `NetworkCoalesceExec` / `NetworkBroadcastExec` at execute time. It hands
//! out a [`ShmWorkerChannel`] whose `execute_task(task_key, partition_range)` yields one stream per
//! consumer partition from the shared `inbound_receiver`.
//!
//! [`InProcessWorkerResolver`] hands the planner `n_workers` placeholder URLs. The transport
//! routes by task index, not URL, so the URLs are never dialed; the resolver exists only because
//! the planner sizes stages from the URL count. It replaces the placeholder URL the fork used to
//! substitute internally under the old `in_process_mode` flag.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use futures::stream::{self, BoxStream, StreamExt};
use http::HeaderMap;
use url::Url;

use crate::common::serialize_uuid;
use crate::proto as pb;
use crate::work_unit_feed::RemoteWorkUnitFeedTxs;
use crate::{
    ChannelResolver, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, SetPlanRequest, WorkerChannel, WorkerResolver, WorkerToCoordinatorMsg,
};

use super::AliveFlag;
use super::transport::{
    CooperativeDrainSet, DrainHandle, DrainItem, Interrupt, MppDataStreamKey, MppFrameHeader,
    MppSender, SendBatchStats, SetPlanFrame,
};

/// A proc's outbound senders to each peer inbox, shared between the mesh (for `Cancel` frames) and
/// the embedder that owns their lifetime. Indexed by destination `proc_idx`; this proc's own slot
/// is `None`.
pub type PeerSenders = Arc<Mutex<Vec<Option<MppSender>>>>;

/// `task_idx → proc_idx` round-robin over the worker procs. The leader is `proc_idx = 0`
/// (consumer-only), workers are `1..n_procs` (each hosts producer fragments).
///
/// A stage's task count is set by the DF-D task estimator chain, not by the worker proc count.
/// Several tasks may share a worker; their data streams remain independent because the transport
/// key includes `task_id` in addition to the sender proc, stage, and output partition.
#[inline]
pub fn proc_for_task(n_workers: u32, task_idx: u32) -> u32 {
    1 + (task_idx % n_workers.max(1))
}

/// Runtime handle the customscan populates at DSM-init time.
///
/// Each process owns one MPSC inbox in DSM that receives frames from every peer.
/// `inbound_receiver` consolidates that inbox plus the in-proc self-loop channel (for
/// producer-and-consumer-on-same-worker fragments) into a single `DrainHandle`. Frames
/// carry `(sender_proc, stage_id, task_id, partition)` in their header so the routing registry
/// inside the handle delivers each frame to the matching consumer.
///
/// [`MppFrameHeader`]: super::transport::MppFrameHeader
pub struct MppMesh {
    /// This process's `proc_idx` (= 0 for the leader, `ParallelWorkerNumber + 1` for workers).
    /// Frames addressed to this proc arrive on this proc's own inbox.
    pub this_proc: u32,
    /// Total proc count. Bounds the producer/consumer proc lookups in
    /// [`ShmWorkerChannel::execute_task`].
    pub n_procs: u32,
    /// Single cooperative inbound handle pulling every frame addressed to this proc. The
    /// DSM MPSC inbox and an in-proc self-loop receiver both feed into this handle. Demux
    /// to per-`(sender_proc, stage_id, task_id, partition)` channel buffers happens inside via
    /// `DrainHandle::register_channel`.
    pub(super) inbound_receiver: Arc<DrainHandle>,
    /// Cancellation hook, injected by the embedder, checked at the transport's block points (the
    /// cooperative send spin and the consumer pull loop).
    interrupt: Arc<dyn Interrupt>,
    /// This proc's outbound senders to each peer inbox, shared with the embedder that owns their
    /// lifetime (it clears the `Vec` before the DSM unmaps). Used by [`Self::cancel_stream`] to ship
    /// a `Cancel` frame to a producing proc when this proc abandons a stream. `None` until the
    /// embedder installs it.
    cancel_senders: Mutex<Option<PeerSenders>>,
    alive: AliveFlag,
}

impl MppMesh {
    /// Build a fresh mesh.
    pub fn new(
        this_proc: u32,
        n_procs: u32,
        inbound_receiver: Arc<DrainHandle>,
        interrupt: Arc<dyn Interrupt>,
        alive: AliveFlag,
    ) -> Self {
        Self {
            this_proc,
            n_procs,
            inbound_receiver,
            interrupt,
            cancel_senders: Mutex::new(None),
            alive,
        }
    }

    /// Mark this proc's DSM mesh detached so every ring handle's operations become no-ops.
    /// The embedder calls this from its dsm-detach callback, while the segment is still mapped,
    /// so handles dropped afterward (e.g. by a memory-context reset) never touch freed memory.
    pub fn mark_detached(&self) {
        self.alive.store(false, Ordering::Release);
        *self.cancel_senders.lock().unwrap() = None;
    }

    /// The raw liveness flag shared with every ring handle, for embedders that register a C
    /// dsm-detach callback against it directly.
    pub fn detached_flag(&self) -> AliveFlag {
        Arc::clone(&self.alive)
    }

    /// Share this proc's outbound senders so [`Self::cancel_stream`] can reach the producers. The
    /// embedder keeps owning their lifetime: it passes a clone of the same `Arc` it releases before
    /// the DSM unmaps, so the mesh never extends the senders past teardown.
    pub fn set_cancel_senders(&self, senders: PeerSenders) {
        *self.cancel_senders.lock().unwrap() = Some(senders);
    }

    /// Route a leader-built `SetPlan` frame to the worker proc that owns `task_number`, over the
    /// leader's outbound senders (installed via [`Self::set_cancel_senders`]). This is the unified
    /// dispatch path: the coordinator ships each stage's plan through here, so the embedder no
    /// longer routes it out of band. Errors if the senders aren't installed or the destination
    /// slot is empty; the caller treats a failure as best-effort (the worker starves on its
    /// `SetPlan` wait, which the dispatch deadlock detector surfaces).
    pub async fn send_set_plan(
        self: &Arc<Self>,
        stage_id: u32,
        task_number: u32,
        frame: SetPlanFrame,
    ) -> Result<()> {
        let dest_proc = proc_for_task(self.n_workers(), task_number);
        let sender = {
            let guard = self.cancel_senders.lock().unwrap();
            let Some(senders) = guard.as_ref() else {
                return internal_err!(
                    "shm mesh: control senders not installed; cannot route SetPlan"
                );
            };
            let senders = senders.lock().unwrap();
            let Some(Some(base)) = senders.get(dest_proc as usize) else {
                return internal_err!("shm mesh: no control sender for proc {dest_proc}");
            };
            base.clone_with_header(MppFrameHeader::set_plan(stage_id, task_number, 0))
                .with_cooperative_drain(Arc::clone(self) as Arc<dyn CooperativeDrainSet>)
        };
        let mut stats = SendBatchStats::default();
        sender.send_set_plan_traced(&frame, &mut stats).await
    }

    /// Sends an `ExecuteTask` control frame to `dest_proc` to request partition stream execution
    /// for `(stage_id, task_number)`.
    pub async fn send_execute_task(
        self: &Arc<Self>,
        stage_id: u32,
        task_number: u32,
        frame: crate::shm::transport::ExecuteTaskFrame,
    ) -> Result<()> {
        let dest_proc = proc_for_task(self.n_workers(), task_number);
        let sender = {
            let guard = self.cancel_senders.lock().unwrap();
            let Some(senders) = guard.as_ref() else {
                return internal_err!(
                    "shm mesh: control senders not installed; cannot route ExecuteTask"
                );
            };
            let senders = senders.lock().unwrap();
            let Some(Some(base)) = senders.get(dest_proc as usize) else {
                return internal_err!("shm mesh: no control sender for proc {dest_proc}");
            };
            base.clone_with_header(crate::shm::transport::MppFrameHeader::execute_task(
                stage_id,
                task_number,
                self.this_proc,
            ))
            .with_cooperative_drain(Arc::clone(self) as Arc<dyn CooperativeDrainSet>)
        };
        let mut stats = crate::shm::transport::SendBatchStats::default();
        sender.send_execute_task_traced(&frame, &mut stats).await
    }

    /// Obtains the channel receiver for incoming `ExecuteTask` requests targeting `(stage_id, task_number)`.
    pub(crate) fn take_execute_task_rx(
        self: &Arc<Self>,
        stage_id: u32,
        task_number: u32,
    ) -> Result<crate::shm::transport::ExecuteTaskRx> {
        self.inbound_receiver
            .take_execute_task_rx(stage_id, task_number)
    }

    /// Tell the producer on `producer_proc` to stop the `(stage_id, task_id, partition)` stream: this proc's
    /// consumer of it stopped reading before EOF. Ships one `Cancel` frame, leaving the rings
    /// healthy for metrics and every other stream. A no-op when no senders are installed (the
    /// embedder hasn't wired this proc, or teardown cleared them).
    ///
    /// Stream-level so any consumer can cancel its own input: every `(producer_proc, stage, task,
    /// partition)` channel has a single consumer, so one stream's drop never cuts off a sibling's.
    pub fn cancel_stream(&self, producer_proc: u32, stream: MppDataStreamKey) {
        let guard = self.cancel_senders.lock().unwrap();
        let Some(senders) = guard.as_ref() else {
            return;
        };
        let senders = senders.lock().unwrap();
        if let Some(Some(sender)) = senders.get(producer_proc as usize) {
            sender.try_send_cancel(stream);
        }
    }

    /// Obtains an outbound sender pre-configured to route a `TaskError` frame back to `dest_proc`
    /// (the process that requested task execution for `(stage_id, task_number)`).
    ///
    /// Returning `TaskError` to `dest_proc` matches standard RPC response routing: the node
    /// that requested execution is directly notified of task failure or fragment panic so its
    /// drain handle can fail dependent stream waiters.
    pub(crate) fn error_sender(
        &self,
        dest_proc: u32,
        stage_id: u32,
        task_number: u32,
    ) -> Option<crate::shm::transport::MppSender> {
        let guard = self.cancel_senders.lock().unwrap();
        let senders = guard.as_ref()?;
        let senders = senders.lock().unwrap();
        let base = senders.get(dest_proc as usize)?.as_ref()?;
        Some(
            base.clone_with_header(crate::shm::transport::MppFrameHeader::task_error(
                stage_id,
                task_number,
                self.this_proc,
            )),
        )
    }

    /// The single cooperative inbound handle that pulls frames from every peer (and the
    /// self-loop) into per-`(sender_proc, stage_id, task_id, partition)` channel buffers.
    pub fn inbound_receiver(&self) -> &Arc<DrainHandle> {
        &self.inbound_receiver
    }

    /// Install the senders of one task's work-unit feed channels on this proc's drain, so
    /// inbound `WorkUnit` frames for `(stage_id, task_number)` flow into them. Units that
    /// arrived first are flushed; a `FeedEof` that already came through closes the channels
    /// immediately. The drain only fills channels: something on this proc must keep draining
    /// (a consumer pull loop, a producer send spin, or an explicit
    /// [`CooperativeDrainSet::try_drain_pass`] pump) or a fragment blocked on its feed starves.
    pub fn register_work_unit_senders(
        &self,
        stage_id: u32,
        task_number: u32,
        senders: RemoteWorkUnitFeedTxs,
    ) {
        self.inbound_receiver
            .register_work_unit_senders(stage_id, task_number, senders);
    }

    /// Take the plan delivered for `(stage_id, task_number)` as a `SetPlan` frame on this proc's
    /// inbox, waiting for it if it has not arrived yet. Something on this proc must keep
    /// draining (a pump, a consumer pull loop, or a cooperative send spin) or the wait starves.
    pub async fn take_set_plan(
        &self,
        stage_id: u32,
        task_number: u32,
    ) -> Result<super::transport::SetPlanFrame> {
        self.inbound_receiver
            .take_set_plan(stage_id, task_number)
            .await
    }

    /// Take the stream of `TaskMetrics` frames arriving on this proc's inbox:
    /// `(stage_id, task_number, metrics)` per producer task that reported in. Meant for the
    /// leader; the first caller gets it, later calls get `None`.
    pub fn take_task_metrics_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<(u32, u32, pb::TaskMetrics)>> {
        self.inbound_receiver.take_task_metrics_receiver()
    }

    /// Number of worker procs (= `n_procs - 1`, since the leader is proc 0). Used as the
    /// modulus in [`proc_for_task`].
    ///
    /// The embedder must guarantee `n_procs >= 2` (one consumer-only leader plus at least one
    /// producer worker) before constructing an `MppMesh`; the subtraction is otherwise unsound.
    /// `compute_dsm_layout` enforces the same bound. Asserted in debug builds so a future misuse
    /// fails loudly.
    pub fn n_workers(&self) -> u32 {
        debug_assert!(
            self.n_procs >= 2,
            "MppMesh::n_workers() called with n_procs={} (< 2); the embedder must gate on \
             n_procs >= 2",
            self.n_procs
        );
        self.n_procs - 1
    }

    /// Pull from the single inbound handle. Called from
    /// [`super::transport::MppSender`]'s cooperative-send spin so a
    /// producer stalled on a full outbound can pull inbound peer data inline. That's what
    /// prevents the symmetric-send deadlock when every peer is simultaneously stalled waiting
    /// for space.
    pub(super) fn drain_all_inbound(&self) -> Result<(), DataFusionError> {
        self.inbound_receiver.try_drain_pass()
    }
}

impl CooperativeDrainSet for MppMesh {
    fn try_drain_pass(&self) -> Result<(), DataFusionError> {
        self.drain_all_inbound()
    }

    fn check_interrupt(&self) -> Result<(), DataFusionError> {
        self.interrupt.check()
    }

    fn stream_cancelled(&self, stream: MppDataStreamKey) -> bool {
        self.inbound_receiver.stream_cancelled(stream)
    }
}

/// Hands out [`ShmWorkerChannel`]s over the leader's [`MppMesh`]. The shm mesh has no URLs to dial,
/// so every `get_worker_client_for_url` resolves to the same mesh; the channel routes by the
/// `task_key` it is handed at `execute_task`, not by URL.
pub struct ShmChannelResolver {
    mesh: Arc<MppMesh>,
}

impl ShmChannelResolver {
    pub fn new(mesh: Arc<MppMesh>) -> Self {
        Self { mesh }
    }
}

#[async_trait]
impl ChannelResolver for ShmChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        _url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
        Ok(Box::new(ShmWorkerChannel {
            mesh: Arc::clone(&self.mesh),
        }))
    }
}

/// A [`WorkerChannel`] over the mesh. `execute_task((stage, task), partition_range)` translates the
/// DF-D `(stage, task)` addressing into the proc-pair grid: `proc_for_task(n_workers, task_number)`
/// selects which `sender_proc` hosts the producer-side task, and each returned stream pulls that
/// task's `(sender_proc, stage_id, task_id, partition)` slice from the inbound drain.
struct ShmWorkerChannel {
    mesh: Arc<MppMesh>,
}

#[async_trait]
impl WorkerChannel for ShmWorkerChannel {
    /// Route each `SetPlanRequest` the coordinator ships to the worker proc that owns its task, as a
    /// `SetPlan` frame the worker picks up with `take_set_plan`. The coordinator's keep-alive tail
    /// emits nothing after the request, so the loop then blocks until the stream closes, draining it
    /// to exhaustion. Completes with an empty output stream: task metrics travel back over the mesh,
    /// not here.
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        set_plan_request: SetPlanRequest,
        mut c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
        _metrics: ExecutionPlanMetricsSet,
        task_ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        let mesh = Arc::clone(&self.mesh);
        let stage_id = set_plan_request.task_key.stage_id as u32;
        let task_number = set_plan_request.task_key.task_number as u32;
        let plan_proto = set_plan_request.plan.encode(task_ctx)?;
        let set_plan = pb::SetPlanRequest {
            task_key: Some(pb::TaskKey {
                query_id: serialize_uuid(&set_plan_request.task_key.query_id),
                stage_id: set_plan_request.task_key.stage_id as u64,
                task_number: set_plan_request.task_key.task_number as u64,
            }),
            task_count: set_plan_request.task_count as u64,
            plan_proto,
            work_unit_feed_declarations: set_plan_request
                .work_unit_feed_declarations
                .into_iter()
                .map(|d| pb::set_plan_request::WorkUnitFeedDeclaration {
                    id: serialize_uuid(&d.id),
                    partitions: d.partitions as u64,
                })
                .collect(),
            target_worker_url: set_plan_request.target_worker_url.to_string(),
            query_start_time_ns: set_plan_request.query_start_time_ns as u64,
        };
        let frame = match SetPlanFrame::from_parts(set_plan, &headers) {
            Ok(frame) => frame,
            Err(e) => {
                log::warn!("shm coordinator_channel: SetPlan frame build failed: {e}");
                return Ok(stream::empty().boxed());
            }
        };
        if let Err(e) = mesh.send_set_plan(stage_id, task_number, frame).await {
            log::warn!("shm coordinator_channel: SetPlan route failed: {e}");
        }

        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move { while let Some(_msg) = c2w_stream.next().await {} });
        Ok(stream::empty().boxed())
    }

    async fn execute_task(
        &mut self,
        headers: HeaderMap,
        request: ExecuteTaskRequest,
        metrics: ExecutionPlanMetricsSet,
        task_ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        MetricBuilder::new(&metrics)
            .global_counter("mesh_connections_used")
            .add(1);
        let stage_id = u32::try_from(request.task_key.stage_id).map_err(|_| {
            DataFusionError::Internal(format!(
                "ShmWorkerChannel: stage_id={} > u32::MAX",
                request.task_key.stage_id
            ))
        })?;
        let task_number = u32::try_from(request.task_key.task_number).map_err(|_| {
            DataFusionError::Internal(format!(
                "ShmWorkerChannel: task_number={} > u32::MAX",
                request.task_key.task_number
            ))
        })?;
        let sender_proc = proc_for_task(self.mesh.n_workers(), task_number);
        // Several tasks can fold onto this sender proc. `task_number` remains part of the stream
        // identity, while this range check protects the mesh lookup itself.
        if sender_proc >= self.mesh.n_procs {
            return Err(DataFusionError::Internal(format!(
                "ShmWorkerChannel: sender_proc={sender_proc} >= n_procs={} \
                 (stage_id={stage_id}, task_number={task_number})",
                self.mesh.n_procs
            )));
        }

        let pb_request = crate::proto::ExecuteTaskRequest {
            task_key: Some(crate::proto::TaskKey {
                query_id: crate::common::serialize_uuid(&request.task_key.query_id),
                stage_id: request.task_key.stage_id as u64,
                task_number: request.task_key.task_number as u64,
            }),
            target_partition_start: request.target_partition_start as u64,
            target_partition_end: request.target_partition_end as u64,
            producer_head: Some(match request.producer_head {
                crate::ProducerHead::None => {
                    crate::proto::execute_task_request::ProducerHead::None(
                        crate::proto::NoneHead {},
                    )
                }
                crate::ProducerHead::BroadcastExec { output_partitions } => {
                    crate::proto::execute_task_request::ProducerHead::Broadcast(
                        crate::proto::BroadcastExecHead {
                            output_partitions: output_partitions as u64,
                        },
                    )
                }
                crate::ProducerHead::RepartitionExec { partitioning } => {
                    crate::proto::execute_task_request::ProducerHead::Repartition(
                        crate::proto::RepartitionExecHead {
                            partitioning: partitioning.encode(task_ctx)?,
                        },
                    )
                }
            }),
        };

        let frame = match crate::shm::transport::ExecuteTaskFrame::from_parts(pb_request, &headers)
        {
            Ok(f) => f,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "ExecuteTask frame build failed: {e}"
                )));
            }
        };
        self.mesh
            .send_execute_task(stage_id, task_number, frame)
            .await?;

        // First line to grep when a query hangs on a channel nothing registered.
        log::debug!(
            "shm transport execute_task: this_proc={} stage_id={stage_id} \
             task_number={task_number} -> sender_proc={sender_proc}",
            self.mesh.this_proc
        );
        let mut streams = Vec::with_capacity(
            request
                .target_partition_end
                .saturating_sub(request.target_partition_start),
        );
        for partition in request.target_partition_start..request.target_partition_end {
            let partition_u32 = u32::try_from(partition).map_err(|_| {
                DataFusionError::Internal(format!(
                    "ShmWorkerChannel: partition={partition} > u32::MAX"
                ))
            })?;
            streams.push(pull_partition_stream(
                Arc::clone(&self.mesh),
                sender_proc,
                MppDataStreamKey::new(stage_id, task_number, partition_u32),
            ));
        }
        Ok(streams)
    }

    async fn get_worker_info(
        &mut self,
        _request: GetWorkerInfoRequest,
    ) -> Result<GetWorkerInfoResponse> {
        Ok(GetWorkerInfoResponse {
            version: String::new(),
        })
    }
}

/// Build the cooperative pull-loop stream for one `(producer_proc, stage_id, task_id, partition)` channel.
///
/// The inbound drain runs inline on the consumer's thread. Each iteration checks for cancellation
/// (via the injected interrupt extension point), drains the receiver into the registry, pops one
/// batch to yield, then yields back to Tokio so sibling tasks (e.g. the leader's own producer
/// subplan) advance. The send side runs the same interrupt check inside `MppSender`'s retry spin.
fn pull_partition_stream(
    mesh: Arc<MppMesh>,
    producer_proc: u32,
    stream: MppDataStreamKey,
) -> BoxStream<'static, Result<RecordBatch>> {
    // One drain per process, shared across all sender_procs. The channel-buffer registry keys by
    // `(sender_proc, stage_id, task_id, partition)` so this consumer still sees only its named
    // sender's slice
    // even though the underlying inbox is shared with all peers.
    let drain = Arc::clone(mesh.inbound_receiver());
    log::debug!(
        "shm transport execute: register channel sender_proc={producer_proc} \
         stage_id={} task_id={} partition={}",
        stream.stage_id,
        stream.task_id,
        stream.partition,
    );
    let buffer = drain.register_data_channel(producer_proc, stream);
    let stream = async_stream::stream! {
        // Any consumer cancels its own input stream when it drops early. The mesh no-ops the send
        // until the embedder wires this proc's outbound senders.
        let mut cancel_guard = StreamCancelGuard {
            mesh: Arc::clone(&mesh),
            producer_proc,
            stream,
            armed: true,
        };
        loop {
            if let Err(e) = mesh.check_interrupt() {
                yield Err(e);
                return;
            }
            if let Err(e) = drain.try_drain_pass() {
                yield Err(e);
                return;
            }
            match buffer.try_pop() {
                Some(DrainItem::Batch(batch)) => yield Ok(batch),
                Some(DrainItem::Eof) => {
                    // Clean end: the producer already EOF'd, so there's nothing to cancel.
                    cancel_guard.armed = false;
                    break;
                }
                Some(DrainItem::Failed(msg)) => {
                    yield Err(DataFusionError::Execution(msg));
                    return;
                }
                None => tokio::task::yield_now().await,
            }
        }
    };
    Box::pin(stream)
}

/// Placeholder worker resolver for the in-process MPP transport.
///
/// The shm_mq transport routes by `target_task` (proc index), never by URL, so these URLs are
/// never dialed. The distributed planner still needs a resolver: it sizes stages and assigns
/// tasks from the URL count. `n_workers` placeholder URLs is exactly what the planner needs. This
/// replaces the placeholder URL the fork used to substitute internally under `in_process_mode`.
pub struct InProcessWorkerResolver {
    n_workers: usize,
}

impl InProcessWorkerResolver {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }
}

impl WorkerResolver for InProcessWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        (0..self.n_workers.max(1))
            .map(|i| {
                Url::parse(&format!("inprocess://worker/{i}")).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "InProcessWorkerResolver: invalid placeholder url: {e}"
                    ))
                })
            })
            .collect()
    }
}

/// Cancels one `(stage_id, task_id, partition)` stream if its consumer drops before EOF, telling the
/// producer on `producer_proc` to stop. A consumer that stops pulling early (a top-N `LIMIT`, an
/// inner merge join exhausting a side, etc.) would otherwise leave that producer spinning on the
/// full inbox until the statement timeout. Disarmed on a clean EOF: there the producer already
/// finished, so there's nothing to cancel.
struct StreamCancelGuard {
    mesh: Arc<MppMesh>,
    producer_proc: u32,
    stream: MppDataStreamKey,
    armed: bool,
}

impl Drop for StreamCancelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.mesh.cancel_stream(self.producer_proc, self.stream);
        }
    }
}
