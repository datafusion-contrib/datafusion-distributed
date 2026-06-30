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

//! Deferred: `mod self_hosted` is gated out (`#[cfg(any())]`) in `shm/mod.rs`. This file still
//! targets the removed `WorkerTransport`/`WorkerDispatch` dispatch umbrella, which the
//! `ChannelResolver`/`WorkerChannel` protocol has no analog for. Its no-gRPC-default role is now
//! served by `InProcessChannelResolver`, and the in-crate ring safety net by the `in_process` test,
//! so it stays unported until reimplemented as a `ChannelResolver` driving produce over
//! `coordinator_channel`. Kept for reference; it does not compile against this branch.
//!
//! The shared-memory transport hosting its own workers, so it can serve as a session default.
//!
//! Production embedders drive the mesh themselves: they allocate the region, launch the worker
//! processes, and deliver plans out of band, so [`ShmMqWorkerTransport`]'s dispatcher is a no-op.
//! That shape cannot be a default transport: a default gets nothing but the `WorkerTransport`
//! calls. [`SelfHostedShmTransport`] closes that gap by playing the embedder itself, in-process:
//! dispatch delivers each task's plan through the worker session machinery
//! ([`Worker::set_task_plan`]: codec round-trip, work-unit feed channels, metrics back-channel)
//! and runs the producer fragments as tasks pushing through a heap-backed DSM mesh; `open` reads
//! the rings from the leader side. Every cross-stage byte moves through the same rings, framing,
//! and cooperative drain a production embedder uses.
//!
//! Per query, the harness lives from the first dispatch to the cancellation token firing (the
//! head stream dropping). The mesh is sized and built lazily at the first `open`, once every
//! stage has been dispatched and the routing is known.

use std::alloc::Layout;
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use dashmap::DashMap;
use datafusion::common::instant::Instant;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, HashMap, Result, exec_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::mpsc_ring::Wakeup;
use super::runtime::{MppMesh, ShmMqWorkerTransport, proc_for_task};
use super::setup::{dsm_region_bytes, leader_setup, worker_setup};
use super::transport::{
    CooperativeDrainSet, Interrupt, MppFrameHeader, MppPartitionSink, MppSender, SendBatchStats,
    SetPlanFrame,
};
use crate::proto as pb;
use crate::{
    CoordinatorToWorkerMetrics, DistributedTaskContext, MetricsStore, NetworkBoundaryExt,
    NetworkCoalesceExec, PartitionSink, ProducerHead, RemoteStage, TreeNodeExt, Worker,
    WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerSessionBuilder, WorkerTransport,
    collect_task_work_unit_feeds, encode_task_plan, execute_local_task,
    get_config_extension_propagation_headers, get_distributed_cancellation_token,
    get_passthrough_headers, serialize_uuid, set_distributed_worker_transport, set_sent_time,
};

/// Per-inbox ring size. Frames fragment across slots, so this bounds a single batch at roughly
/// the whole ring; the suite's batches sit well under it while keeping the per-query footprint
/// (`n_procs` inboxes) modest.
const SELF_HOSTED_QUEUE_BYTES: usize = 1 << 22;

/// No-op wakeup: the cooperative consumers yield instead of parking, so a publish never has to
/// wake a blocked thread.
pub(super) struct NoopWakeup;
impl Wakeup for NoopWakeup {
    fn wake(&self, _token: u64) {}
}

/// Opaque, non-sentinel receiver token. The wakeup ignores the value; this just keeps the
/// producer from treating the consumer as unregistered.
pub(super) fn receiver_token(proc_idx: u32) -> u64 {
    proc_idx as u64 + 1
}

/// Owns the heap buffer standing in for a shared-memory segment. Every proc reads and writes the
/// same region through raw pointers; the lock-free rings make the concurrent access sound.
pub(super) struct HeapRegion {
    ptr: *mut u8,
    layout: Layout,
}

impl HeapRegion {
    pub(super) fn new(bytes: usize) -> Self {
        // 64-byte alignment so each per-inbox ring header lands on its own cache line; the
        // dsm layout aligns the offsets within the region, but only if the base is aligned too.
        let layout = Layout::from_size_align(bytes, 64).expect("dsm region layout");
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "dsm region alloc failed");
        Self { ptr, layout }
    }

    pub(super) fn base(&self) -> *mut c_void {
        self.ptr as *mut c_void
    }
}

impl Drop for HeapRegion {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

// The raw pointer is only dereferenced through the rings, which are designed for concurrent
// access from multiple procs.
unsafe impl Send for HeapRegion {}
unsafe impl Sync for HeapRegion {}

/// Interrupt extension point wired to the query's cancellation token, so producers blocked in a send spin
/// and consumers in the pull loop both unwind when the head stream drops.
struct CancellationInterrupt(CancellationToken);
impl Interrupt for CancellationInterrupt {
    fn check(&self) -> Result<(), DataFusionError> {
        if self.0.is_cancelled() {
            return Err(DataFusionError::Execution(
                "mpp: query cancelled".to_string(),
            ));
        }
        Ok(())
    }
}

/// [`WorkerTransport`] over the shared-memory mesh, hosting its own workers in-process.
///
/// All tasks share one [Worker] (one task registry, one session builder), like the in-memory
/// transport; what differs is the data plane: producer fragments run eagerly as background tasks
/// pushing through DSM rings, and reads pull from those rings instead of executing lazily.
///
/// With the `flight` feature off this is the default transport. Multi-process embedders keep
/// driving [`ShmMqWorkerTransport`] directly.
#[derive(Clone)]
pub struct SelfHostedShmTransport {
    worker: Worker,
    queries: Arc<DashMap<Uuid, Arc<QueryHarness>>>,
    queue_bytes: usize,
}

impl Default for SelfHostedShmTransport {
    fn default() -> Self {
        Self::new(Worker::default())
    }
}

impl SelfHostedShmTransport {
    /// Builds the transport around an existing [Worker], sharing its task registry, session
    /// builder, and runtime environment.
    pub fn new(worker: Worker) -> Self {
        Self {
            worker,
            queries: Arc::new(DashMap::new()),
            queue_bytes: SELF_HOSTED_QUEUE_BYTES,
        }
    }

    /// Overrides the per-inbox ring size. Small values force multi-slot fragmentation and the
    /// cooperative send spin on every query, which is how the ring mechanics get stress-tested;
    /// the default is generous enough that only large batches touch them.
    pub fn with_queue_bytes(mut self, queue_bytes: usize) -> Self {
        self.queue_bytes = queue_bytes;
        self
    }

    /// Builds the transport with a custom [WorkerSessionBuilder], the same customization hook a
    /// remote worker offers.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self::new(Worker::from_session_builder(session_builder))
    }

    /// The in-process [Worker] backing this transport.
    pub fn worker(&self) -> &Worker {
        &self.worker
    }
}

/// Recovers the producer head the planner already inserted at the top of a stage's shipped
/// fragment. The worker's `insert_producer_head` strips an existing head and re-adds one from the
/// [`ProducerHead`] it is given, so a `None` here would flatten a sliced stage to one partition;
/// echoing the existing head back keeps it intact.
fn producer_head_from_plan(plan: &Arc<dyn ExecutionPlan>) -> ProducerHead {
    use datafusion::physical_plan::repartition::RepartitionExec;
    if let Some(r) = plan.downcast_ref::<RepartitionExec>() {
        ProducerHead::RepartitionExec {
            partitioning: r.partitioning().clone(),
        }
    } else if let Some(b) = plan.downcast_ref::<crate::BroadcastExec>() {
        ProducerHead::BroadcastExec {
            output_partitions: b.properties().partitioning.partition_count(),
        }
    } else {
        ProducerHead::None
    }
}

impl WorkerTransport for SelfHostedShmTransport {
    fn open(
        &self,
        input_stage: &RemoteStage,
        target_partitions: std::ops::Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn WorkerConnection>> {
        let Some(harness) = self
            .queries
            .get(&input_stage.query_id)
            .map(|e| Arc::clone(e.value()))
        else {
            return exec_err!(
                "self-hosted shm transport: no harness for query {}; stage {} was never \
                 dispatched through this transport",
                input_stage.query_id,
                input_stage.num
            );
        };
        // The first read finalizes the harness: by now every stage has been dispatched (plan
        // preparation completes before the head executes), so the mesh can be sized and the
        // producer drivers released.
        harness.ensure_started()?;
        let leader_mesh = harness.leader_mesh()?;
        let inner = ShmMqWorkerTransport::new(leader_mesh).open(
            input_stage,
            target_partitions,
            target_task,
            producer_head,
            ctx,
            metrics,
        )?;
        Ok(Box::new(PinnedConnection { inner, harness }))
    }

    fn dispatcher(&self) -> Box<dyn WorkerDispatch> {
        Box::new(SelfHostedDispatcher {
            transport: self.clone(),
            metrics: OnceLock::new(),
        })
    }
}

/// Keeps the query harness (and through it the heap region the rings live in) alive for as long
/// as any stream is still reading from the mesh.
struct PinnedConnection {
    inner: Box<dyn WorkerConnection>,
    harness: Arc<QueryHarness>,
}

impl WorkerConnection for PinnedConnection {
    fn execute(
        &self,
        partition: usize,
    ) -> Result<BoxStream<'static, Result<datafusion::arrow::array::RecordBatch>>> {
        let stream = self.inner.execute(partition)?;
        let harness = Arc::clone(&self.harness);
        Ok(Box::pin(stream.map(move |item| {
            let _ = &harness; // <- the region must outlive the ring receivers.
            item
        })))
    }
}

/// Per-query plan-delivery state. As with the other transports, the plan-send metrics and the
/// query start timestamp live for the whole query.
struct SelfHostedDispatcher {
    transport: SelfHostedShmTransport,
    metrics: OnceLock<CoordinatorToWorkerMetrics>,
}

impl WorkerDispatch for SelfHostedDispatcher {
    fn dispatch(&self, request: WorkerDispatchRequest<'_>) -> Result<()> {
        let WorkerDispatchRequest {
            stage,
            routed_urls,
            task_ctx,
            metrics,
            metrics_store,
            join_set,
            ..
        } = request;
        let metrics = self
            .metrics
            .get_or_init(|| CoordinatorToWorkerMetrics::new(metrics))
            .clone();

        let token = get_distributed_cancellation_token(task_ctx);
        let harness = match self.transport.queries.entry(stage.query_id) {
            dashmap::Entry::Occupied(e) => Arc::clone(e.get()),
            dashmap::Entry::Vacant(e) => {
                let harness = Arc::new(QueryHarness::new(
                    stage.query_id,
                    token.clone(),
                    self.transport.queue_bytes,
                    metrics_store.cloned(),
                ));
                e.insert(Arc::clone(&harness));
                // The token fires when the head stream drops (normal completion included), which
                // is the query's end of life; drop the registry entry then. The region itself
                // stays alive through the Arcs the drivers and streams hold.
                let queries = Arc::clone(&self.transport.queries);
                let query_id = stage.query_id;
                let watched = token.clone();
                #[allow(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    watched.cancelled().await;
                    queries.remove(&query_id);
                });
                // One pump set per query: drains every proc's inbox so control frames flow even
                // when no consumer or producer is actively draining (a fragment blocked on its
                // feed, the metrics frames after the gather finished), and forwards the task
                // metrics into the store. Lives until every driver and feed pump reported done.
                join_set.spawn(run_pumps(Arc::clone(&harness), token.clone()));
                // Plans travel as `SetPlan` frames like every other control message, but the
                // rings only exist at finalize; this pump waits for them and ships the queue.
                harness.participants.fetch_add(1, Ordering::SeqCst);
                join_set.spawn(run_plan_delivery(Arc::clone(&harness), token.clone()));
                harness
            }
        };

        let mut encoded_tasks = Vec::with_capacity(routed_urls.len());
        for task_i in 0..routed_urls.len() {
            encoded_tasks.push(encode_task_plan(
                &stage.plan,
                task_i,
                stage.tasks,
                task_ctx.session_config(),
            )?);
        }
        harness.record_stage(
            stage.num,
            stage.tasks,
            encoded_tasks.iter().map(|e| e.partitions).collect(),
        );
        harness
            .state
            .lock()
            .unwrap()
            .coord_metrics
            .get_or_insert_with(|| metrics.clone());
        harness.scan_for_child_routing(&stage.plan, stage.tasks)?;

        // The planner already baked the producer head into the shipped fragment. Re-state it so
        // each worker's `insert_producer_head` re-creates the same head rather than flattening the
        // stage to one partition.
        let producer_head = producer_head_from_plan(&stage.plan).to_proto(task_ctx)?;

        let mut headers = get_config_extension_propagation_headers(task_ctx.session_config())?;
        headers.extend(get_passthrough_headers(task_ctx.session_config()));

        for (task_i, (url, encoded)) in routed_urls.iter().zip(encoded_tasks).enumerate() {
            let plan_size = encoded.plan_proto.len();

            let task_key = pb::TaskKey {
                query_id: serialize_uuid(&stage.query_id),
                stage_id: stage.num as u64,
                task_number: task_i as u64,
            };
            let set_plan = pb::SetPlanRequest {
                plan_proto: encoded.plan_proto,
                task_count: stage.tasks as u64,
                task_key: Some(task_key.clone()),
                work_unit_feed_declarations: encoded.feed_declarations,
                target_worker_url: url.to_string(),
                query_start_time_ns: metrics.instantiation_time,
            };
            // The plan reaches the driver as a `SetPlan` frame on its proc's inbox, the same
            // wire crossing a separate-process worker would see; the driver only holds its
            // address. The frames queue here because the rings are only built at finalize.
            harness
                .state
                .lock()
                .unwrap()
                .pending_set_plans
                .push(PendingSetPlan {
                    stage_num: stage.num as u32,
                    task_i,
                    frame: SetPlanFrame::from_parts(set_plan, &headers)?,
                    plan_size,
                });

            // Collected before spawning so the providers see the same eager `feed()` timing as
            // they do under the other transports.
            let feed_streams =
                collect_task_work_unit_feeds(&stage.plan, task_ctx, task_i, stage.tasks)?;
            let has_feeds = !feed_streams.is_empty();

            let driver = TaskDriver {
                harness: Arc::clone(&harness),
                worker: self.transport.worker.clone(),
                stage_num: stage.num as u32,
                task_i,
                n_partitions: encoded.partitions,
                has_feeds,
                task_key,
                token: token.clone(),
                producer_head: producer_head.clone(),
            };
            harness.participants.fetch_add(1, Ordering::SeqCst);
            join_set.spawn(driver.run());

            // The feeds travel as `WorkUnit` frames through the leader's outbound senders, so
            // the worker side reads them exactly as a separate process would.
            if has_feeds {
                harness.state.lock().unwrap().any_feeds = true;
                harness.participants.fetch_add(1, Ordering::SeqCst);
                join_set.spawn(run_leader_feed_pump(
                    Arc::clone(&harness),
                    stage.num as u32,
                    task_i,
                    feed_streams,
                    token.clone(),
                ));
            }
        }
        Ok(())
    }
}

/// How a producer stage's output reaches its consumers, as parent-stage task indexes. A stage no
/// parent boundary ever claims has no entry: it is consumed by the head on the leader (proc 0).
///
/// Built by simulating each consumer task's reads under its effective task contexts (a
/// `ChildrenIsolatorUnionExec` hands its children remapped contexts, so a boundary under one is
/// read with that inner context, not the stage-level one). `None` slots were never claimed by
/// any consumer (padding partitions); they route to the leader, where they sit buffered until
/// teardown.
enum RoutingSpec {
    /// Nested `NetworkCoalesceExec`: consumers read whole producer tasks, so the destination
    /// depends on the producer task only.
    PerTask(Vec<Option<u32>>),
    /// Nested shuffle/broadcast: consumers read partition slices, identical across producer
    /// tasks, so the destination depends on the output partition only.
    PerPartition(Vec<Option<u32>>),
}

struct StageRec {
    tasks: usize,
    /// Output partitions of each task's specialized plan. Task-isolated nodes make these differ
    /// from the unspecialized stage plan (and possibly from each other).
    task_partitions: Vec<usize>,
}

/// What a task driver needs to start producing: its proc's mesh and one routed sink per output
/// partition.
struct Launch {
    mesh: Arc<MppMesh>,
    sinks: Vec<Box<dyn PartitionSink>>,
    /// Send end for this task's `TaskMetrics` frame to the leader.
    metrics_sender: MppSender,
}

/// One task's plan, queued between `dispatch` and finalize, when the rings exist to carry it.
struct PendingSetPlan {
    stage_num: u32,
    task_i: usize,
    frame: SetPlanFrame,
    plan_size: usize,
}

struct HarnessState {
    stages: HashMap<u32, StageRec>,
    routing: HashMap<u32, RoutingSpec>,
    started: bool,
    /// Whether any dispatched task declared work-unit feeds; decides whether a feed pump runs.
    any_feeds: bool,
    /// Plans queued by `dispatch` until finalize builds the mesh; the delivery pump then ships
    /// each as a `SetPlan` frame to the proc hosting its task.
    pending_set_plans: Vec<PendingSetPlan>,
    /// Dispatch-side metrics (plan send latency / bytes), recorded by the delivery pump.
    coord_metrics: Option<CoordinatorToWorkerMetrics>,
    n_workers: u32,
    leader_mesh: Option<Arc<MppMesh>>,
    /// The leader's outbound senders, the control-plane path for `WorkUnit` frames.
    leader_senders: Vec<Option<MppSender>>,
    /// One mesh per worker proc, kept for the per-proc drain pumps.
    worker_meshes: Vec<Arc<MppMesh>>,
    launches: HashMap<(u32, usize), Launch>,
    /// Declared after the meshes so it would drop last either way; the harness Arcs held by
    /// drivers and pinned streams are what actually keep it alive long enough.
    region: Option<HeapRegion>,
}

struct QueryHarness {
    query_id: Uuid,
    token: CancellationToken,
    queue_bytes: usize,
    metrics_store: Option<Arc<MetricsStore>>,
    /// Drivers and feed pumps spawned for this query; `done` counts their exits. The pumps run
    /// until the two meet, which is the deterministic "no more frames are coming" signal.
    participants: AtomicUsize,
    done: AtomicUsize,
    state: Mutex<HarnessState>,
    ready_tx: tokio::sync::watch::Sender<bool>,
    ready_rx: tokio::sync::watch::Receiver<bool>,
}

impl QueryHarness {
    fn new(
        query_id: Uuid,
        token: CancellationToken,
        queue_bytes: usize,
        metrics_store: Option<Arc<MetricsStore>>,
    ) -> Self {
        let (ready_tx, ready_rx) = tokio::sync::watch::channel(false);
        Self {
            query_id,
            token,
            queue_bytes,
            metrics_store,
            participants: AtomicUsize::new(0),
            done: AtomicUsize::new(0),
            state: Mutex::new(HarnessState {
                stages: HashMap::new(),
                routing: HashMap::new(),
                started: false,
                any_feeds: false,
                pending_set_plans: Vec::new(),
                coord_metrics: None,
                n_workers: 0,
                leader_mesh: None,
                leader_senders: Vec::new(),
                worker_meshes: Vec::new(),
                launches: HashMap::new(),
                region: None,
            }),
            ready_tx,
            ready_rx,
        }
    }

    /// Wait until the first read finalizes the harness.
    async fn ready(&self) -> Result<()> {
        let mut rx = self.ready_rx.clone();
        while !*rx.borrow() {
            rx.changed().await.map_err(|_| {
                DataFusionError::Internal(
                    "self-hosted shm transport: harness dropped before start".to_string(),
                )
            })?;
        }
        Ok(())
    }

    /// The leader's send end for one task's `WorkUnit` frames, cooperative-draining the
    /// leader's own inbox so a symmetric full-ring stall cannot deadlock the feed push.
    fn leader_feed_sender(&self, stage_num: u32, task_i: usize) -> Result<MppSender> {
        let state = self.state.lock().unwrap();
        let dest_proc = proc_for_task(state.n_workers, task_i as u32);
        let Some(base) = state
            .leader_senders
            .get(dest_proc as usize)
            .and_then(|s| s.as_ref())
        else {
            return internal_err!(
                "self-hosted shm transport: no leader sender for proc {dest_proc}"
            );
        };
        let Some(leader_mesh) = state.leader_mesh.clone() else {
            return internal_err!("self-hosted shm transport: leader mesh not built");
        };
        Ok(base
            .clone_with_header(MppFrameHeader::work_unit(stage_num, task_i as u32, 0))
            .with_cooperative_drain(leader_mesh as Arc<dyn CooperativeDrainSet>))
    }

    fn record_stage(&self, num: usize, tasks: usize, task_partitions: Vec<usize>) {
        let mut state = self.state.lock().unwrap();
        state.stages.insert(
            num as u32,
            StageRec {
                tasks,
                task_partitions,
            },
        );
    }

    /// Classify the routing of every child stage referenced by `plan`'s network boundaries. The
    /// children were dispatched before this stage (plan preparation converts bottom-up), so their
    /// records exist; stages no parent ever claims are consumed by the head on the leader.
    fn scan_for_child_routing(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        consumer_tasks: usize,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        for task_i in 0..consumer_tasks {
            let d_ctx = DistributedTaskContext {
                task_index: task_i,
                task_count: consumer_tasks,
            };
            let state = &mut *state;
            let mut scan_err = Ok(());
            plan.apply_with_dt_ctx(d_ctx, |node, d_ctx| {
                let Some(nb) = node.as_ref().as_network_boundary() else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let child_num = nb.input_stage().num() as u32;
                let Some(rec) = state.stages.get(&child_num) else {
                    scan_err = internal_err!(
                        "self-hosted shm transport: stage {child_num} referenced before dispatch"
                    );
                    return Ok(TreeNodeRecursion::Stop);
                };
                let child_tasks = rec.tasks;
                let child_max_parts = rec.task_partitions.iter().copied().max().unwrap_or(0);

                if node.as_ref().is::<NetworkCoalesceExec>() {
                    let spec = state
                        .routing
                        .entry(child_num)
                        .or_insert_with(|| RoutingSpec::PerTask(vec![None; child_tasks]));
                    let RoutingSpec::PerTask(dest) = spec else {
                        scan_err = internal_err!(
                            "self-hosted shm transport: stage {child_num} read through mixed \
                             boundary kinds"
                        );
                        return Ok(TreeNodeRecursion::Stop);
                    };
                    // Mirror of the consumer's `task_group` split: contiguous groups, the first
                    // `extra` groups one producer task longer.
                    let base = child_tasks / d_ctx.task_count.max(1);
                    let extra = child_tasks % d_ctx.task_count.max(1);
                    let len = base + usize::from(d_ctx.task_index < extra);
                    let start = d_ctx.task_index * base + d_ctx.task_index.min(extra);
                    let end = (start + len).min(child_tasks);
                    for slot in dest[start..end].iter_mut() {
                        *slot = Some(task_i as u32);
                    }
                } else {
                    let spec = state
                        .routing
                        .entry(child_num)
                        .or_insert_with(|| RoutingSpec::PerPartition(vec![None; child_max_parts]));
                    let RoutingSpec::PerPartition(dest) = spec else {
                        scan_err = internal_err!(
                            "self-hosted shm transport: stage {child_num} read through mixed \
                             boundary kinds"
                        );
                        return Ok(TreeNodeRecursion::Stop);
                    };
                    // Shuffle and broadcast read the same partition slice per consumer context.
                    let p_c = nb.partitions_per_consumer_task();
                    let from = (p_c * d_ctx.task_index).min(child_max_parts);
                    let to = (p_c * (d_ctx.task_index + 1)).min(child_max_parts);
                    for slot in dest[from..to].iter_mut() {
                        *slot = Some(task_i as u32);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            scan_err?;
        }
        Ok(())
    }

    /// Size and build the mesh, resolve the routing, and release the waiting task drivers. Runs
    /// once, on the first `open`.
    fn ensure_started(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.started {
            return Ok(());
        }

        let n_workers = state
            .stages
            .values()
            .map(|s| s.tasks)
            .max()
            .unwrap_or(1)
            .max(1) as u32;
        let n_procs = n_workers + 1;

        let region_total = dsm_region_bytes(n_procs, self.queue_bytes, 0)?;
        let region = HeapRegion::new(region_total);
        let wakeup: Arc<dyn Wakeup> = Arc::new(NoopWakeup);
        let interrupt: Arc<dyn Interrupt> = Arc::new(CancellationInterrupt(self.token.clone()));

        let leader_attach = unsafe {
            leader_setup(
                region.base(),
                n_procs,
                self.queue_bytes,
                &[],
                Arc::clone(&wakeup),
                receiver_token(0),
                Arc::clone(&interrupt),
                // The harness holds the senders until the query ends, so attaching is safe,
                // and the plan delivery pump always needs them.
                true,
            )
        }?;
        let leader_mesh = leader_attach.mesh;
        let mut worker_meshes = Vec::with_capacity(n_workers as usize);
        for proc_idx in 1..n_procs {
            let attach = unsafe {
                worker_setup(
                    region.base(),
                    region_total,
                    proc_idx,
                    Arc::clone(&wakeup),
                    receiver_token(proc_idx),
                    Arc::clone(&interrupt),
                )
            }?;
            worker_meshes.push((attach.mesh, attach.outbound_senders));
        }

        // Build every fragment's launch package: one routed sink per output partition, sharing
        // the proc's outbound senders. The base senders drop at the end of this scope, so the
        // rings observe the last-sender detach once the per-partition clones finish.
        let mut launches = HashMap::new();
        for (&stage_num, rec) in state.stages.iter() {
            let spec = state.routing.get(&stage_num);
            for task_i in 0..rec.tasks {
                let n_out = rec.task_partitions.get(task_i).copied().unwrap_or(0);
                let proc = proc_for_task(n_workers, task_i as u32);
                let (mesh, outbound) = &worker_meshes[(proc - 1) as usize];
                let Some(to_leader) = outbound[0].as_ref() else {
                    return internal_err!(
                        "self-hosted shm transport: no outbound sender from proc {proc} to the \
                         leader"
                    );
                };
                // No cooperative drain on purpose: metrics frames go out after the cancellation
                // token fired (it fires on normal completion), when the interrupt-checking spin
                // would abort the send.
                let metrics_sender = to_leader.clone_with_header(MppFrameHeader::task_metrics(
                    stage_num,
                    task_i as u32,
                    proc,
                ));
                let mut sinks: Vec<Box<dyn PartitionSink>> = Vec::with_capacity(n_out);
                for q in 0..n_out {
                    let consumer = match spec {
                        // No parent boundary claimed this stage: the head consumes it on the
                        // leader.
                        None => None,
                        Some(RoutingSpec::PerTask(dest)) => dest.get(task_i).copied().flatten(),
                        Some(RoutingSpec::PerPartition(dest)) => dest.get(q).copied().flatten(),
                    };
                    let dest_proc = match (spec, consumer) {
                        (None, _) | (_, None) => 0,
                        (_, Some(parent_task)) => proc_for_task(n_workers, parent_task),
                    };
                    let Some(base) = outbound[dest_proc as usize].as_ref() else {
                        return internal_err!(
                            "self-hosted shm transport: no outbound sender from proc {proc} to \
                             proc {dest_proc}"
                        );
                    };
                    let sender = base
                        .clone_with_header(MppFrameHeader::batch(stage_num, q as u32, proc))
                        .with_cooperative_drain(Arc::clone(mesh) as Arc<dyn CooperativeDrainSet>);
                    sinks.push(Box::new(MppPartitionSink::new(sender)));
                }
                launches.insert(
                    (stage_num, task_i),
                    Launch {
                        mesh: Arc::clone(mesh),
                        sinks,
                        metrics_sender,
                    },
                );
            }
        }

        state.n_workers = n_workers;
        state.leader_mesh = Some(leader_mesh);
        state.leader_senders = leader_attach.outbound_senders;
        state.worker_meshes = worker_meshes.iter().map(|(m, _)| Arc::clone(m)).collect();
        state.launches = launches;
        state.region = Some(region);
        state.started = true;
        drop(state);
        let _ = self.ready_tx.send(true);
        Ok(())
    }

    fn leader_mesh(&self) -> Result<Arc<MppMesh>> {
        self.state
            .lock()
            .unwrap()
            .leader_mesh
            .clone()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "self-hosted shm transport: leader mesh not built".to_string(),
                )
            })
    }

    /// Wait until the harness is finalized, then take this fragment's launch package.
    async fn wait_launch(&self, stage_num: u32, task_i: usize) -> Result<Launch> {
        self.ready().await?;
        let mut state = self.state.lock().unwrap();
        state.launches.remove(&(stage_num, task_i)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "self-hosted shm transport: no launch package for stage {stage_num} task {task_i}"
            ))
        })
    }
}

/// Delivers one task's plan through the worker session machinery and produces its fragment into
/// the mesh.
struct TaskDriver {
    harness: Arc<QueryHarness>,
    worker: Worker,
    stage_num: u32,
    task_i: usize,
    n_partitions: usize,
    has_feeds: bool,
    task_key: pb::TaskKey,
    token: CancellationToken,
    /// The producer head the distributed planner already baked into the shipped fragment.
    /// Re-stated here so the worker's `insert_producer_head` strips and re-adds the same head
    /// (a no-op) instead of flattening the stage to a single partition.
    producer_head: pb::execute_task_request::ProducerHead,
}

impl TaskDriver {
    async fn run(self) -> Result<()> {
        let harness = Arc::clone(&self.harness);
        let result = self.run_inner().await;
        harness.done.fetch_add(1, Ordering::SeqCst);
        result
    }

    async fn run_inner(self) -> Result<()> {
        let Self {
            harness,
            worker,
            stage_num,
            task_i,
            n_partitions,
            has_feeds,
            task_key,
            token,
            producer_head,
        } = self;

        // The launch package arrives when the first read finalizes the harness. A query torn
        // down before any read just unwinds the driver.
        let launch = tokio::select! {
            launch = harness.wait_launch(stage_num, task_i) => launch?,
            _ = token.cancelled() => return Ok(()),
        };

        // The plan arrives as a `SetPlan` frame on this proc's inbox, shipped by the leader's
        // delivery pump; the pumps drain it into the registry this take resolves from.
        let set_plan_frame = tokio::select! {
            frame = launch.mesh.take_set_plan(stage_num, task_i as u32) => frame?,
            _ = token.cancelled() => return Ok(()),
        };
        let (set_plan, headers) = set_plan_frame.into_parts()?;

        let mesh = Arc::clone(&launch.mesh);
        let outcome = worker
            .set_task_plan(set_plan, headers, move |mut cfg| {
                // Child-stage reads inside the decoded fragment must pull from this proc's
                // inbox, and its dispatcher must stay a no-op: the plans are already here.
                set_distributed_worker_transport(&mut cfg, ShmMqWorkerTransport::new(mesh));
                Ok(cfg)
            })
            .await?;

        // The feeds arrive as `WorkUnit` frames on this proc's inbox; install the channels so
        // the drain fills them (and flushes whatever arrived first). The leader-side pump owns
        // delivery and the `FeedEof` that ends the streams.
        if has_feeds {
            launch.mesh.register_work_unit_senders(
                stage_num,
                task_i as u32,
                outcome.work_unit_senders,
            );
        }

        let produce = async {
            let request = pb::ExecuteTaskRequest {
                task_key: Some(task_key),
                target_partition_start: 0,
                target_partition_end: n_partitions as u64,
                producer_head: Some(producer_head),
            };
            // Through `execute_local_task` rather than a bare `plan.execute` so the task metrics
            // (added/executed/finished stamps, per-node metrics) flow exactly like a pull-based
            // read would deliver them.
            let (streams, _ctx) = execute_local_task(worker.task_data_entries(), request).await?;
            if streams.len() != launch.sinks.len() {
                return internal_err!(
                    "self-hosted shm transport: stage {stage_num} task {task_i} decoded into {} \
                     partitions but routed {} sinks",
                    streams.len(),
                    launch.sinks.len()
                );
            }
            let mut futures = Vec::with_capacity(streams.len());
            for (mut stream, mut sink) in streams.into_iter().zip(launch.sinks) {
                let token = token.clone();
                futures.push(async move {
                    let stream_result: Result<()> = async {
                        loop {
                            let batch = tokio::select! {
                                next = stream.next() => next,
                                // Head stream dropped: stop pulling so this fragment and its upstream
                                // scan unwind, instead of draining the input into a buffer no one reads.
                                _ = token.cancelled() => break,
                            };
                            let Some(batch) = batch else { break };
                            let batch = batch?;
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            sink.send(&batch).await?;
                            // A downstream worker abandoned this stream (its mesh carries the cancel
                            // senders): stop pulling so the cancel cascades to this fragment's own
                            // producers, matching the embedder's `run_worker_fragment` loop.
                            if sink.cancelled() {
                                break;
                            }
                        }
                        Ok(())
                    }
                    .await;
                    // EOF always, even after a failed send, so the consumer side unblocks; the
                    // stream error stays the primary one.
                    let eof_result = sink.finish().await;
                    stream_result.and(eof_result)
                });
            }
            // `join_all`, not fail-fast: cancelling sibling partitions mid-await would skip their
            // EOFs and wedge the consumer's channel buffers.
            let results = futures::future::join_all(futures).await;
            for r in results {
                r?;
            }
            Ok(())
        };
        let produce_res: Result<()> = produce.await;

        // The metrics receiver resolves as the last partition stream above completes, so this
        // does not block on anything remote. The frame goes back over the mesh, where the
        // leader-side pump forwards it into the metrics store.
        if let Ok(task_metrics) = outcome.metrics_rx.await {
            let _ = launch
                .metrics_sender
                .send_task_metrics_best_effort(&task_metrics)
                .await;
        }
        produce_res
    }
}

/// Leader-side delivery of every dispatched plan: waits for finalize to build the rings, then
/// ships each queued plan as a `SetPlan` frame to the proc hosting its task. One pump per query,
/// counted as a participant so the drain pumps outlive it.
async fn run_plan_delivery(harness: Arc<QueryHarness>, token: CancellationToken) -> Result<()> {
    let result = run_plan_delivery_inner(&harness, token).await;
    harness.done.fetch_add(1, Ordering::SeqCst);
    result
}

async fn run_plan_delivery_inner(
    harness: &Arc<QueryHarness>,
    token: CancellationToken,
) -> Result<()> {
    tokio::select! {
        ready = harness.ready() => ready?,
        _ = token.cancelled() => return Ok(()),
    }
    let (pending, senders, metrics) = {
        let mut state = harness.state.lock().unwrap();
        let pending = std::mem::take(&mut state.pending_set_plans);
        let Some(leader_mesh) = state.leader_mesh.clone() else {
            return internal_err!("self-hosted shm transport: leader mesh not built");
        };
        let mut senders = Vec::with_capacity(pending.len());
        for plan in &pending {
            let dest_proc = proc_for_task(state.n_workers, plan.task_i as u32);
            let Some(base) = state
                .leader_senders
                .get(dest_proc as usize)
                .and_then(|s| s.as_ref())
            else {
                return internal_err!(
                    "self-hosted shm transport: no leader sender for proc {dest_proc}"
                );
            };
            senders.push(
                base.clone_with_header(MppFrameHeader::set_plan(
                    plan.stage_num,
                    plan.task_i as u32,
                    0,
                ))
                .with_cooperative_drain(Arc::clone(&leader_mesh) as Arc<dyn CooperativeDrainSet>),
            );
        }
        (pending, senders, state.coord_metrics.clone())
    };
    let mut stats = SendBatchStats::default();
    for (plan, sender) in pending.into_iter().zip(senders) {
        let start = Instant::now();
        sender.send_set_plan_traced(&plan.frame, &mut stats).await?;
        if let Some(metrics) = &metrics {
            metrics.plan_send_latency.record(&start);
            metrics.plan_bytes_sent.add_bytes(plan.plan_size);
        }
    }
    Ok(())
}

/// Leader-side pump for one task's work-unit feeds: drives the providers and ships each unit as
/// a `WorkUnit` frame to the proc hosting the task, then closes the task's channels with a
/// `FeedEof`. The close is unconditional, error or not: without it the fragment's feed streams
/// never end and the worker side wedges instead of surfacing this pump's error.
async fn run_leader_feed_pump(
    harness: Arc<QueryHarness>,
    stage_num: u32,
    task_i: usize,
    feed_streams: Vec<BoxStream<'static, Result<pb::WorkUnit>>>,
    token: CancellationToken,
) -> Result<()> {
    let result = async {
        tokio::select! {
            ready = harness.ready() => ready?,
            _ = token.cancelled() => return Ok(()),
        }
        let sender = harness.leader_feed_sender(stage_num, task_i)?;

        let pump_result = async {
            let mut pumps = Vec::with_capacity(feed_streams.len());
            for mut stream in feed_streams {
                let sender = &sender;
                pumps.push(async move {
                    let mut stats = SendBatchStats::default();
                    while let Some(unit) = stream.next().await {
                        let mut unit = unit?;
                        set_sent_time(&mut unit);
                        sender.send_work_unit_traced(&unit, &mut stats).await?;
                    }
                    Ok::<_, DataFusionError>(())
                });
            }
            futures::future::try_join_all(pumps).await.map(|_| ())
        }
        .await;

        let mut stats = SendBatchStats::default();
        let eof_result = sender.send_feed_eof_traced(&mut stats).await;
        pump_result.and(eof_result)
    }
    .await;
    harness.done.fetch_add(1, Ordering::SeqCst);
    result
}

/// Per-proc drain pumps plus the metrics forwarder, alive until every driver and feed pump
/// reported done. They are what moves control frames when nothing else drains: a fragment
/// blocked on its feed before producing, and the metrics frames arriving after the gather
/// already finished.
async fn run_pumps(harness: Arc<QueryHarness>, token: CancellationToken) -> Result<()> {
    tokio::select! {
        ready = harness.ready() => ready?,
        _ = token.cancelled() => return Ok(()),
    }
    let (leader_mesh, worker_meshes) = {
        let state = harness.state.lock().unwrap();
        let Some(leader_mesh) = state.leader_mesh.clone() else {
            return internal_err!("self-hosted shm transport: leader mesh not built");
        };
        (leader_mesh, state.worker_meshes.clone())
    };
    let metrics_rx = leader_mesh.take_task_metrics_receiver();

    let mut meshes: Vec<Arc<MppMesh>> = Vec::with_capacity(worker_meshes.len() + 1);
    meshes.push(Arc::clone(&leader_mesh));
    meshes.extend(worker_meshes);
    let pumps = meshes.into_iter().map(|mesh| {
        let harness = Arc::clone(&harness);
        async move {
            loop {
                let all_done = harness.done.load(Ordering::SeqCst)
                    >= harness.participants.load(Ordering::SeqCst);
                // Drain errors surface through the consumers reading the same registry;
                // the pump just stops contributing.
                if mesh.try_drain_pass().is_err() {
                    break;
                }
                if all_done {
                    // The pass above ran after the last participant reported done, so every
                    // frame sent before that point has been routed.
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    });
    futures::future::join_all(pumps).await;

    // Every frame is routed by now; whatever metrics arrived are in the channel.
    if let (Some(mut rx), Some(store)) = (metrics_rx, harness.metrics_store.clone()) {
        while let Ok((stage_id, task_number, task_metrics)) = rx.try_recv() {
            store.insert(
                pb::TaskKey {
                    query_id: serialize_uuid(&harness.query_id),
                    stage_id: stage_id as u64,
                    task_number: task_number as u64,
                },
                task_metrics,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::session_context::register_temp_parquet_table;
    use crate::{DistributedConfig, DistributedExt, SessionStateBuilderExt, display_plan_ascii};
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    /// Forces the ring mechanics on every batch: with `RING_SLOTS = 8`, a 64 KiB inbox has
    /// ~8 KiB slots, so the ~16 KiB frames below fragment across slots, and the ~2 MB of
    /// payload wraps each ring dozens of times, exercising the cooperative send spin.
    const TINY_QUEUE_BYTES: usize = 64 * 1024;

    const ROWS: usize = 2000;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));
        // ~1 KiB per row, unique values so the GROUP BY keeps the full volume flowing
        // through the shuffle instead of compacting it away at the partial aggregate.
        let strings: Vec<String> = (0..ROWS)
            .map(|i| format!("{i:06}-{}", "x".repeat(1024)))
            .collect();
        let vals: Vec<i32> = (0..ROWS as i32).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(strings)),
                Arc::new(Int32Array::from(vals)),
            ],
        )
        .unwrap()
    }

    async fn run(ctx: &SessionContext) -> Result<(String, Vec<String>)> {
        // Shaped so every ring frame stays bounded by `shuffle_batch_size`. The strings cross
        // the shuffle inside `max`'s partial state, which the repartition rebuilds with `take`
        // into fresh per-batch arrays; the projection then reduces them to a length before the
        // gather. Shipping `s` itself out of a sort or an aggregate would not work: those emit
        // offset slices of their accumulated state, a sliced variable-length array ships its
        // whole values buffer through arrow-ipc, and a single frame balloons to the size of the
        // partition's state no matter the batch size.
        let query = "SELECT val, length(max(s)) AS l FROM t GROUP BY val";
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let batches: Vec<_> = execute_stream(plan, ctx.task_ctx())?.try_collect().await?;
        let mut lines: Vec<String> = pretty_format_batches(&batches)?
            .to_string()
            .lines()
            .map(str::to_string)
            .collect();
        lines.sort();
        Ok((display, lines))
    }

    /// A high-cardinality shuffle query over rings far smaller than the data, so every
    /// cross-stage byte moves through fragmented frames under send-spin backpressure. The
    /// result must still match the serial reference exactly.
    #[tokio::test]
    async fn tiny_rings_force_fragmentation_and_backpressure() -> Result<()> {
        let transport = SelfHostedShmTransport::default().with_queue_bytes(TINY_QUEUE_BYTES);
        // Small producer batches keep each frame a few slots big instead of overflowing the
        // whole ring (a single frame must fit within one ring).
        let d_cfg = DistributedConfig {
            shuffle_batch_size: 16,
            ..Default::default()
        };
        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_option_extension(d_cfg)
            .with_distributed_planner()
            .with_distributed_task_estimator(2)
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .with_distributed_worker_transport(transport)
            .build();
        state.config_mut().options_mut().execution.target_partitions = 3;
        let ctx = SessionContext::from(state);
        let path =
            register_temp_parquet_table("t", sample_batch().schema(), vec![sample_batch()], &ctx)
                .await?;

        let (display, distributed) = run(&ctx).await?;
        assert!(
            display.contains("NetworkShuffleExec"),
            "the query did not distribute:\n{display}"
        );

        let single = SessionContext::default();
        single
            .register_parquet("t", path.to_string_lossy().as_ref(), Default::default())
            .await?;
        let (_, expected) = run(&single).await?;

        assert_eq!(distributed, expected);
        Ok(())
    }

    /// The per-query harness must be reclaimed when the output stream drops, or the `queries` map
    /// grows one entry per query for the transport's lifetime.
    #[tokio::test]
    async fn query_harness_is_reclaimed_after_the_stream_drops() -> Result<()> {
        let transport = SelfHostedShmTransport::default();
        let probe = transport.clone();
        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_distributed_planner()
            .with_distributed_task_estimator(2)
            .with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
            .with_distributed_worker_transport(transport)
            .build();
        state.config_mut().options_mut().execution.target_partitions = 3;
        let ctx = SessionContext::from(state);
        register_temp_parquet_table("t", sample_batch().schema(), vec![sample_batch()], &ctx)
            .await?;

        let (display, _) = run(&ctx).await?;
        assert!(
            display.contains("NetworkShuffleExec"),
            "the query did not distribute:\n{display}"
        );

        // The token fires as the gathered stream drops above; the registry entry is dropped on a
        // task the cancelled token wakes, so poll briefly rather than assume it already ran.
        for _ in 0..100 {
            if probe.queries.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "queries map still holds {} entries; the harness leaked",
            probe.queries.len()
        );
    }
}
