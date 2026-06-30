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

//! In-process instantiation of the shared-memory transport, plus an end-to-end test.
//!
//! A real distributed query runs through [`ShmChannelResolver`] with no Postgres and no Flight.
//! This is the rebase-safety payoff: a single process plays every role that production splits
//! across the leader (build and slice the plan), Postgres (allocate the DSM region, launch the
//! workers), and each worker (run a producer fragment and push it through the mesh). If an upstream
//! change breaks the channel-protocol contract, the boundary routing API, or the ready-to-run
//! fragment contract, the test below fails here, before any downstream embedder rebuilds.
//!
//! Workers are passive executors: they run the ready-to-run per-task plans the coordinator
//! dispatches (task-specialized, nested stages `Remote`), whose boundary leaves read the mesh
//! through the session's [`ShmChannelResolver`]. Nothing on the worker re-plans, converts, or
//! dispatches. A [`DispatchPlanSource`] on the leader session captures each dispatched plan,
//! standing in for a production source that serializes it with the embedder's codec.
//!
//! What's faithful vs. simplified relative to the Postgres path:
//! - Faithful: the DSM ring mesh, the framing, the cooperative drain, `ShmWorkerChannel::execute_task`,
//!   the per-fragment routing (`collect_dispatched_stages`), `run_worker_fragment`, the leader
//!   consuming via `DistributedExec::execute`, and the dispatch handoff through
//!   `DispatchPlanSource`.
//! - Simplified: the dispatched subplans are captured as `Arc`s rather than serialized through
//!   the `SetPlan` frames the coordinator routes over the mesh (all roles live in one address
//!   space), the wakeup is a no-op (the cooperative consumer yields rather than parking), and
//!   there's no cancellation source.

use std::alloc::Layout;
use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, HashMap, Result};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{
    DispatchPlanSource, DistributedConfig, DistributedExec, DistributedExt, DistributedLeafExec,
    DistributedTaskContext, NetworkBoundaryExt, NetworkBroadcastExec, NetworkCoalesceExec,
    NetworkShuffleExec, PartitionSink, SessionStateBuilderExt, Stage, TaskEstimation,
    TaskEstimator, TaskKey, WorkerSink, decode_task_metrics,
};

use super::mpsc_ring::Wakeup;
use super::runtime::{InProcessWorkerResolver, MppMesh, ShmChannelResolver, proc_for_task};
use super::setup::{
    collect_task_metrics, dsm_region_bytes, leader_setup, run_worker_fragment, worker_setup,
};
use super::transport::{
    CooperativeDrainSet, MppFrameHeader, MppPartitionSink, MppSender, NoInterrupt,
};

/// Per-inbox DSM ring size for the in-process mesh. Generous: the test ships a handful of tiny
/// batches, so backpressure never kicks in. Production sizes this from `paradedb.mpp_queue_size`.
const IN_PROCESS_QUEUE_BYTES: usize = 1 << 20;

/// No-op wakeup. The cooperative consumer in `runtime::pull_partition_stream` yields rather
/// than parking, so a publish never needs to wake a blocked thread. The real cross-thread wakeup
/// extension point is covered by `mpsc_ring`'s `injected_wakeup_unparks_blocked_consumer`.
struct NoopWakeup;
impl Wakeup for NoopWakeup {
    fn wake(&self, _token: u64) {}
}

/// Owns the single heap buffer that stands in for the PG DSM segment. Every proc (leader + workers)
/// reads and writes the same region through raw pointers; the lock-free rings make concurrent
/// access sound. Kept alive in the harness until after all producer tasks join.
struct HeapRegion {
    ptr: *mut u8,
    layout: Layout,
}

impl HeapRegion {
    fn new(bytes: usize) -> Self {
        // 64-byte alignment so each per-inbox ring header lands on its own cache line; the
        // dsm layout aligns the offsets within the region, but only if the base is aligned too.
        let layout = Layout::from_size_align(bytes, 64).expect("dsm region layout");
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "dsm region alloc failed");
        Self { ptr, layout }
    }

    fn base(&self) -> *mut c_void {
        self.ptr as *mut c_void
    }
}

impl Drop for HeapRegion {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

/// Send wrapper so the leader-init / worker-attach raw base pointer can be handed to the per-proc
/// setup. All setup runs on the harness thread, so the pointer never crosses into a spawned task;
/// only the resulting `Send` meshes and senders do.
#[derive(Clone, Copy)]
struct SharedBase(*mut c_void);
unsafe impl Send for SharedBase {}

/// Opaque, non-sentinel receiver token. `NoopWakeup` ignores the value; this just exercises the
/// `set_receiver` path with something the producer won't skip as "no consumer registered".
fn receiver_token(proc_idx: u32) -> u64 {
    proc_idx as u64 + 1
}

// ---------------------------------------------------------------------------------------------
// Leader-side producer-stage discovery (in-process port of pg_search's `collect_dispatched_stages`).
// The crate's gRPC path keys dispatch on resolver URLs and never decides this; the shm_mq peers are
// push-driven without URLs, so the embedder classifies each boundary's routing here.
// ---------------------------------------------------------------------------------------------

/// Routing rule for a producer fragment's output partitions.
#[derive(Clone, Debug)]
enum FragmentRouting {
    /// Every output partition goes to one destination proc (a `NetworkCoalesceExec`, or the
    /// top-level gather to the leader).
    Coalesce { dest_proc: u32 },
    /// Hash-partitioned mesh (`NetworkShuffleExec` / `NetworkBroadcastExec`): output partition `q`
    /// goes to the consumer task the crate's `route_partition(q)` selects.
    Hashed {
        consumer_task: Vec<u32>,
        broadcast: bool,
    },
}

/// One producer stage's routing metadata, captured from a network boundary. The stage's plan is
/// not captured here: workers run the specialized plans the coordinator dispatches, delivered
/// through the leader session's [`CapturingPlanSource`].
struct StageEntry {
    stage_num: u32,
    task_count: usize,
    routing: FragmentRouting,
}

/// The dispatched plans, keyed by `(stage_id, task_number)`. Filled by [`CapturingPlanSource`]
/// while the leader's `execute` prepares the query; read by the worker tasks. One query per
/// mesh, so the key drops the query id.
type CapturedPlans = Arc<Mutex<HashMap<(usize, usize), Arc<dyn ExecutionPlan>>>>;

/// Test-harness [`DispatchPlanSource`]: records each `(stage, task)` specialized plan the
/// coordinator hands over, standing in for a production source that serializes it with the
/// embedder's codec. The returned empty bytes keep the coordinator from encoding plans nobody
/// decodes; the workers run the captured `Arc`s directly.
struct CapturingPlanSource(CapturedPlans);

impl DispatchPlanSource for CapturingPlanSource {
    fn dispatch_plan_proto(
        &self,
        task: &crate::TaskKey,
        specialized: &Arc<dyn ExecutionPlan>,
    ) -> Option<Result<Vec<u8>>> {
        self.0
            .lock()
            .unwrap()
            .insert((task.stage_id, task.task_number), Arc::clone(specialized));
        Some(Ok(Vec::new()))
    }
}

/// Walk the distributed physical plan and collect every producer stage, once per boundary.
fn collect_dispatched_stages(root: &Arc<dyn ExecutionPlan>, n_workers: u32) -> Vec<StageEntry> {
    let mut out = Vec::new();
    collect_stages(root, n_workers, /* nested = */ false, &mut out);
    out
}

fn collect_stages(
    plan: &Arc<dyn ExecutionPlan>,
    n_workers: u32,
    nested: bool,
    out: &mut Vec<StageEntry>,
) {
    if let Some(nb) = plan.as_ref().as_network_boundary() {
        let stage = nb.input_stage();
        let stage_id = stage.num() as u32;
        let route_consumer_tasks = || {
            // The crate owns the consumer-slice layout, so the push side reads it from
            // `route_partition` instead of re-deriving it and drifting when the layout changes.
            let n_out = stage
                .local_plan()
                .map_or(0, |p| p.properties().partitioning.partition_count());
            (0..n_out)
                .map(|q| {
                    nb.route_partition(q)
                        .expect("route_partition")
                        .consumer_task as u32
                })
                .collect::<Vec<u32>>()
        };
        let plan_any = plan.as_ref();
        let routing = if plan_any.is::<NetworkCoalesceExec>() {
            if nested {
                FragmentRouting::Coalesce {
                    dest_proc: proc_for_task(n_workers, 0),
                }
            } else {
                FragmentRouting::Coalesce { dest_proc: 0 }
            }
        } else if plan_any.is::<NetworkShuffleExec>() {
            assert!(nested, "top-level NetworkShuffleExec is unsupported");
            FragmentRouting::Hashed {
                consumer_task: route_consumer_tasks(),
                broadcast: false,
            }
        } else if plan_any.is::<NetworkBroadcastExec>() {
            assert!(nested, "top-level NetworkBroadcastExec is unsupported");
            FragmentRouting::Hashed {
                consumer_task: route_consumer_tasks(),
                broadcast: true,
            }
        } else {
            panic!("unrecognized network boundary {}", plan.name());
        };

        let task_count = stage.task_count();
        if let Some(stage_plan) = stage.local_plan() {
            out.push(StageEntry {
                stage_num: stage_id,
                task_count,
                routing,
            });
            // The boundary's children() returns [stage.plan], so descending here would double-count
            // every nested stage. Recurse through the stage plan directly with nested = true.
            collect_stages(stage_plan, n_workers, true, out);
        }
        return;
    }
    for child in plan.children() {
        collect_stages(child, n_workers, nested, out);
    }
}

/// One producer fragment assigned to a worker proc: a single task of a producer stage. The plan
/// arrives separately, through the [`CapturedPlans`] map the leader's dispatch fills.
struct FragmentAssignment {
    stage_id: u32,
    task_idx: usize,
    task_count: usize,
    routing: FragmentRouting,
}

/// Rebuild a plan subtree so each fragment executes its own node instances. In production every
/// proc decodes its own copy of the plan; a captured `Arc` is shared with the coordinator and
/// with sibling tasks of the same stage, and sharing execute-once state breaks (a
/// `RepartitionExec` panics when a second fragment executes a partition the first already
/// consumed; a boundary's connection pool hands each partition stream out once). A `Remote`
/// boundary is childless, so it gets a fresh node explicitly; other leaves are shared (they
/// carry no execute-once state here).
fn reinstantiate(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(nb) = plan.as_ref().as_network_boundary()
        && matches!(nb.input_stage(), Stage::Remote(_))
    {
        return nb
            .with_input_stage(nb.input_stage().clone())
            .expect("with_input_stage with the same stage");
    }
    let children: Vec<_> = plan.children().into_iter().map(reinstantiate).collect();
    if children.is_empty() {
        Arc::clone(plan)
    } else {
        Arc::clone(plan)
            .with_new_children(children)
            .expect("with_new_children with the same arity")
    }
}

/// Wait for the leader's dispatch to capture this fragment's plan. The capture happens inside
/// the leader's `execute` (during plan preparation), which interleaves with the worker tasks on
/// the cooperative runtime. Bounded so a dispatch that never happens fails the test instead of
/// hanging it.
async fn captured_plan(
    captured: &CapturedPlans,
    stage_id: u32,
    task_idx: usize,
) -> Arc<dyn ExecutionPlan> {
    for _ in 0..100_000 {
        if let Some(plan) = captured
            .lock()
            .unwrap()
            .get(&(stage_id as usize, task_idx))
            .cloned()
        {
            return plan;
        }
        tokio::task::yield_now().await;
    }
    panic!("no dispatched plan captured for stage {stage_id} task {task_idx}");
}

/// Expand the dispatched stages into the fragments `this_proc` owns under `proc_for_task`.
fn fragments_for_proc(
    entries: &[StageEntry],
    this_proc: u32,
    n_workers: u32,
) -> Vec<FragmentAssignment> {
    let mut out = Vec::new();
    for entry in entries {
        for task_idx in 0..entry.task_count {
            if proc_for_task(n_workers, task_idx as u32) != this_proc {
                continue;
            }
            // Broadcast caps its build subtree at task 0; the other tasks would re-emit the same
            // canonical replica and the consumer's select_all would over-count.
            if matches!(
                entry.routing,
                FragmentRouting::Hashed {
                    broadcast: true,
                    ..
                }
            ) && task_idx != 0
            {
                continue;
            }
            out.push(FragmentAssignment {
                stage_id: entry.stage_num,
                task_idx,
                task_count: entry.task_count,
                routing: entry.routing.clone(),
            });
        }
    }
    out
}

/// Build a fragment's `TaskContext`, carrying the right `DistributedTaskContext` so nested boundary
/// nodes know their `(task_index, task_count)` and the worker session's channel resolver rides
/// along for their mesh reads.
fn fragment_task_ctx(
    session: &SessionContext,
    task_index: usize,
    task_count: usize,
) -> Arc<TaskContext> {
    let cfg = session
        .state()
        .config()
        .clone()
        .with_extension(Arc::new(DistributedTaskContext {
            task_index,
            task_count,
        }));
    Arc::new(TaskContext::default().with_session_config(cfg))
}

/// Run all fragments owned by one worker proc, then signal completion. Mirrors the body of
/// pg_search's `run_mpp_worker`: build a [`WorkerSink`] that routes by partition, open a
/// [`PartitionSink`] per output partition, execute each dispatched fragment as-is (it arrives
/// ready-to-run, nested stages `Remote`, boundary leaves reading the mesh), and join.
async fn run_worker_proc(
    fragments: Vec<FragmentAssignment>,
    outbound: Vec<Option<MppSender>>,
    mesh: Arc<MppMesh>,
    session: SessionContext,
    n_workers: u32,
    captured: CapturedPlans,
) -> Result<()> {
    let mut routing = HashMap::new();
    for fragment in &fragments {
        routing.insert(fragment.stage_id, fragment.routing.clone());
    }
    // One sink serves every stage this proc produces; it owns the base outbound senders and routes
    // each (stage, partition) to the destination proc's send end.
    let worker_sink = ShmMqWorkerSink {
        outbound,
        mesh: Arc::clone(&mesh),
        n_workers,
        routing,
    };

    let mut prepared = Vec::with_capacity(fragments.len());
    for fragment in &fragments {
        let task_ctx = fragment_task_ctx(&session, fragment.task_idx, fragment.task_count);
        // Production decodes a fresh plan per fragment; the captured Arc is shared, so copy it.
        let plan =
            reinstantiate(&captured_plan(&captured, fragment.stage_id, fragment.task_idx).await);
        let n_out = plan.output_partitioning().partition_count();
        let mut sinks: Vec<Box<dyn PartitionSink>> = Vec::with_capacity(n_out);
        for q in 0..n_out {
            sinks.push(worker_sink.open_partition(fragment.stage_id as usize, q)?);
        }
        prepared.push((fragment, plan, sinks, task_ctx));
    }
    // The metrics frames go to the leader after the fragments finish; the clone keeps one sender
    // on the leader's inbox alive past the drop below, which only delays that ring's detach
    // observation, never a per-channel EOF.
    let metrics_sender_base = worker_sink
        .outbound
        .first()
        .and_then(|s| s.as_ref())
        .map(|s| s.clone_with_header(MppFrameHeader::task_metrics(0, 0, mesh.this_proc)));
    // Drop the base senders so the only senders left are the per-partition clones the fragment
    // futures own; otherwise the rings never observe the last-sender detach.
    drop(worker_sink);

    let mut futures = Vec::with_capacity(prepared.len());
    let mut executed = Vec::with_capacity(prepared.len());
    for (fragment, plan, sinks, task_ctx) in prepared {
        executed.push((
            fragment.stage_id,
            fragment.task_idx,
            fragment.task_count,
            Arc::clone(&plan),
        ));
        futures.push(run_worker_fragment(plan, sinks, task_ctx));
    }
    for r in futures::future::join_all(futures).await {
        r?;
    }
    // Report per-fragment metrics, the same frames pg's workers ship after their last EOF; the
    // leader files them into the executed plan's metrics store.
    if let Some(base) = metrics_sender_base {
        for (stage_id, task_idx, task_count, plan) in &executed {
            let frame = collect_task_metrics(plan, *task_idx, *task_count);
            let sender = base.clone_with_header(MppFrameHeader::task_metrics(
                *stage_id,
                *task_idx as u32,
                mesh.this_proc,
            ));
            let _ = sender.send_task_metrics_best_effort(&frame).await;
        }
    }
    Ok(())
}

/// Test-harness [`WorkerSink`]: routes each `(stage, partition)` to the destination proc's outbound
/// send end, the in-process analog of what pg_search builds on a real backend. Holds the base
/// senders plus the per-stage routing so `open_partition` reproduces the header + cooperative-drain
/// wiring the produce loop used to apply inline.
struct ShmMqWorkerSink {
    outbound: Vec<Option<MppSender>>,
    mesh: Arc<MppMesh>,
    n_workers: u32,
    routing: HashMap<u32, FragmentRouting>,
}

impl WorkerSink for ShmMqWorkerSink {
    fn open_partition(&self, stage: usize, partition: usize) -> Result<Box<dyn PartitionSink>> {
        let routing = self.routing.get(&(stage as u32)).ok_or_else(|| {
            DataFusionError::Internal(format!("run_worker_proc: no routing for stage {stage}"))
        })?;
        let dest_proc = match routing {
            FragmentRouting::Coalesce { dest_proc } => *dest_proc,
            FragmentRouting::Hashed { consumer_task, .. } => {
                proc_for_task(self.n_workers, consumer_task[partition])
            }
        };
        let base = self.outbound[dest_proc as usize].as_ref().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "run_worker_proc: no outbound sender for dest proc {dest_proc}"
            ))
        })?;
        let sender = base
            .clone_with_header(MppFrameHeader::batch(
                stage as u32,
                partition as u32,
                self.mesh.this_proc,
            ))
            .with_cooperative_drain(Arc::clone(&self.mesh) as Arc<dyn CooperativeDrainSet>);
        Ok(Box::new(MppPartitionSink::new(sender)))
    }
}

/// Splits an in-memory `DataSourceExec` leaf across tasks, the in-memory analog of the crate's
/// `FileScanConfigTaskEstimator` (which only handles file scans). Each task reads a disjoint subset
/// of the source's partitions, so a gather over the tasks reproduces the serial result exactly.
#[derive(Debug)]
struct MemShardEstimator {
    n_tasks: usize,
}

impl MemShardEstimator {
    fn mem_source(plan: &Arc<dyn ExecutionPlan>) -> Option<&MemorySourceConfig> {
        plan.downcast_ref::<DataSourceExec>()?
            .data_source()
            .downcast_ref::<MemorySourceConfig>()
    }
}

impl TaskEstimator for MemShardEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        Self::mem_source(plan).map(|_| TaskEstimation::desired(self.n_tasks))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if task_count <= 1 {
            return Ok(None);
        }
        let Some(mem) = Self::mem_source(plan) else {
            return Ok(None);
        };
        let parts = mem.partitions().to_vec();
        let n_part = parts.len();
        // The stored batches are unprojected; reuse the source's exact schema + projection so each
        // variant's projected output schema matches the original leaf.
        let unprojected_schema: SchemaRef = parts
            .iter()
            .flatten()
            .next()
            .map(|b| b.schema())
            .unwrap_or_else(|| plan.schema());
        let projection = mem.projection().clone();
        let variants = (0..task_count).map(|i| {
            // Keep every variant at the original partition count (pad with empties) so
            // DistributedLeafExec's same-partition-count contract holds; round-robin the
            // non-empty partitions so each task reads a disjoint slice.
            let per_task: Vec<Vec<RecordBatch>> = (0..n_part)
                .map(|j| {
                    if j % task_count == i {
                        parts[j].clone()
                    } else {
                        Vec::new()
                    }
                })
                .collect();
            MemorySourceConfig::try_new_exec(
                &per_task,
                unprojected_schema.clone(),
                projection.clone(),
            )
            .expect("memory variant") as Arc<dyn ExecutionPlan>
        });
        Ok(Some(Arc::new(DistributedLeafExec::try_new(
            Arc::clone(plan),
            variants,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::MemTable;
    use futures::TryStreamExt;

    /// Total procs = leader (proc 0) + `N_WORKERS` producers. With round-robin `proc_for_task`,
    /// worker proc `p` runs producer task `p - 1`.
    const N_WORKERS: u32 = 3;

    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]))
    }

    /// `N_WORKERS` partitions, two rows each, so the shard estimator hands one partition per task.
    fn table_partitions() -> Vec<Vec<RecordBatch>> {
        let schema = table_schema();
        (0..N_WORKERS as i32)
            .map(|p| {
                let ids = Int32Array::from(vec![p * 2, p * 2 + 1]);
                let vals = Int32Array::from(vec![p * 20, p * 20 + 10]);
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vals)])
                        .unwrap();
                vec![batch]
            })
            .collect()
    }

    fn register_table(ctx: &SessionContext) {
        let table = MemTable::try_new(table_schema(), table_partitions()).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
    }

    /// A worker is a consumer too (it reads shuffle inputs), so when one of its input streams drops
    /// early it has to cancel that stream's producer, not just the leader. This checks the wiring
    /// end-to-end: a worker proc's `cancel_stream` reaches the producing proc's inbox through the
    /// control senders `worker_setup` installs, and the producer records it. The producer then ends
    /// the stream cleanly, which `producer_send_ends_when_consumer_cancels_the_stream` covers.
    #[test]
    fn worker_consumer_cancels_its_producer() {
        use crate::shm::transport::CooperativeDrainSet;

        // procs: leader 0, plus workers 1 and 2.
        let boot = bootstrap_mesh(3);
        let consumer = Arc::clone(&boot.workers[0].1); // proc 1
        let producer = Arc::clone(&boot.workers[1].1); // proc 2

        assert!(!producer.stream_cancelled(7, 0));

        // Proc 1 abandons the `(stage 7, partition 0)` stream it reads from proc 2.
        consumer.cancel_stream(2, 7, 0);

        // Proc 2 drains its inbox and sees the cancel its consumer sent.
        producer.try_drain_pass().unwrap();
        assert!(producer.stream_cancelled(7, 0));
        // Scoped to that one stream: a sibling partition stays live.
        assert!(!producer.stream_cancelled(7, 1));
    }

    /// `dispatch_capture` is Some on the leader session only: its coordinator is the one that
    /// dispatches, and the capturing source stands in for a production embedder's serializing
    /// source.
    fn build_session(
        mesh: Arc<MppMesh>,
        dispatch_capture: Option<CapturedPlans>,
    ) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(N_WORKERS as usize);
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_worker_resolver(InProcessWorkerResolver::new(N_WORKERS as usize))
            .with_distributed_channel_resolver(ShmChannelResolver::new(mesh))
            .with_distributed_task_estimator(MemShardEstimator {
                n_tasks: N_WORKERS as usize,
            })
            .with_distributed_planner();
        if let Some(captured) = dispatch_capture {
            builder = builder
                .with_distributed_dispatch_plan_source(CapturingPlanSource(captured))
                .with_distributed_metrics_collection(true)
                .expect("with_distributed_metrics_collection");
        }
        let ctx = SessionContext::new_with_state(builder.build());
        register_table(&ctx);
        ctx
    }

    fn new_captured_plans() -> CapturedPlans {
        Arc::new(Mutex::new(HashMap::default()))
    }

    /// Collect the `id` column out of a batch list, in row order.
    fn ids_of(batches: &[RecordBatch]) -> Vec<i32> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column");
            out.extend((0..col.len()).map(|i| col.value(i)));
        }
        out
    }

    /// A real distributed query runs end-to-end through the shm_mq transport with no Postgres and
    /// no Flight: producer fragments push through the heap-backed mesh while the leader gathers via
    /// `DistributedExec::execute`. The gathered, ordered result must match the serial reference.
    ///
    /// Single-threaded runtime on purpose: it mirrors the per-process current-thread runtime each
    /// PG worker runs, and it's exactly the cooperative model the transport is built for (the
    /// producer send spin and the consumer pull loop interleave by yielding, not by parallelism).
    #[tokio::test(flavor = "current_thread")]
    async fn in_process_distributed_query_matches_serial() {
        let query = "SELECT id, val FROM t ORDER BY id";

        // Serial reference: same query, no distribution.
        let serial_ctx = SessionContext::new();
        register_table(&serial_ctx);
        let expected = serial_ctx
            .sql(query)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected_ids = ids_of(&expected);
        assert_eq!(expected_ids, vec![0, 1, 2, 3, 4, 5]);

        // One heap region stands in for the DSM segment; size it for n_procs = leader + workers.
        let n_procs = N_WORKERS + 1;
        let region_total = dsm_region_bytes(n_procs, IN_PROCESS_QUEUE_BYTES, 0).unwrap();
        let region = HeapRegion::new(region_total);
        let base = SharedBase(region.base());
        let wakeup: Arc<dyn Wakeup> = Arc::new(NoopWakeup);

        // Leader first (it initializes the rings), then each worker attaches. No plan bytes travel
        // through the region here; all roles share the producer subplans as Arcs.
        let leader_mesh = unsafe {
            leader_setup(
                base.0,
                n_procs,
                IN_PROCESS_QUEUE_BYTES,
                &[],
                Arc::clone(&wakeup),
                receiver_token(0),
                Arc::new(NoInterrupt),
                /* attach_senders */ false,
            )
        }
        .unwrap()
        .mesh;
        let mut worker_setups = Vec::new();
        for proc_idx in 1..n_procs {
            let attach = unsafe {
                worker_setup(
                    base.0,
                    region_total,
                    proc_idx,
                    Arc::clone(&wakeup),
                    receiver_token(proc_idx),
                    Arc::new(NoInterrupt),
                )
            }
            .unwrap();
            worker_setups.push((proc_idx, attach.mesh, attach.outbound_senders));
        }

        // Build the distributed plan once on the leader session; producers and consumer share it.
        let captured = new_captured_plans();
        let leader_ctx = build_session(Arc::clone(&leader_mesh), Some(Arc::clone(&captured)));
        let physical = leader_ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(
            physical.is::<DistributedExec>(),
            "expected a DistributedExec root, got {}",
            physical.name()
        );

        let entries = collect_dispatched_stages(&physical, N_WORKERS);
        // Guard against a planner change that silently collapses the query to a trivial one-task
        // gather: the producer stage must actually fan across every worker, or the transport's
        // multi-task routing never gets exercised.
        assert!(
            entries.iter().any(|e| e.task_count == N_WORKERS as usize),
            "expected a producer stage with task_count = {N_WORKERS}; got {:?}",
            entries.iter().map(|e| e.task_count).collect::<Vec<_>>()
        );

        // Launch the producer fragments before the leader pulls, so the mesh has data flowing while
        // the consumer drains. On the current-thread runtime the spawned tasks interleave with the
        // consumer by yielding, the same cooperative model each PG worker runs.
        let mut workers = JoinSet::new();
        for (proc_idx, mesh, outbound) in worker_setups {
            let fragments = fragments_for_proc(&entries, proc_idx, N_WORKERS);
            let session = build_session(Arc::clone(&mesh), None);
            workers.spawn(run_worker_proc(
                fragments,
                outbound,
                mesh,
                session,
                N_WORKERS,
                Arc::clone(&captured),
            ));
        }

        // Leader consumer: execute the DistributedExec root, same as the production embedder; the
        // network boundary nodes pull from the mesh through ShmWorkerChannel::execute_task.
        let leader_task_ctx = leader_ctx.task_ctx();
        let stream = physical.execute(0, leader_task_ctx).unwrap();
        let got: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        while let Some(res) = workers.join_next().await {
            res.expect("worker task panicked").expect("worker proc");
        }

        let got_ids = ids_of(&got);
        assert_eq!(got_ids, expected_ids, "distributed gather != serial");

        // The workers shipped one metrics frame per fragment over the mesh; file them into the
        // executed plan's store the way the pg embedder does, and require full coverage.
        let dist = physical
            .downcast_ref::<DistributedExec>()
            .expect("DistributedExec");
        let store = dist.metrics_store().expect("metrics collection enabled");
        let mut query_id = None;
        let mut expected_reports = 0usize;
        let _ = physical.apply(|node| {
            if let Some(nb) = node.as_ref().as_network_boundary() {
                let stage = nb.input_stage();
                query_id.get_or_insert_with(|| stage.query_id());
                expected_reports += stage.task_count();
            }
            Ok(TreeNodeRecursion::Continue)
        });
        let query_id = query_id.expect("a distributed plan has at least one boundary");
        let mut rx = leader_mesh
            .take_task_metrics_receiver()
            .expect("task metrics receiver");
        let mut inserted = 0usize;
        for _ in 0..1_000 {
            let _ = leader_mesh.try_drain_pass();
            while let Ok((stage_id, task_number, metrics)) = rx.try_recv() {
                let metrics = decode_task_metrics(metrics).expect("decode task metrics");
                store.insert(
                    TaskKey {
                        query_id,
                        stage_id: stage_id as usize,
                        task_number: task_number as usize,
                    },
                    metrics,
                );
                inserted += 1;
            }
            if inserted >= expected_reports {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            inserted, expected_reports,
            "every producer task reports metrics"
        );

        // `region` is declared before the meshes, so reverse drop order frees it after every
        // receiver handle into it is gone.
    }

    /// Mesh bootstrap shared by the tests: leader first (it initializes the rings), then each
    /// worker attaches.
    struct Bootstrap {
        leader_mesh: Arc<MppMesh>,
        workers: Vec<(u32, Arc<MppMesh>, Vec<Option<MppSender>>)>,
        // Last field on purpose: struct fields drop in declaration order, so the region
        // outlives every receiver handle into it.
        _region: HeapRegion,
    }

    fn bootstrap_mesh(n_procs: u32) -> Bootstrap {
        let region_total = dsm_region_bytes(n_procs, IN_PROCESS_QUEUE_BYTES, 0).unwrap();
        let region = HeapRegion::new(region_total);
        let base = SharedBase(region.base());
        let wakeup: Arc<dyn Wakeup> = Arc::new(NoopWakeup);
        let leader_mesh = unsafe {
            leader_setup(
                base.0,
                n_procs,
                IN_PROCESS_QUEUE_BYTES,
                &[],
                Arc::clone(&wakeup),
                receiver_token(0),
                Arc::new(NoInterrupt),
                /* attach_senders */ false,
            )
        }
        .unwrap()
        .mesh;
        let mut workers = Vec::new();
        for proc_idx in 1..n_procs {
            let attach = unsafe {
                worker_setup(
                    base.0,
                    region_total,
                    proc_idx,
                    Arc::clone(&wakeup),
                    receiver_token(proc_idx),
                    Arc::new(NoInterrupt),
                )
            }
            .unwrap();
            workers.push((proc_idx, attach.mesh, attach.outbound_senders));
        }
        Bootstrap {
            leader_mesh,
            workers,
            _region: region,
        }
    }

    /// A `GROUP BY` plans a nested `NetworkShuffleExec`, so this exercises hash-routed
    /// worker-to-worker traffic and the self-loop sender, which the plain gather test never
    /// touches. That routing is the main thing an upstream rebase can silently break.
    #[tokio::test(flavor = "current_thread")]
    async fn in_process_shuffle_query_matches_serial() {
        let query = "SELECT val, count(*) AS c FROM t GROUP BY val ORDER BY val";

        let serial_ctx = SessionContext::new();
        register_table(&serial_ctx);
        let expected = serial_ctx
            .sql(query)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let boot = bootstrap_mesh(N_WORKERS + 1);
        let captured = new_captured_plans();
        let leader_ctx = build_session(Arc::clone(&boot.leader_mesh), Some(Arc::clone(&captured)));
        let physical = leader_ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let entries = collect_dispatched_stages(&physical, N_WORKERS);
        assert!(
            entries
                .iter()
                .any(|e| matches!(e.routing, FragmentRouting::Hashed { .. })),
            "expected a hash-routed producer stage; got {:?}",
            entries.iter().map(|e| &e.routing).collect::<Vec<_>>()
        );

        let mut workers = JoinSet::new();
        for (proc_idx, mesh, outbound) in boot.workers {
            let fragments = fragments_for_proc(&entries, proc_idx, N_WORKERS);
            let session = build_session(Arc::clone(&mesh), None);
            workers.spawn(run_worker_proc(
                fragments,
                outbound,
                mesh,
                session,
                N_WORKERS,
                Arc::clone(&captured),
            ));
        }

        let leader_task_ctx = leader_ctx.task_ctx();
        let stream = physical.execute(0, leader_task_ctx).unwrap();
        let got: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        while let Some(res) = workers.join_next().await {
            res.expect("worker task panicked").expect("worker proc");
        }

        use datafusion::arrow::util::pretty::pretty_format_batches;
        assert_eq!(
            pretty_format_batches(&expected).unwrap().to_string(),
            pretty_format_batches(&got).unwrap().to_string(),
            "distributed shuffle != serial"
        );
    }

    /// A producer that attaches and then goes away without sending its EOFs must fail the
    /// gather, not hang it: the drain fails the channels the dead receiver fed once the ring
    /// detaches.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_loss_fails_the_gather_instead_of_hanging() {
        let query = "SELECT id, val FROM t ORDER BY id";

        let boot = bootstrap_mesh(N_WORKERS + 1);
        let captured = new_captured_plans();
        let leader_ctx = build_session(Arc::clone(&boot.leader_mesh), Some(Arc::clone(&captured)));
        let physical = leader_ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let entries = collect_dispatched_stages(&physical, N_WORKERS);

        let mut workers = JoinSet::new();
        for (proc_idx, mesh, outbound) in boot.workers {
            if proc_idx == 1 {
                // Simulated crash: the proc attached (its senders exist), then dies without
                // running its fragments or sending EOF. Dropping the senders is what process
                // exit does.
                drop(outbound);
                drop(mesh);
                continue;
            }
            let fragments = fragments_for_proc(&entries, proc_idx, N_WORKERS);
            let session = build_session(Arc::clone(&mesh), None);
            workers.spawn(run_worker_proc(
                fragments,
                outbound,
                mesh,
                session,
                N_WORKERS,
                Arc::clone(&captured),
            ));
        }

        let leader_task_ctx = leader_ctx.task_ctx();
        let stream = physical.execute(0, leader_task_ctx).unwrap();
        let res: Result<Vec<RecordBatch>, _> = stream.try_collect().await;

        while let Some(r) = workers.join_next().await {
            r.expect("worker task panicked").expect("worker proc");
        }

        let err = res
            .expect_err("gather must fail when a producer goes away")
            .to_string();
        assert!(
            err.contains("detached before this channel's EOF"),
            "unexpected error: {err}"
        );
    }
}
