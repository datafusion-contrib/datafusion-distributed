use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::task_spawner::{
    CoordinatorToWorkerMetrics, CoordinatorToWorkerTaskSpawner,
};
use crate::distributed_planner::{
    NetworkBoundaryBuilderResult, inject_network_boundaries, network_boundary_inject_sampler,
};
use crate::stage::{LocalStage, RemoteStage};
use crate::worker::generated::worker as pb;
use crate::{
    DistributedCodec, NetworkBoundary, NetworkBoundaryExt, NetworkCoalesceExec, Stage,
    TaskCountAnnotation, get_distributed_worker_resolver,
};
use dashmap::DashMap;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::{Stream, StreamExt};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

pub(super) async fn prepare_dynamic_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    metrics: &ExecutionPlanMetricsSet,
    task_metrics: &Option<Arc<MetricsStore>>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedPlan> {
    let metrics = CoordinatorToWorkerMetrics::new(metrics);

    let worker_idx = AtomicUsize::new(rand::rng().random_range(0..100)); // TODO
    let plans_for_viz = PlanReconstructor::default();
    let outer_join_set = Mutex::new(JoinSet::new());

    let head_stage = inject_network_boundaries(
        Arc::clone(base_plan),
        |nb: Arc<dyn NetworkBoundary>, _cfg: &ConfigOptions| {
            let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;
            let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
            let mut join_set = JoinSet::new();
            let Stage::Local(input_stage) = nb.input_stage() else {
                return exec_err!("NetworkBoundary's input stage was in remote mode.");
            };
            let mut input_stage = input_stage.clone();
            input_stage.plan = network_boundary_inject_sampler(input_stage.plan)?;
            let mut spawner = CoordinatorToWorkerTaskSpawner::new(
                &input_stage,
                &metrics,
                task_metrics,
                &codec,
                &mut join_set,
            )?;

            let urls = worker_resolver.get_urls()?;
            let next_url = || urls[(worker_idx.fetch_add(1, SeqCst)) % urls.len()].clone();

            let mut workers = Vec::with_capacity(input_stage.tasks);
            let mut load_info_rxs = Vec::with_capacity(input_stage.tasks);

            let mut url = if input_stage.tasks == 1 {
                get_child_stages_urls(&input_stage.plan)?
                    .iter()
                    .find_map(|v| match v.len() == 1 {
                        true => Some(v.first().cloned()),
                        false => None,
                    })
                    .flatten()
                    .unwrap_or_else(next_url)
            } else {
                next_url()
            };

            for i in 0..input_stage.tasks {
                workers.push(url.clone());
                // Spawns the task that feeds this subplan to this worker. There will be as
                // many as this spawned tasks as workers.
                let (tx, worker_rx) = spawner.send_plan_task(Arc::clone(ctx), i, url)?;
                load_info_rxs.push({
                    let rx = spawner.load_info_and_metrics_collection_task(i, worker_rx);
                    // Tag each LoadInfoBatch with the producer task index so
                    // `calculate_task_count` can identify (task_idx, partition) slices
                    // independently — `select_all` would otherwise collapse them.
                    UnboundedReceiverStream::new(rx).map(move |batch| (i, batch))
                });
                spawner.work_unit_feed_task(Arc::clone(ctx), i, tx)?;
                url = next_url();
            }

            outer_join_set
                .lock()
                .expect("poisoned lock")
                .spawn(async move {
                    for result in join_set.join_all().await {
                        result?;
                    }
                    Ok(())
                });

            plans_for_viz.insert(input_stage.num, Arc::clone(&input_stage.plan));

            let nb = nb.with_input_stage(Stage::Remote(RemoteStage {
                query_id: input_stage.query_id,
                num: input_stage.num,
                workers,
            }))?;

            let load_info_stream = futures::stream::select_all(load_info_rxs);

            Ok(async move {
                let task_count_above = if nb.as_any().is::<NetworkCoalesceExec>() {
                    TaskCountAnnotation::Maximum(1)
                } else {
                    TaskCountAnnotation::Desired(calculate_task_count(load_info_stream).await)
                };
                Ok(NetworkBoundaryBuilderResult {
                    task_count_above,
                    network_boundary: nb,
                })
            })
        },
        ctx.session_config().options(),
    )
    .await?;
    Ok(PreparedPlan {
        final_plan: plans_for_viz.reconstruct(&head_stage)?,
        head_stage,
        join_set: std::mem::take(&mut outer_join_set.lock().unwrap()),
    })
}

#[derive(Default)]
struct PlanReconstructor {
    stage_map: DashMap<usize, Arc<dyn ExecutionPlan>>,
}

impl PlanReconstructor {
    fn insert(&self, stage: usize, plan: Arc<dyn ExecutionPlan>) {
        self.stage_map.insert(stage, plan);
    }

    fn reconstruct(&self, head_stage: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let reconstructed = Arc::clone(head_stage).transform_down(|plan| {
            let Some(nb) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };
            let input_stage = nb.input_stage();
            let Some(plan_for_viz) = self.stage_map.get(&input_stage.num()) else {
                return exec_err!(
                    "Failed to retrieve plan for stage {} for visualization purposes",
                    input_stage.num()
                );
            };

            let nb = nb.with_input_stage(Stage::Local(LocalStage {
                query_id: input_stage.query_id(),
                num: input_stage.num(),
                plan: Arc::clone(&plan_for_viz),
                tasks: input_stage.task_count(),
            }))?;

            Ok(Transformed::yes(nb))
        })?;
        Ok(reconstructed.data)
    }
}

fn get_child_stages_urls(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec</* stage */ &Vec</* worker */ Url>>> {
    let mut result = vec![];
    plan.apply(|plan| {
        let Some(nb) = plan.as_network_boundary() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        match nb.input_stage() {
            Stage::Local(_) => exec_err!("While gathering child stages URLs, one was in local mode. This is a bug in the dynamic task count execution logic, please report it.")?,
            Stage::Remote(remote) => result.push(&remote.workers)
        }

        Ok(TreeNodeRecursion::Jump)
    })?;

    Ok(result)
}

/// Estimates the next stage's task count from per-(task, partition) sampler observations.
///
/// Each producer `(task_idx, partition)` is treated as an independent slice that observes a
/// fraction of the stage's output. The function samples for [`SAMPLING_WINDOW`] of wall-clock
/// time after the first message arrives, then has each observed slice cast a vote: the task
/// count it would assign to the consumer if every observed slice produced at this slice's
/// observed velocity. The final task count is the **median** of votes — robust to a handful
/// of skewed (very fast or very slow) producers.
///
/// Returns early as soon as every observed slice has emitted a terminating signal
/// (`max_memory_reached` or `eos`); otherwise exits at the sampling deadline. Returns 1 if
/// no slice could compute a usable velocity.
async fn calculate_task_count(
    mut load_info_stream: impl Stream<Item = (usize, pb::LoadInfoBatch)> + Unpin,
) -> usize {
    /// Target sustained throughput per downstream task. The next stage is sized so each
    /// task is expected to absorb roughly this many bytes per second of producer output.
    const TARGET_BYTES_PER_SEC_PER_TASK: u64 = 64 * 1024 * 1024;
    /// Per-`(task_idx, partition)` cap on buffered `LoadInfo` messages. Once a slice has
    /// produced this many messages it is considered done — enough information has been seen
    /// to estimate its velocity.
    const MAX_MESSAGES_PER_SLICE: usize = 2;
    /// Minimum number of slices that must reach the "done" state before voting. A slice is
    /// done when it signals `eos`, `max_memory_reached`, or hits `MAX_MESSAGES_PER_SLICE`.
    const TARGET_DONE_SLICES: usize = 2;
    /// Wall-clock safety net measured from the first received `LoadInfo`. If neither
    /// `TARGET_DONE_SLICES` nor full slice coverage is reached within this window, vote with
    /// whatever has been observed. Prevents deadlock if a stage has fewer slices than
    /// `TARGET_DONE_SLICES` but new slices are still appearing slowly.
    const SAMPLING_WINDOW: Duration = Duration::from_millis(25);

    #[derive(Default)]
    struct Slice {
        total_bytes: u64,
        max_elapsed_ns: u64,
        msg_count: usize,
        done: bool,
    }
    let mut slices: HashMap<(usize, u64), Slice> = HashMap::new();
    let mut done_count: usize = 0;
    let mut deadline: Option<Instant> = None;

    loop {
        let next = match deadline {
            None => load_info_stream.next().await,
            Some(d) => match tokio::time::timeout_at(d.into(), load_info_stream.next()).await {
                Ok(item) => item,
                Err(_) => break, // sampling window elapsed
            },
        };
        let Some((task_idx, batch)) = next else { break }; // stream terminated

        for info in batch.batch {
            let entry = slices.entry((task_idx, info.partition)).or_default();
            entry.total_bytes = entry.total_bytes.saturating_add(info.byte_size);
            entry.max_elapsed_ns = entry.max_elapsed_ns.max(info.time_mark_ns);
            entry.msg_count += 1;
            if !entry.done
                && (info.eos
                    || info.max_memory_reached
                    || entry.msg_count >= MAX_MESSAGES_PER_SLICE)
            {
                entry.done = true;
                done_count += 1;
            }
        }
        if deadline.is_none() && !slices.is_empty() {
            deadline = Some(Instant::now() + SAMPLING_WINDOW);
        }
        if done_count >= TARGET_DONE_SLICES {
            break;
        }
    }

    // Each slice that observed enough data votes for a task count. The vote extrapolates the
    // slice's observed velocity to the full producer (assumes all observed slices share the
    // same velocity):
    //   slice_velocity   = total_bytes / max_elapsed_ns                  (bytes/ns)
    //   stage_throughput = slice_velocity * num_slices_observed * 1e9    (bytes/sec)
    //   vote             = ceil(stage_throughput / TARGET_BYTES_PER_SEC_PER_TASK)
    let observed = slices.len().max(1) as u128;
    let mut votes: Vec<u128> = slices
        .values()
        .filter_map(|s| {
            if s.max_elapsed_ns == 0 || s.total_bytes == 0 {
                return None;
            }
            let numerator = (s.total_bytes as u128)
                .saturating_mul(1_000_000_000)
                .saturating_mul(observed);
            let denominator =
                (s.max_elapsed_ns as u128).saturating_mul(TARGET_BYTES_PER_SEC_PER_TASK as u128);
            Some(numerator.div_ceil(denominator).max(1))
        })
        .collect();

    // Floor at the number of distinct producer task_idxs observed: never shrink the consumer
    // stage below the producer's parallelism. Mirrors the static `CardinalityTaskCountStrategy`
    // behavior where a consumer at least matches its producer.
    let producer_task_floor = slices
        .keys()
        .map(|(t, _)| *t)
        .collect::<std::collections::HashSet<_>>()
        .len() as u128;

    if votes.is_empty() {
        return producer_task_floor.max(1) as usize;
    }

    votes.sort_unstable();
    let median = votes[votes.len() / 2].max(producer_task_floor);
    usize::try_from(median).unwrap_or(usize::MAX)
}
