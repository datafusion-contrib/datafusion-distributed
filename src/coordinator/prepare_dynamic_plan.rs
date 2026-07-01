use crate::TaskCountAnnotation::{Desired, Maximum};
use crate::common::{TreeNodeExt, element_wise_sum, vec_avg_reduce, vec_div, vec_mul};
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::distributed_planner::{
    InjectNetworkBoundaryContext, NetworkBoundaryBuilderResult, ProducerHead, calculate_cost,
    inject_network_boundaries,
};
use crate::execution_plans::SamplerExec;
use crate::stage::{LocalStage, RemoteStage};
use crate::worker::generated::worker as pb;
use crate::{BytesCounterMetric, NetworkBoundaryExt, NetworkCoalesceExec, Stage};
use dashmap::DashMap;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    ColumnStatistics, ExecutionPlan, ExecutionPlanProperties, Statistics,
};
use futures::{Stream, StreamExt};
use std::any::TypeId;
use std::sync::Arc;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(super) async fn prepare_dynamic_plan(
    query_coordinator: &QueryCoordinator,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    let plans_for_viz = Arc::new(PlanReconstructor::default());

    let head_stage = inject_network_boundaries(
        Arc::clone(base_plan),
        |mut input_stage: LocalStage, nb_type: TypeId, nb_ctx: &InjectNetworkBoundaryContext| {
            let mut metrics = MetricsSet::new();

            // At this point, input_stage.plan has two kind of leaf nodes:
            // - The ones that naturally do not read from any children, like DataSourceExec
            // - Network boundaries whose Stage was set to Stage::Remote by a previous iteration
            //   of this same function.
            // Both types of leaf nodes contain very valuable and accurate statistics that are used
            // here for computing an estimation of the compute cost (measured in bytes):
            // - DataSourceExec (or natural leaf nodes) contain stats pulled directly from their
            //   data source, like parquet files.
            // - Network boundaries contain statistics collected from runtime information, gathered
            //   by the SamplerExec injected by this same function.
            let cost = calculate_cost(&input_stage.plan)?;
            metrics.push(BytesCounterMetric::new_metric(
                "cpu_cost",
                *cost.cpu.get_value().unwrap_or(&0),
            ));
            metrics.push(BytesCounterMetric::new_metric(
                "memory_cost",
                *cost.memory.get_value().unwrap_or(&0),
            ));
            metrics.push(BytesCounterMetric::new_metric(
                "network_cost",
                *cost.network.get_value().unwrap_or(&0),
            ));
            let compute_based_task_count = cost
                .cpu
                .get_value()
                .unwrap_or(&0)
                .div_ceil(nb_ctx.d_cfg.bytes_per_partition_per_second.max(1))
                .div_ceil(input_stage.plan.output_partitioning().partition_count())
                .clamp(1, nb_ctx.max_tasks()?);
            let task_count = nb_ctx
                .task_count(&input_stage.plan)?
                .merge(Desired(compute_based_task_count));

            // Propagate the final task_count inferred based on runtime statistics and compute cost.
            // Here is where leaf nodes are scaled up by TaskEstimator::scale_up_leaf_node, and the
            // plan is finally left ready for distribution.
            input_stage.plan = nb_ctx
                .propagate_task_count_until_network_boundaries(&input_stage.plan, task_count)?;
            input_stage.tasks = task_count.as_usize();
            // In order to infer the compute the cost of the stage above this one, here a sampler
            // is injected to gather runtime statistics.
            input_stage.plan = ProducerHead::insert_sampler(input_stage.plan)?;

            let mut stage_coordinator = query_coordinator.stage_coordinator(&input_stage);

            let mut workers = Vec::with_capacity(input_stage.tasks);
            let mut load_info_rxs = Vec::with_capacity(input_stage.tasks);

            let routed_urls = if input_stage.tasks == 1 {
                // If there's an input stage with a single worker, and the current stage is also
                // going to run in a single worker, we want to co-locate them so that unnecessary
                // network transfers are avoided.
                match stage_coordinator.find_input_stage_with_single_url() {
                    Some(single_url) => vec![single_url],
                    None => stage_coordinator.routed_urls()?,
                }
            } else {
                stage_coordinator.routed_urls()?
            };

            for (i, routed_url) in routed_urls.into_iter().enumerate() {
                workers.push(routed_url.clone());
                // Spawns the task that feeds this subplan to this worker. There will be as
                // many as this spawned tasks as workers.
                let (worker_tx, worker_rx) = stage_coordinator.send_plan_task(i, routed_url)?;
                load_info_rxs.push({
                    let rx = stage_coordinator.worker_to_coordinator_task(i, worker_rx);
                    UnboundedReceiverStream::new(rx)
                });
                stage_coordinator.coordinator_to_worker_task(i, worker_tx)?;
            }

            let plans_for_viz = Arc::clone(&plans_for_viz);
            Ok(async move {
                let (stats, consumer_tc) = if nb_type == TypeId::of::<NetworkCoalesceExec>() {
                    (None, Maximum(1))
                } else {
                    let stats = gather_runtime_statistics(load_info_rxs, &input_stage.plan).await?;
                    let sampled_bytes = *stats.total_byte_size.get_value().unwrap_or(&0);
                    metrics.push(BytesCounterMetric::new_metric(
                        "sampled_bytes",
                        sampled_bytes,
                    ));
                    // returning Desired(1) here is our way to tell the planner that we don't care
                    // about the task count assigned to the network boundary in the consumer stage,
                    // and we don't want it to affect other task count decisions.
                    (Some(Arc::new(stats)), Desired(1))
                };

                // Capture the output partitioning of the (rescaled, sampler-wrapped) input plan
                // before it's moved: the returned stage is remote and carries no plan to read it
                // back from.
                let input_properties = Arc::clone(input_stage.plan.properties());
                plans_for_viz.insert(input_stage.num, input_stage.plan, metrics);
                Ok(NetworkBoundaryBuilderResult {
                    consumer_task_count: consumer_tc,
                    input_stage: Stage::Remote(RemoteStage {
                        query_id: input_stage.query_id,
                        num: input_stage.num,
                        workers,
                        runtime_stats: stats,
                    }),
                    input_properties,
                })
            })
        },
        query_coordinator.session_config().options(),
    )
    .await?;

    Ok(PreparedPlan {
        plan_for_viz: plans_for_viz.reconstruct(&head_stage)?,
        head_stage,
    })
}

/// Reconstructs the plan dynamically as stages get transitioned to Remote and get sent to the
/// respective workers.
///
/// As the [prepare_dynamic_plan] function recurses and progressively sends the plan to workers, the
/// original plan gets modified, and subplans belong to the different [Stage]s get lost as they get
/// transitioned to [Stage::Remote].
///
/// This struct is in charge of tracking the [prepare_dynamic_plan] process and storing the final
/// version of all the subplans so that it can be reconstructed into a fully blown plan for
/// visualization purposes.
#[derive(Default)]
struct PlanReconstructor {
    stage_map: DashMap<usize, (Arc<dyn ExecutionPlan>, MetricsSet)>,
}

impl PlanReconstructor {
    fn insert(&self, stage: usize, plan: Arc<dyn ExecutionPlan>, metrics_set: MetricsSet) {
        self.stage_map.insert(stage, (plan, metrics_set));
    }

    fn reconstruct(&self, head_stage: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let head_stage = Arc::clone(head_stage);
        let reconstructed = head_stage.transform_down_with_task_count(1, |plan, tc| {
            let Some(nb) = plan.as_network_boundary() else {
                return Ok(Transformed::no(plan));
            };
            let input_stage = nb.input_stage();
            let Some((_, entry)) = self.stage_map.remove(&input_stage.num()) else {
                return exec_err!(
                    "Failed to retrieve plan for stage {} for visualization purposes",
                    input_stage.num()
                );
            };
            let (plan_for_viz, metrics_set) = entry;

            let plan_for_viz = nb.producer_head(tc).insert(plan_for_viz)?;

            let nb = nb.with_input_stage(Stage::Local(LocalStage {
                query_id: input_stage.query_id(),
                num: input_stage.num(),
                plan: plan_for_viz,
                tasks: input_stage.task_count(),
                metrics_set,
            }))?;

            Ok(Transformed::yes(nb))
        })?;
        Ok(reconstructed.data)
    }
}

/// Estimates the bytes per second flowing through a stage by reading sample information.
async fn gather_runtime_statistics(
    per_task_load_info_stream: Vec<impl Stream<Item = pb::LoadInfo> + Unpin>,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Statistics> {
    const ESTIMATED_QUERY_TIME_S: usize = 10;
    const BYTES_READY_SAMPLE_PERCENTAGE: f32 = 0.2;
    const BYTES_PER_SECOND_SAMPLE_PERCENTAGE: f32 = 0.2;

    let Some(sampler) = find_sampler(plan) else {
        return plan_err!("Mising SamplerExec while gathering load report");
    };
    let n_cols = sampler.schema().fields.len();

    fn apply_pct(value: usize, pct: f32) -> usize {
        (value as f32 * pct).round() as usize
    }

    let partitions_per_task = sampler.partition_samplers.len();
    let task_count = per_task_load_info_stream.len();
    let total_partitions = partitions_per_task * task_count;

    let mut partitions_with_bytes_per_second_done = 0;
    let mut partitions_with_bytes_ready_done = 0;
    let mut partitions_done = 0;
    let mut rows_ready = 0;
    let mut rows_per_second = 0;
    let mut per_col_bytes_ready = vec![0usize; n_cols];
    let mut per_col_bytes_per_second = vec![0usize; n_cols];

    let mut ndv_pct = vec![];
    let mut null_pct = vec![];

    let mut load_info_stream = futures::stream::select_all(per_task_load_info_stream);
    while let Some(load_info) = load_info_stream.next().await {
        rows_per_second += load_info.rows_per_second as usize;
        rows_ready += load_info.rows_ready as usize;
        per_col_bytes_per_second = element_wise_sum(
            per_col_bytes_per_second,
            &load_info.per_column_bytes_per_second,
        )?;
        per_col_bytes_ready =
            element_wise_sum(per_col_bytes_ready, &load_info.per_column_bytes_ready)?;
        ndv_pct.push(load_info.per_column_ndv_percentage);
        null_pct.push(load_info.per_column_null_percentage);

        partitions_with_bytes_per_second_done +=
            load_info.per_column_bytes_per_second.iter().any(|v| *v > 0) as usize;
        partitions_with_bytes_ready_done +=
            load_info.per_column_bytes_ready.iter().any(|v| *v > 0) as usize;
        partitions_done += 1;

        // Short circuit if we collected enough bytes_ready measurements.
        if partitions_with_bytes_ready_done
            >= apply_pct(total_partitions, BYTES_READY_SAMPLE_PERCENTAGE).max(1)
        {
            break;
        }

        // Short circuit if we collected enough bytes_per_second measurements.
        if partitions_with_bytes_per_second_done
            >= apply_pct(total_partitions, BYTES_PER_SECOND_SAMPLE_PERCENTAGE).max(1)
        {
            break;
        }

        // Short circuit if there are no further partitions remaining to sample from.
        if partitions_done == total_partitions {
            break;
        }
    }

    if partitions_done == 0 {
        return Ok(zero_stats(plan.schema().fields.len()));
    }

    let per_col_bytes_ready = vec_div(
        vec_mul(per_col_bytes_ready, total_partitions),
        partitions_done,
    );
    let per_col_bytes_per_second = vec_div(
        vec_mul(per_col_bytes_per_second, total_partitions),
        partitions_done,
    );

    let rows_ready = rows_ready * total_partitions / partitions_done;
    let rows_per_second = rows_per_second * total_partitions / partitions_done;

    let total_num_rows = rows_ready + rows_per_second * ESTIMATED_QUERY_TIME_S;

    if total_num_rows == 0 {
        return Ok(zero_stats(n_cols));
    }

    let per_col_byte_size = element_wise_sum(
        per_col_bytes_ready,
        &vec_mul(per_col_bytes_per_second, ESTIMATED_QUERY_TIME_S),
    )?;
    let total_byte_size: usize = per_col_byte_size.iter().sum();

    let ndv_pct = vec_avg_reduce(ndv_pct)?;
    if ndv_pct.len() != n_cols {
        return plan_err!("Expected {n_cols} ndv values, but got {}", ndv_pct.len());
    }
    let null_pct = vec_avg_reduce(null_pct)?;
    if null_pct.len() != n_cols {
        return plan_err!("Expected {n_cols} null values, but got {}", null_pct.len());
    }

    Ok(Statistics {
        num_rows: Precision::Inexact(total_num_rows),
        total_byte_size: Precision::Inexact(total_byte_size),
        column_statistics: ndv_pct
            .into_iter()
            .zip(null_pct)
            .zip(per_col_byte_size)
            .map(|((ndv, null), col_bytes)| ColumnStatistics {
                null_count: Precision::Inexact((null * total_num_rows as f32) as usize),
                distinct_count: Precision::Inexact((ndv * total_num_rows as f32) as usize),
                byte_size: Precision::Inexact(col_bytes),
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect(),
    })
}

fn find_sampler(plan: &Arc<dyn ExecutionPlan>) -> Option<&SamplerExec> {
    let mut sampler = None;
    plan.apply(|plan| {
        if let Some(node) = plan.downcast_ref::<SamplerExec>() {
            sampler = Some(node);
            return Ok(TreeNodeRecursion::Stop);
        };
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Cannot fail");
    sampler
}

fn zero_stats(n_cols: usize) -> Statistics {
    Statistics {
        num_rows: Precision::Exact(0),
        total_byte_size: Precision::Exact(0),
        column_statistics: (0..n_cols)
            .map(|_| ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Exact(0),
                byte_size: Precision::Exact(0),
            })
            .collect(),
    }
}
