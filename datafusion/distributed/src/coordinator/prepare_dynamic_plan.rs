use crate::common::{TreeNodeExt, element_wise_sum, vec_avg_reduce, vec_div, vec_mul};
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::distributed_planner::{
    InjectNetworkBoundaryContext, NetworkBoundaryBuilder, NetworkBoundaryBuilderResult,
    ProducerHead, calculate_cost, inject_network_boundaries, is_left_broadcast_safe,
};
use crate::dynamic_filtering::{
    is_remote_dynamic_filtering_enabled, orphan_dynamic_filter_consumers,
};
use crate::execution_plans::SamplerExec;
use crate::stage::{LocalStage, RemoteStage};
use crate::{
    BroadcastExec, BytesCounterMetric, CoordinatorToWorkerMsg, DistributedConfig, LoadInfo,
    MaxGaugeMetric, NetworkBoundaryExt, NetworkBroadcastExec, NetworkCoalesceExec,
    NetworkShuffleExec, Stage, TaskCountAnnotation,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ColumnStatistics, ExecutionPlan, ExecutionPlanProperties,
    ReplaceChildrenOptions, Statistics, StatisticsArgs, StatisticsContext,
};
use futures::{Stream, StreamExt};
use std::any::TypeId;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(super) async fn prepare_dynamic_plan(
    query_coordinator: &Arc<QueryCoordinator>,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    let plans_for_viz = PlanReconstructor::default();

    let head_stage = inject_network_boundaries(
        Arc::clone(base_plan),
        optimize_remainder,
        DynamicPlanBuilder {
            query_coordinator,
            plans_for_viz: &plans_for_viz,
        },
        query_coordinator.session_config(),
    )
    .await?;

    Ok(PreparedPlan {
        plan_for_viz: plans_for_viz.reconstruct(&head_stage)?,
        head_stage,
    })
}

struct DynamicPlanBuilder<'a> {
    query_coordinator: &'a Arc<QueryCoordinator>,
    plans_for_viz: &'a PlanReconstructor,
}

#[async_trait]
impl NetworkBoundaryBuilder for DynamicPlanBuilder<'_> {
    async fn build<'a>(
        &'a self,
        mut input_stage: LocalStage,
        nb_type: TypeId,
        nb_ctx: &'a InjectNetworkBoundaryContext<'a>,
    ) -> Result<NetworkBoundaryBuilderResult> {
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
        let compute_based_task_count = *cost.cpu.get_value().unwrap_or(&0) as f64
            / nb_ctx.d_cfg.dynamic_bytes_per_partition.max(1) as f64
            / input_stage
                .plan
                .output_partitioning()
                .partition_count()
                .max(1) as f64;
        let compute_based_task_count = compute_based_task_count.min(nb_ctx.max_tasks()? as f64);
        let task_count = nb_ctx
            .task_count(&input_stage.plan)?
            .merge(TaskCountAnnotation::soft(compute_based_task_count))?;

        // Propagate the final task_count inferred based on runtime statistics and compute cost.
        // Here is where leaf nodes are scaled up by ScaleUpLeafNodeHandler, and the
        // plan is finally left ready for distribution.
        input_stage.plan = nb_ctx
            .propagate_task_count_until_network_boundaries(&input_stage.plan, task_count)?
            .data;
        input_stage.tasks = task_count.as_usize();
        // In order to infer the compute the cost of the stage above this one, here a sampler
        // is injected to gather runtime statistics.
        input_stage.plan = ProducerHead::insert_sampler(input_stage.plan)?;
        let dynamic_filter_anchors =
            if is_remote_dynamic_filtering_enabled(self.query_coordinator.session_config())? {
                orphan_dynamic_filter_consumers(&input_stage.plan)?
            } else {
                vec![]
            };

        let mut load_info_rxs = Vec::with_capacity(input_stage.tasks);

        let mut stage_coordinator = self.query_coordinator.stage_coordinator(&input_stage);

        let mut futures = Vec::with_capacity(input_stage.tasks);
        for task_i in 0..input_stage.tasks {
            futures.push(stage_coordinator.init_bidirectional_stream(task_i));
        }
        let results = futures::future::try_join_all(futures).await?;

        let mut workers = Vec::with_capacity(input_stage.tasks);
        for (task_i, (url, worker_tx, worker_rx_stream)) in results.into_iter().enumerate() {
            workers.push(url);
            load_info_rxs.push({
                let rx = stage_coordinator.worker_to_coordinator_task(task_i, worker_rx_stream);
                UnboundedReceiverStream::new(rx)
            });
            let _ = worker_tx.send(CoordinatorToWorkerMsg::KickOffSampling);
            stage_coordinator.coordinator_to_worker_task(task_i, worker_tx)?;
        }
        stage_coordinator.seal_dynamic_filter_stage()?;

        let (stats, consumer_tc) = if nb_type == TypeId::of::<NetworkCoalesceExec>() {
            (None, TaskCountAnnotation::exact(NonZeroUsize::MIN, 0.0))
        } else {
            let (stats, new_metrics) = gather_runtime_statistics(
                load_info_rxs,
                &input_stage.plan,
                nb_ctx.d_cfg.dynamic_bytes_ready_sample_percentage,
            )
            .await?;
            metrics.extend(new_metrics);
            // returning Desired(0) here is our way to tell the planner that we don't care
            // about the task count assigned to the network boundary in the consumer stage,
            // and we don't want it to affect other task count decisions. A 0.0 hint does
            // not mask smaller fractional values via max-merge, and does not inflate
            // UNION sums when this boundary sits under a child of a
            // ChildrenIsolatorUnionExec.
            (Some(Arc::new(stats)), TaskCountAnnotation::soft(0.0))
        };

        // Capture the output partitioning of the (rescaled, sampler-wrapped) input plan
        // before it's moved: the returned stage is remote and carries no plan to read it
        // back from.
        let input_properties = Arc::clone(input_stage.plan.properties());
        self.plans_for_viz
            .insert(input_stage.num, input_stage.plan, metrics);
        Ok(NetworkBoundaryBuilderResult {
            consumer_task_count: consumer_tc,
            input_stage: Stage::Remote(RemoteStage {
                query_id: input_stage.query_id,
                num: input_stage.num,
                workers,
                runtime_stats: stats,
                dynamic_filter_anchors,
            }),
            input_properties,
        })
    }
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

            let plan_for_viz = nb.producer_head(tc)?.insert(plan_for_viz)?;

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
    per_task_load_info_stream: Vec<impl Stream<Item = LoadInfo> + Unpin>,
    plan: &Arc<dyn ExecutionPlan>,
    // Fraction of partitions with non-zero sampled bytes that need to report before considering
    // that there's enough samples collected to make a final decision.
    bytes_ready_sample_percentage: f64,
) -> Result<(Statistics, MetricsSet)> {
    /// If the leaf nodes in the provided `plan` do not provide any row count estimation, there's
    /// no way for us here to estimate the % of completion of the stage during sampling. This is
    /// the default % of completion for those cases.
    const FALLBACK_PCT_COMPLETION: f32 = 0.2;

    let mut new_metrics = MetricsSet::new();
    let Some(sampler) = find_sampler(plan) else {
        return plan_err!("Missing SamplerExec while gathering load report");
    };
    let n_cols = sampler.schema().fields.len();

    fn apply_pct(value: usize, pct: f64) -> usize {
        (value as f64 * pct).round() as usize
    }

    let partitions_per_task = sampler.partition_samplers.len();
    let task_count = per_task_load_info_stream.len();
    let total_partitions = partitions_per_task * task_count;

    let mut partitions_with_bytes_ready_done = 0;
    let mut partitions_done = 0;
    let mut partitions_reached_eos = 0;
    let mut rows_ready = 0;
    let mut rows_pulled_from_leaves = 0;
    let mut per_col_bytes_ready = vec![0usize; n_cols];

    let mut ndv_pct = vec![];
    let mut null_pct = vec![];

    let mut load_info_stream = futures::stream::select_all(per_task_load_info_stream);
    while let Some(load_info) = load_info_stream.next().await {
        rows_ready += load_info.rows_ready;
        rows_pulled_from_leaves += load_info.rows_pulled_from_leaf;
        per_col_bytes_ready =
            element_wise_sum(per_col_bytes_ready, &load_info.per_column_bytes_ready)?;
        ndv_pct.push(load_info.per_column_ndv_percentage);
        null_pct.push(load_info.per_column_null_percentage);

        partitions_with_bytes_ready_done +=
            load_info.per_column_bytes_ready.iter().any(|v| *v > 0) as usize;
        partitions_reached_eos += load_info.reached_eos as usize;
        partitions_done += 1;

        // Short circuit if we collected enough bytes_ready measurements.
        if partitions_with_bytes_ready_done
            >= apply_pct(total_partitions, bytes_ready_sample_percentage).max(1)
        {
            break;
        }

        // Short circuit if there are no further partitions remaining to sample from.
        if partitions_done == total_partitions {
            break;
        }
    }

    if partitions_done == 0 {
        return Ok((zero_stats(plan.schema().fields.len()), new_metrics));
    }

    let per_col_bytes_ready = vec_div(
        vec_mul(per_col_bytes_ready, total_partitions),
        partitions_done,
    );
    let rows_ready = rows_ready * total_partitions / partitions_done;
    let rows_pulled_from_leaves = rows_pulled_from_leaves * total_partitions / partitions_done;

    let estimated_pct_sampled = if partitions_reached_eos == partitions_done {
        // Every sampled partition's stream reached end-of-stream, so `rows_ready` /
        // `per_col_bytes_ready` are the partitions' final output rather than a partial snapshot —
        // the stage is fully sampled. This is the reliable "done" signal, and it correctly covers
        // legitimately-empty stages (which would otherwise report `rows_pulled_from_leaves == 0` and
        // make the completion fraction 0, blowing up the `ready / fraction` extrapolation below).
        1.0
    } else if let Some(estimated_driver_path_leaf_rows) = estimated_driver_path_leaf_rows(plan)
        && rows_pulled_from_leaves > 0
    {
        // The stage is still producing. Estimate how far along it is from the fraction of the
        // driver-path leaf rows consumed so far.
        (rows_pulled_from_leaves as f32 / estimated_driver_path_leaf_rows.max(1) as f32).min(1.0)
    } else {
        // We can't measure progress (no leaf-row estimate, or nothing pulled from the leaves
        // yet even though we're not at EOS): fall back rather than dividing by ~0.
        FALLBACK_PCT_COMPLETION
    };

    new_metrics.push(MaxGaugeMetric::new_metric(
        "estimated_pct_sampled",
        (estimated_pct_sampled * 100.) as usize,
    ));

    let total_num_rows = (rows_ready as f32 / estimated_pct_sampled) as usize;

    if total_num_rows == 0 {
        return Ok((zero_stats(n_cols), new_metrics));
    }

    let per_col_byte_size = vec_mul(per_col_bytes_ready, 1. / estimated_pct_sampled);
    let total_byte_size: usize = per_col_byte_size.iter().sum();

    new_metrics.push(BytesCounterMetric::new_metric(
        "estimated_output_bytes",
        total_byte_size,
    ));

    let ndv_pct = vec_avg_reduce(ndv_pct)?;
    if ndv_pct.len() != n_cols {
        return plan_err!("Expected {n_cols} ndv values, but got {}", ndv_pct.len());
    }
    let null_pct = vec_avg_reduce(null_pct)?;
    if null_pct.len() != n_cols {
        return plan_err!("Expected {n_cols} null values, but got {}", null_pct.len());
    }

    let stats = Statistics {
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
    };

    Ok((stats, new_metrics))
}

fn estimated_driver_path_leaf_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let mut total_rows = None;
    let _ = plan.apply_driver_path(|plan| {
        if plan.children().is_empty() {
            let stats = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
            if let Some(num_rows) = stats.num_rows.get_value() {
                if let Some(total_rows) = &mut total_rows {
                    *total_rows += *num_rows;
                } else {
                    total_rows = Some(*num_rows);
                };
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    total_rows
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

fn optimize_remainder(
    mut plan: Arc<dyn ExecutionPlan>,
    nb_ctx: &InjectNetworkBoundaryContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = nb_ctx.cfg.options();
    let d_cfg = DistributedConfig::from_config_options(config)?;
    if !d_cfg.dynamic_plan_remainder_replanning {
        return Ok(plan);
    }
    plan = OutputRequirements::new_add_mode().optimize(plan, config)?;
    plan = reorder_joins(Arc::clone(&plan), nb_ctx)?;
    plan = EnsureRequirements::new().optimize(plan, config)?;
    plan = CombinePartialFinalAggregate::new().optimize(plan, config)?;
    plan = OptimizeAggregateOrder::new().optimize(plan, config)?;
    plan = OutputRequirements::new_remove_mode().optimize(plan, config)?;
    if plan.output_partitioning().partition_count() > 1 {
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }

    Ok(plan)
}

fn reorder_joins(
    plan: Arc<dyn ExecutionPlan>,
    nb_ctx: &InjectNetworkBoundaryContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let cfg = nb_ctx.cfg.options();
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    plan.transform_up(|plan| {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            if join.null_aware {
                return Ok(Transformed::no(plan));
            }

            let supports_swap = cfg.optimizer.join_reordering
                && !join.null_aware
                && join.dynamic_expressions_produced().is_empty()
                && join.join_type.supports_swap();

            let should_swap =
                supports_swap && should_swap(join.left.as_ref(), join.right.as_ref())?;

            // The build side is the left input, which after a swap is the current right input.
            let (candidate_build, join_type_after_swap) = match should_swap {
                true => (join.right(), join.join_type().swap()),
                false => (join.left(), *join.join_type()),
            };

            let supports_broadcast = cfg.optimizer.join_reordering
                && !join.null_aware
                && d_cfg.broadcast_joins
                && is_left_broadcast_safe(&join_type_after_swap);
            let should_broadcast =
                supports_broadcast && should_broadcast(candidate_build.as_ref(), cfg)?;

            match join.partition_mode() {
                PartitionMode::Partitioned if should_swap && should_broadcast => {
                    // Partitioned -> Swapped Broadcasted
                    //
                    // - The probe side needs to go to the build side, and it needs to go from shuffle to broadcast.
                    // - The build side needs to go to the probe side and:
                    //   - If it was a hash shuffle, it needs to go to a round-robin shuffle
                    //   - If everything below was already co-partitioned, it can stay like that
                    let join_or_proj = join.swap_inputs(PartitionMode::CollectLeft)?;
                    let join_or_proj = join_or_proj.transform_down(|plan| {
                        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                            return Ok(Transformed::no(plan));
                        };

                        // New probe side (old build side)
                        let mut right = Arc::clone(join.right());

                        if let Some(shuffle) = right.downcast_ref::<NetworkShuffleExec>() {
                            // hash shuffle -> round-robin shuffle
                            let rr_shuffle = Arc::new(shuffle.to_round_robin_shuffle()) as _;
                            nb_ctx.set_task_count(&rr_shuffle, nb_ctx.task_count(&right)?);
                            right = rr_shuffle;
                        } else {
                            // no partitioning -> stay the same, no specific partitioning is required.
                        };

                        // New build side (old probe side)
                        let left = Arc::clone(join.left());
                        let (coalesce, mut left) = left.split_head::<CoalescePartitionsExec>();

                        if let Some(shuffle) = left.downcast_ref::<NetworkShuffleExec>() {
                            // hash-shuffle -> broadcast
                            let broadcast = Arc::new(shuffle.to_broadcast()) as _;
                            nb_ctx.set_task_count(&broadcast, nb_ctx.task_count(&left)?);
                            left = broadcast;
                        } else {
                            // no partitioning -> new BroadcastExec that will eventually be wired to
                            // a NetworkBroadcastExec.
                            left = Arc::new(BroadcastExec::new(left, 1))
                        };

                        left = coalesce.reattach(left)?;

                        let plan = plan.replace_children(
                            vec![left, right],
                            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                        )?;

                        Ok(Transformed::yes(plan))
                    })?;
                    Ok(join_or_proj)
                }
                PartitionMode::Partitioned if should_swap && !should_broadcast => {
                    // Partitioned -> Swapped Partitioned
                    //
                    // The join needs to stay partitioned, but their inputs need to be swapped.
                    // This is easy to do, as both sides already expose the appropriate partitioning.
                    let join_or_proj = join.swap_inputs(PartitionMode::Partitioned)?;
                    Ok(Transformed::yes(join_or_proj))
                }
                PartitionMode::Partitioned if !should_swap && should_broadcast => {
                    // Partitioned -> Broadcasted (no swap)
                    //
                    // - The probe side needs to:
                    //   - If it was a hash shuffle, it needs to go to a round-robin shuffle
                    //   - If everything below was already co-partitioned, it can stay like that
                    // - The build side needs to go from shuffle to broadcast.

                    // Probe side (does not swap)
                    let mut right = Arc::clone(join.right());
                    if let Some(shuffle) = right.downcast_ref::<NetworkShuffleExec>() {
                        // hash shuffle -> round-robin shuffle
                        let rr_shuffle = Arc::new(shuffle.to_round_robin_shuffle()) as _;
                        nb_ctx.set_task_count(&rr_shuffle, nb_ctx.task_count(&right)?);
                        right = rr_shuffle;
                    } else {
                        // no partitioning -> stay the same, no specific partitioning is required.
                    };

                    // Build side (does not swap)
                    let left = Arc::clone(join.left());
                    let (coalesce, mut left) = left.split_head::<CoalescePartitionsExec>();

                    if let Some(shuffle) = left.downcast_ref::<NetworkShuffleExec>() {
                        // hash shuffle -> round-robin shuffle
                        let broadcast = Arc::new(shuffle.to_broadcast()) as _;
                        nb_ctx.set_task_count(&broadcast, nb_ctx.task_count(&left)?);
                        left = broadcast;
                    } else {
                        // no partitioning -> new BroadcastExec that will eventually be wired to
                        // a NetworkBroadcastExec.
                        left = Arc::new(BroadcastExec::new(left, 1))
                    };

                    left = coalesce.reattach(left)?;

                    let join = join
                        .builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .with_new_children(vec![left, right])?
                        .build()?;
                    Ok(Transformed::yes(Arc::new(join)))
                }
                PartitionMode::Partitioned if !should_swap && !should_broadcast => {
                    // Stay partitioned
                    Ok(Transformed::no(plan))
                }
                PartitionMode::CollectLeft if should_swap && should_broadcast => {
                    // Broadcast -> Swapped Broadcast
                    //
                    // - The probe side needs to go to the build side, and we need to inject a new broadcast
                    //   network boundary right there.
                    // - The build side needs to go to the probe side, but it had already started executing in
                    //   broadcast mode, so we need to set it to round-robin shuffle.
                    let join_or_proj = join.swap_inputs(PartitionMode::CollectLeft)?;
                    let join_or_proj = join_or_proj.transform_down(|plan| {
                        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                            return Ok(Transformed::no(plan));
                        };

                        // New probe side (old build side)
                        let right = Arc::clone(join.right());
                        let (_coalesce, mut right) = right.split_head::<CoalescePartitionsExec>();
                        if let Some(broadcast) = right.downcast_ref::<NetworkBroadcastExec>() {
                            // broadcast -> round-robin shuffle
                            let rr_shuffle = Arc::new(broadcast.to_round_robin_shuffle()) as _;
                            nb_ctx.set_task_count(&rr_shuffle, nb_ctx.task_count(&right)?);
                            right = rr_shuffle;
                        } else {
                            // no partitioning -> stay the same, no specific partitioning is required.
                        };

                        // New build side (old probe side)
                        let left = Arc::clone(join.left());
                        let (coalesce, mut left) = left.split_head::<CoalescePartitionsExec>();

                        if let Some(shuffle) = left.downcast_ref::<NetworkShuffleExec>() {
                            // any shuffle -> broadcast
                            let broadcast = Arc::new(shuffle.to_broadcast()) as _;
                            nb_ctx.set_task_count(&broadcast, nb_ctx.task_count(&left)?);
                            left = broadcast;
                        } else {
                            // no partitioning -> new BroadcastExec that will eventually be wired to
                            // a NetworkBroadcastExec.
                            left = Arc::new(BroadcastExec::new(left, 1))
                        };

                        left = coalesce.reattach(left)?;

                        let plan = plan.replace_children(
                            vec![left, right],
                            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                        )?;

                        Ok(Transformed::yes(plan))
                    })?;
                    Ok(join_or_proj)
                }
                PartitionMode::CollectLeft if should_swap && !should_broadcast => {
                    // Broadcast -> Swapped Partitioned
                    //
                    // - The probe side needs to go to the build side, and in needs to move to a hash shuffle.
                    // - The build side needs to go to the probe side, and it needs to swap from broadcast to
                    //   shuffle.
                    let join_or_proj = join.swap_inputs(PartitionMode::Partitioned)?;
                    let join_or_proj = join_or_proj.transform_down(|plan| {
                        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
                            return Ok(Transformed::no(plan));
                        };

                        let target_partitions = cfg.execution.target_partitions;

                        // New probe side (old build side)
                        let right = Arc::clone(join.right());
                        let right_keys = join.on().iter().map(|(_, r)| Arc::clone(r)).collect();
                        let (_coalesce, mut right) = right.split_head::<CoalescePartitionsExec>();
                        if let Some(broadcast) = right.downcast_ref::<NetworkBroadcastExec>() {
                            // broadcast -> hash shuffle
                            let rr_shuffle = Arc::new(broadcast.to_hash_shuffle(right_keys)) as _;
                            nb_ctx.set_task_count(&rr_shuffle, nb_ctx.task_count(&right)?);
                            right = rr_shuffle;
                        } else {
                            // no partitioning -> new RepartitionExec that will eventually be wired
                            // to a NetworkShuffleExec.
                            let partitioning = Partitioning::Hash(right_keys, target_partitions);
                            right = Arc::new(RepartitionExec::try_new(right, partitioning)?);
                        };

                        // New build side (old probe side)
                        let mut left = Arc::clone(join.left());
                        let left_keys = join.on().iter().map(|(l, _)| Arc::clone(l)).collect();
                        if let Some(broadcast) = left.downcast_ref::<NetworkBroadcastExec>() {
                            // broadcast -> hash shuffle
                            let h_shuffle = Arc::new(broadcast.to_hash_shuffle(left_keys)) as _;
                            nb_ctx.set_task_count(&h_shuffle, nb_ctx.task_count(&left)?);
                            left = h_shuffle;
                        } else {
                            // no partitioning -> new RepartitionExec that will eventually be wired
                            // to a NetworkShuffleExec.
                            let partitioning = Partitioning::Hash(left_keys, target_partitions);
                            left = Arc::new(RepartitionExec::try_new(left, partitioning)?);
                        };

                        let plan = plan.replace_children(
                            vec![left, right],
                            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                        )?;

                        Ok(Transformed::yes(plan))
                    })?;
                    Ok(join_or_proj)
                }
                PartitionMode::CollectLeft if !should_swap && should_broadcast => {
                    // Stay broadcasted
                    Ok(Transformed::no(plan))
                }
                PartitionMode::CollectLeft if !should_swap && !should_broadcast => {
                    // Broadcast -> Partitioned (no swap)
                    //
                    // - The probe side needs to move to a hash shuffle, regardless of what it was.
                    // - The build side needs to go from broadcast to shuffle.
                    let target_partitions = cfg.execution.target_partitions;

                    // Probe side (does not swap)
                    let mut right = Arc::clone(join.right());
                    let right_keys = join.on().iter().map(|(_, r)| Arc::clone(r)).collect();
                    if let Some(shuffle) = right.downcast_ref::<NetworkShuffleExec>() {
                        // any shuffle -> hash shuffle (over join on right keys)
                        let h_shuffle = Arc::new(shuffle.to_hash_shuffle(right_keys)) as _;
                        nb_ctx.set_task_count(&h_shuffle, nb_ctx.task_count(&right)?);
                        right = h_shuffle;
                    } else {
                        // no partitioning -> RepartitionExec (over join on left keys) that will
                        // eventually be wired to a NetworkShuffleExec
                        let partitioning = Partitioning::Hash(right_keys, target_partitions);
                        right = Arc::new(RepartitionExec::try_new(right, partitioning)?);
                    };

                    // Build side (does not swap)
                    let left = Arc::clone(join.left());
                    let (_coalesce, mut left) = left.split_head::<CoalescePartitionsExec>();
                    let left_keys = join.on().iter().map(|(l, _)| Arc::clone(l)).collect();
                    if let Some(shuffle) = left.downcast_ref::<NetworkBroadcastExec>() {
                        // broadcast -> hash shuffle (over join on left keys)
                        let h_shuffle = Arc::new(shuffle.to_hash_shuffle(left_keys)) as _;
                        nb_ctx.set_task_count(&h_shuffle, nb_ctx.task_count(&left)?);
                        left = h_shuffle;
                    } else {
                        // no partitioning -> RepartitionExec (over join on left keys) that will
                        // eventually be wired to a NetworkShuffleExec
                        let partitioning = Partitioning::Hash(left_keys, target_partitions);
                        left = Arc::new(RepartitionExec::try_new(left, partitioning)?);
                    };

                    let join = join
                        .builder()
                        .with_partition_mode(PartitionMode::Partitioned)
                        .with_new_children(vec![left, right])?
                        .build()?;
                    Ok(Transformed::yes(Arc::new(join)))
                }
                PartitionMode::Auto => plan_err!(
                    "Found a HashJoinExec in PartitionMode::Auto during plan remainder replanning."
                ),
                // Pretty annoying that the compiler does not enforce this
                _ => unreachable!(),
            }
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .map(|v| v.data)
}

fn should_broadcast(candidate_left: &dyn ExecutionPlan, config: &ConfigOptions) -> Result<bool> {
    let left_stats = StatisticsContext::new().compute(candidate_left, &StatisticsArgs::new())?;

    if let Some(byte_size) = left_stats.total_byte_size.get_value() {
        Ok(*byte_size < config.optimizer.hash_join_single_partition_threshold)
    } else if let Some(num_rows) = left_stats.num_rows.get_value() {
        Ok(*num_rows < config.optimizer.hash_join_single_partition_threshold_rows)
    } else {
        Ok(false)
    }
}

fn should_swap(left: &dyn ExecutionPlan, right: &dyn ExecutionPlan) -> Result<bool> {
    let left_stats = StatisticsContext::new().compute(left, &StatisticsArgs::new())?;
    let right_stats = StatisticsContext::new().compute(right, &StatisticsArgs::new())?;

    // First compare total_byte_size, then fall back to num_rows if byte
    // sizes are unavailable.
    match (
        left_stats.total_byte_size.get_value(),
        right_stats.total_byte_size.get_value(),
    ) {
        (Some(l), Some(r)) => Ok(l > r),
        _ => match (
            left_stats.num_rows.get_value(),
            right_stats.num_rows.get_value(),
        ) {
            (Some(l), Some(r)) => Ok(l > r),
            _ => Ok(false),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;

    #[tokio::test]
    async fn partitioned_to_partitioned_swapped_join_reordering() -> Result<()> {
        // Both `abs(...)` filters are opaque to statistics, so both sides get the same estimate
        // statically. At runtime, `a` keeps all 366 rows while `b` keeps only 33.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") >= 0) a
        LEFT JOIN (SELECT * FROM weather WHERE abs("Rainfall") > 5) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) > 5, projection=[MaxTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 5
        ");

        // Note how the two sides of the join are swapped:
        // - static: projection=[MinTemp@0, MaxTemp@2]
        // - adaptive: projection=[MinTemp@2, MaxTemp@0]
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Right, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) > 5, projection=[MaxTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 5
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_aggregation_becomes_build_side() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", b.cnt
        FROM weather a
        JOIN (SELECT "RainToday", count(*) AS cnt FROM weather GROUP BY "RainToday") b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b.cnt
        "#;

        // Statically, the aggregation output is not known to be small, so the full weather scan
        // is kept as the build side.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, cnt@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, cnt@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@0)], projection=[MinTemp@0, cnt@3]
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
                ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as cnt]
                  AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                    [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                      RepartitionExec: partitioning=Hash([RainToday@0], 12), input_partitions=3
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");

        // At runtime, the aggregation yields a handful of rows, so it becomes the build side.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, cnt@1 ASC NULLS LAST]
          [Stage 4] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, cnt@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@0, RainToday@1)], projection=[MinTemp@2, cnt@1]
                CoalescePartitionsExec
                  [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=1
                    BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
                      SamplerExec: partitions=4
                        ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as cnt]
                          AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                            [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                              RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                                SamplerExec: partitions=3
                                  AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                    SamplerExec: partitions=3
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_shuffled_scan_becomes_broadcast_build_side() -> Result<()> {
        // `abs(...)` filters are opaque to statistics, so both sides get the same estimate
        // statically. At runtime, `a` keeps all 366 rows while `b` keeps only 3.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") >= 0) a
        JOIN (SELECT * FROM weather WHERE abs("Rainfall") > 20) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, neither side is known to be small, so both are hash shuffled.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) > 20, projection=[MaxTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
        ");

        // At runtime, `b` is tiny and its stage is already a plain shuffle right under the join,
        // so the join swaps and that shuffle is read as a broadcast instead.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
                      SamplerExec: partitions=4
                        FilterExec: abs(Rainfall@1) > 20, projection=[MaxTemp@0, RainToday@2]
                          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_shuffled_scan_broadcasts_without_swap() -> Result<()> {
        // `abs(...)` filters are opaque to statistics, so both sides get the same estimate
        // statically. At runtime, `a` keeps only 3 rows while `b` keeps all 366.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") > 20) a
        JOIN (SELECT * FROM weather WHERE abs("Rainfall") >= 0) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, neither side is known to be small, so both are hash shuffled.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) > 20, projection=[MinTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=4
                    FilterExec: abs(Rainfall@1) >= 0, projection=[MaxTemp@0, RainToday@2]
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        // At runtime, `a` is tiny and already the build side, so the join keeps its input order
        // and only switches to broadcasting `a`'s shuffle.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1024)
            .hash_join_single_partition_threshold_rows(10)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
                      SamplerExec: partitions=4
                        FilterExec: abs(Rainfall@1) > 20, projection=[MinTemp@0, RainToday@2]
                          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) >= 0, projection=[MaxTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_reverts_swap_after_sampling_probe_side() -> Result<()> {
        // `b`'s filter is estimated from column statistics, while `a`'s `abs(...)` filter is opaque
        // and gets a default selectivity. At runtime, `a` keeps all 366 rows and `b` keeps 218.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") >= 0) a
        JOIN (SELECT * FROM weather WHERE "MinTemp" > 5) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, `b` is estimated smaller, so it is broadcast and `a` is probed in the join's
        // stage.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=12, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=3, output_partitions=12
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        // At runtime, `b`'s sampled size is first compared against `a`'s static estimate, so the
        // join swaps and `a` is moved into its own broadcast stage (Stage 2). Once `a` is sampled
        // it turns out bigger, so the join swaps back: `b` is broadcast again and `a`'s stage is
        // read as a round-robin shuffle.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
                      SamplerExec: partitions=4
                        FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) >= 0, projection=[MinTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) >= 0
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_swaps_broadcast_to_smaller_probe_side() -> Result<()> {
        // `b`'s filter is estimated from column statistics, while `a`'s `abs(...)` filter is opaque
        // and gets a default selectivity. At runtime, `a` keeps only 3 rows and `b` keeps 218.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") > 20) a
        JOIN (SELECT * FROM weather WHERE "MinTemp" > 5) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, `b` is estimated smaller, so it is broadcast and `a` is probed in the join's
        // stage.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=12, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=3, output_partitions=12
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                FilterExec: abs(Rainfall@1) > 20, projection=[MinTemp@0, RainToday@2]
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
        ");

        // At runtime, `b` turns out bigger than `a`, so the join swaps: `a` is moved into its own
        // broadcast stage and `b`'s already dispatched stage is read as a round-robin shuffle.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        // The swap can be verified by looking at projection=[MinTemp@0, MaxTemp@2] vs the test above (projection=[MinTemp@2, MaxTemp@0])
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                CoalescePartitionsExec
                  [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
                      SamplerExec: partitions=4
                        FilterExec: abs(Rainfall@1) > 20, projection=[MinTemp@0, RainToday@2]
                          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 20
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_broadcast_becomes_partitioned_and_swaps() -> Result<()> {
        // `b`'s filter is estimated from column statistics, while `a`'s `abs(...)` filter is opaque
        // and gets a default selectivity. At runtime, `a` keeps 33 rows and `b` keeps 218.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT * FROM weather WHERE abs("Rainfall") > 5) a
        JOIN (SELECT * FROM weather WHERE "MinTemp" > 5) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, `b` is estimated below the row threshold, so it is broadcast and `a` is
        // probed in the join's stage.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(50)
            .hash_join_single_partition_threshold_rows(50)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=12, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=3, output_partitions=12
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                FilterExec: abs(Rainfall@1) > 5, projection=[MinTemp@0, RainToday@2]
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 5
        ");

        // At runtime, `b` turns out bigger than `a`'s estimate, which is itself above the row
        // threshold, so the join swaps into a partitioned join: `a` becomes the hash shuffled build
        // side and `b`'s already dispatched stage is read as a hash shuffle on the probe side.
        // Once sampled, `a` is still smaller than `b` but above the byte threshold, so the join
        // stays swapped and partitioned.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(50)
            .hash_join_single_partition_threshold_rows(50)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: abs(Rainfall@1) > 5, projection=[MinTemp@0, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=abs(Rainfall@2) > 5
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_broadcast_becomes_partitioned_without_swap() -> Result<()> {
        // `b`'s filter is estimated from column statistics, while `a` is a full scan with exact
        // statistics. At runtime, `a` keeps all 366 rows and `b` keeps 218.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a
        JOIN (SELECT * FROM weather WHERE "MinTemp" > 5) b
          ON a."RainToday" = b."RainToday"
        ORDER BY a."MinTemp", b."MaxTemp"
        "#;

        // Statically, `b` is estimated below the byte threshold, so it is broadcast and `a` is
        // probed in the join's stage.
        let (non_dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                CoalescePartitionsExec
                  [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=12, input_tasks=3
                    BroadcastExec: input_partitions=4, consumer_tasks=3, output_partitions=12
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        // At runtime, `b` turns out above the byte threshold but still smaller than `a`, so the join
        // keeps its input order and becomes partitioned: `b`'s already dispatched broadcast stage is
        // read as a hash shuffle, and `a` is hash shuffled on the same keys.
        let (dynamic, physical_plan_ascii) = TestPlanBuilder::default()
            .dynamic_filters(false)
            .broadcast_joins(true)
            .hash_join_single_partition_threshold(1_000)
            .hash_join_single_partition_threshold_rows(1_000)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_bytes_ready_sample_percentage(1.0)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST]
          [Stage 3] => NetworkCoalesceExec: output_partitions=4, input_tasks=1
            SortExec: expr=[MinTemp@0 ASC NULLS LAST, MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
              HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
                [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=4
                    SamplerExec: partitions=4
                      FilterExec: MinTemp@0 > 5, projection=[MaxTemp@1, RainToday@2]
                        RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                          DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 5, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 5, required_guarantees=[]
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
                    SamplerExec: partitions=3
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        assert_eq!(non_dynamic, dynamic);

        Ok(())
    }
}
