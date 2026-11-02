use crate::common::{TreeNodeExt, element_wise_sum, vec_avg_reduce, vec_div, vec_mul};
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::distributed_planner::{
    InjectNetworkBoundaryContext, NetworkBoundaryBuilderResult, ProducerHead, calculate_cost,
    inject_network_boundaries,
};
use crate::dynamic_filtering::{
    is_remote_dynamic_filtering_enabled, orphan_dynamic_filter_consumers,
};
use crate::execution_plans::SamplerExec;
use crate::stage::{LocalStage, RemoteStage};
use crate::{
    BytesCounterMetric, CoordinatorToWorkerMsg, DistributedConfig, LoadInfo, MaxGaugeMetric,
    NetworkBoundaryExt, NetworkCoalesceExec, Stage, TaskCountAnnotation,
};
use dashmap::DashMap;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    ColumnStatistics, ExecutionPlan, ExecutionPlanProperties, Statistics, StatisticsArgs,
    StatisticsContext,
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
    let plans_for_viz = Arc::new(PlanReconstructor::default());

    let head_stage = inject_network_boundaries(
        Arc::clone(base_plan),
        |plan| optimize_remainder(plan, query_coordinator.session_config().options()),
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
                if is_remote_dynamic_filtering_enabled(query_coordinator.session_config())? {
                    orphan_dynamic_filter_consumers(&input_stage.plan)?
                } else {
                    vec![]
                };

            let mut load_info_rxs = Vec::with_capacity(input_stage.tasks);

            let query_coordinator = Arc::clone(query_coordinator);
            let plans_for_viz = Arc::clone(&plans_for_viz);

            Ok(async move {
                let mut stage_coordinator = query_coordinator.stage_coordinator(&input_stage);

                let mut futures = Vec::with_capacity(input_stage.tasks);
                for task_i in 0..input_stage.tasks {
                    futures.push(stage_coordinator.init_bidirectional_stream(task_i));
                }
                let results = futures::future::try_join_all(futures).await?;

                let mut workers = Vec::with_capacity(input_stage.tasks);
                for (task_i, (url, worker_tx, worker_rx_stream)) in results.into_iter().enumerate()
                {
                    workers.push(url);
                    load_info_rxs.push({
                        let rx =
                            stage_coordinator.worker_to_coordinator_task(task_i, worker_rx_stream);
                        UnboundedReceiverStream::new(rx)
                    });
                    let _ = worker_tx.send(CoordinatorToWorkerMsg::KickOffSampling);
                    stage_coordinator.coordinator_to_worker_task(task_i, worker_tx)?;
                }
                stage_coordinator.seal_dynamic_filter_stage()?;

                let (stats, consumer_tc) = if nb_type == TypeId::of::<NetworkCoalesceExec>() {
                    (None, TaskCountAnnotation::exact(NonZeroUsize::MIN, 0.0))
                } else {
                    let (stats, new_metrics) =
                        gather_runtime_statistics(load_info_rxs, &input_stage.plan).await?;
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
                plans_for_viz.insert(input_stage.num, input_stage.plan, metrics);
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
            })
        },
        query_coordinator.session_config(),
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
) -> Result<(Statistics, MetricsSet)> {
    /// How many LoadInfo samples with non-zero sampled bytes should be gathered before considering
    /// that there's enough samples collected to make a final decision. The higher this value is,
    /// the more execution is delayed because of sample collection, but better sampling precision.
    const BYTES_READY_SAMPLE_PERCENTAGE: f32 = 0.2;
    /// If the leaf nodes in the provided `plan` do not provide any row count estimation, there's
    /// no way for us here to estimate the % of completion of the stage during sampling. This is
    /// the default % of completion for those cases.
    const FALLBACK_PCT_COMPLETION: f32 = 0.2;

    let mut new_metrics = MetricsSet::new();
    let Some(sampler) = find_sampler(plan) else {
        return plan_err!("Missing SamplerExec while gathering load report");
    };
    let n_cols = sampler.schema().fields.len();

    fn apply_pct(value: usize, pct: f32) -> usize {
        (value as f32 * pct).round() as usize
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
            >= apply_pct(total_partitions, BYTES_READY_SAMPLE_PERCENTAGE).max(1)
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
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let d_cfg = DistributedConfig::from_config_options(config)?;
    if !d_cfg.dynamic_plan_remainder_replanning {
        return Ok(plan);
    }
    plan = OutputRequirements::new_add_mode().optimize(plan, config)?;
    plan = JoinSelection::new()
        .optimize(Arc::clone(&plan), config)
        // If there's a JOIN expecting to produce a dynamic filter, it cannot be swapped, but the
        // JoinSelection subrule does not know that, and it will fail if we attempt to run it.
        // https://github.com/apache/datafusion/pull/26123 fixed this, and it'll be available in
        // DataFusion 56.
        .unwrap_or(plan);
    plan = EnsureRequirements::new().optimize(plan, config)?;
    plan = CombinePartialFinalAggregate::new().optimize(plan, config)?;
    plan = OptimizeAggregateOrder::new().optimize(plan, config)?;
    plan = OutputRequirements::new_remove_mode().optimize(plan, config)?;

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;

    #[tokio::test]
    async fn dynamic_join_reordering() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;

        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
              [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
              [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        ");

        // Note how the two sides of the join are swapped:
        // - static: projection=[MinTemp@0, MaxTemp@2]
        // - adaptive: projection=[MinTemp@2, MaxTemp@0]
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=Right, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@2, MaxTemp@0]
          [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
          [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_aggregation_becomes_build_side() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", b.cnt
        FROM weather a
        JOIN (SELECT "RainToday", count(*) AS cnt FROM weather GROUP BY "RainToday") b
          ON a."RainToday" = b."RainToday"
        "#;

        // Statically, the aggregation output is not known to be small, so the full weather scan
        // is kept as the build side.
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
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
        let physical_plan_ascii = TestPlanBuilder::default()
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@0, RainToday@1)], projection=[MinTemp@2, cnt@1]
          ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as cnt]
            AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
              [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  SamplerExec: partitions=3
                    AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_keeps_aggregation_as_build_side() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", b.cnt
        FROM (SELECT "RainToday", count(*) AS cnt FROM weather GROUP BY "RainToday") b
        JOIN weather a
          ON a."RainToday" = b."RainToday"
        "#;

        // The small aggregation is already the build side, and runtime statistics confirm it,
        // so the join is not swapped.
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@0, RainToday@1)], projection=[MinTemp@2, cnt@1]
              ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as cnt]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                    RepartitionExec: partitioning=Hash([RainToday@0], 12), input_partitions=3
                      AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
              [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        let physical_plan_ascii = TestPlanBuilder::default()
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@0, RainToday@1)], projection=[MinTemp@2, cnt@1]
          ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as cnt]
            AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
              [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  SamplerExec: partitions=3
                    AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_underestimated_filter() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", a."MaxTemp", a."Rainfall", a."Evaporation", b."Sunshine"
        FROM (SELECT * FROM weather WHERE length("WindGustDir") < 10) a
        JOIN weather b
          ON a."RainToday" = b."RainToday"
        "#;

        // Statically, the filter gets the default selectivity estimate, so the wide filtered side
        // looks smaller than the narrow unfiltered side and is kept as the build side.
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@4, RainToday@1)], projection=[MinTemp@0, MaxTemp@1, Rainfall@2, Evaporation@3, Sunshine@5]
              [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@4], 12), input_partitions=4
                  FilterExec: character_length(WindGustDir@4) < 10, projection=[MinTemp@0, MaxTemp@1, Rainfall@2, Evaporation@3, RainToday@5]
                    RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, WindGustDir, RainToday], file_type=parquet, predicate=character_length(WindGustDir@5) < 10
              [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Sunshine, RainToday], file_type=parquet
        ");

        // At runtime, the filter turns out to let almost every row through, so the narrow
        // unfiltered side becomes the build side.
        let physical_plan_ascii = TestPlanBuilder::default()
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@4)], projection=[MinTemp@2, MaxTemp@3, Rainfall@4, Evaporation@5, Sunshine@0]
          [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Sunshine, RainToday], file_type=parquet
          [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@4], 4), input_partitions=4
              SamplerExec: partitions=4
                FilterExec: character_length(WindGustDir@4) < 10, projection=[MinTemp@0, MaxTemp@1, Rainfall@2, Evaporation@3, RainToday@5]
                  RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, WindGustDir, RainToday], file_type=parquet, predicate=character_length(WindGustDir@5) < 10
        ");

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_exploding_join() -> Result<()> {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp", c."Rainfall"
        FROM weather a
        JOIN weather b ON a."RainToday" = b."RainToday"
        JOIN weather c ON b."WindGustDir" = c."WindGustDir"
        "#;

        // Statically, the output of the first join is kept as the build side of the second join.
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 5] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            HashJoinExec: mode=Partitioned, join_type=Inner, on=[(WindGustDir@2, WindGustDir@1)], projection=[MinTemp@0, MaxTemp@1, Rainfall@3]
              [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([WindGustDir@2], 12), input_partitions=4
                  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@2)], projection=[MinTemp@0, MaxTemp@2, WindGustDir@3]
                    [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                      RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
                    [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                      RepartitionExec: partitioning=Hash([RainToday@2], 12), input_partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, WindGustDir, RainToday], file_type=parquet
              [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([WindGustDir@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, WindGustDir], file_type=parquet
        ");

        // At runtime, joining on the low-cardinality RainToday column fans out into many rows, so
        // the second join swaps its inputs and builds on the plain scan instead.
        let physical_plan_ascii = TestPlanBuilder::default()
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(WindGustDir@1, WindGustDir@2)], projection=[MinTemp@2, MaxTemp@3, Rainfall@0]
          [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([WindGustDir@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, WindGustDir], file_type=parquet
          [Stage 4] => NetworkShuffleExec: output_partitions=4, input_tasks=1
            RepartitionExec: partitioning=Hash([WindGustDir@2], 4), input_partitions=4
              SamplerExec: partitions=4
                HashJoinExec: mode=Partitioned, join_type=Inner, on=[(RainToday@1, RainToday@2)], projection=[MinTemp@0, MaxTemp@2, WindGustDir@3]
                  [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                    RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
                      SamplerExec: partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
                  [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                    RepartitionExec: partitioning=Hash([RainToday@2], 4), input_partitions=3
                      SamplerExec: partitions=3
                        DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, WindGustDir, RainToday], file_type=parquet
        ");

        Ok(())
    }

    #[tokio::test]
    async fn dynamic_join_reordering_semi_join() -> Result<()> {
        let query = r#"
        SELECT "MinTemp" FROM weather WHERE "RainToday" IN (SELECT DISTINCT "RainTomorrow" FROM weather)
        "#;

        // Statically, the outer weather scan is the build side of a LeftSemi join.
        let physical_plan_ascii = TestPlanBuilder::default()
            .dynamic_filters(false)
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        CoalescePartitionsExec
          [Stage 3] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
            HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(RainToday@1, RainTomorrow@0)], projection=[MinTemp@0]
              [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 12), input_partitions=3
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
              AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[]
                [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
                  RepartitionExec: partitioning=Hash([RainTomorrow@0], 12), input_partitions=3
                    AggregateExec: mode=Partial, gby=[RainTomorrow@0 as RainTomorrow], aggr=[]
                      DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainTomorrow], file_type=parquet
        ");

        // At runtime, the DISTINCT subquery yields a handful of rows, so the join is swapped into
        // a RightSemi join that builds on the subquery.
        let physical_plan_ascii = TestPlanBuilder::default()
            .hash_join_single_partition_threshold(0)
            .hash_join_single_partition_threshold_rows(0)
            .dynamic_filters(false)
            .distributed_dynamic_task_count(true)
            .distributed_dynamic_plan_remainder_replanning(true)
            .executed_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(RainTomorrow@0, RainToday@1)], projection=[MinTemp@0]
          AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[]
            [Stage 2] => NetworkShuffleExec: output_partitions=4, input_tasks=3
              RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=3
                SamplerExec: partitions=3
                  AggregateExec: mode=Partial, gby=[RainTomorrow@0 as RainTomorrow], aggr=[]
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainTomorrow], file_type=parquet
          [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
            RepartitionExec: partitioning=Hash([RainToday@1], 4), input_partitions=3
              SamplerExec: partitions=3
                DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");

        Ok(())
    }
}
