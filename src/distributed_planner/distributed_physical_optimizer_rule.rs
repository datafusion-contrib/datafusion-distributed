use crate::common::require_one_child;
use crate::{
    ChannelResolver, DistributedConfig, DistributedExec, NetworkCoalesceExec, NetworkShuffleExec,
    TaskEstimator,
};
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::fmt::{Debug, Formatter};
use std::ops::AddAssign;
use std::sync::Arc;
use uuid::Uuid;

/// Physical optimizer rule that inspects the plan, places the appropriate network
/// boundaries and breaks it down into stages that can be executed in a distributed manner.
///
/// The rule has two steps:
///
/// 1. Inject the appropriate distributed execution nodes in the appropriate places.
///
///  This is done by looking at specific nodes in the original plan and enhancing them
///  with new additional nodes:
///  - a [DataSourceExec] is wrapped with a [PartitionIsolatorExec] for exposing just a subset
///    of the [DataSourceExec] partitions to the rest of the plan.
///  - a [CoalescePartitionsExec] is followed by a [NetworkCoalesceExec] so that all tasks in the
///    previous stage collapse into just 1 in the next stage.
///  - a [SortPreservingMergeExec] is followed by a [NetworkCoalesceExec] for the same reasons as
///    above
///  - a [RepartitionExec] with a hash partition is wrapped with a [NetworkShuffleExec] for
///    shuffling data to different tasks.
///
///
/// 2. Break down the plan into stages
///  
///  Based on the network boundaries ([NetworkShuffleExec], [NetworkCoalesceExec], ...) placed in
///  the plan by the first step, the plan is divided into stages and tasks are assigned to each
///  stage.
///
///  This step might decide to not respect the amount of tasks each network boundary is requesting,
///  like when a plan is not parallelizable in different tasks (e.g. a collect left [HashJoinExec])
///  or when a [DataSourceExec] has not enough partitions to be spread across tasks.
#[derive(Debug, Default)]
pub struct DistributedPhysicalOptimizerRule;

impl PhysicalOptimizerRule for DistributedPhysicalOptimizerRule {
    fn optimize(
        &self,
        original: Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if original.as_any().is::<DistributedExec>() {
            return Ok(original);
        }

        let mut plan = Arc::clone(&original);
        if original.output_partitioning().partition_count() > 1 {
            plan = Arc::new(CoalescePartitionsExec::new(plan))
        }

        let annotated = annotate_plan(plan, cfg)?;

        let mut stage_id = 1;
        let distributed = distribute_plan(annotated, cfg, Uuid::new_v4(), &mut stage_id)?;
        if stage_id == 1 {
            return Ok(original);
        }
        let distributed = push_down_batch_coalescing(distributed, cfg)?;

        Ok(Arc::new(DistributedExec::new(distributed)))
    }

    fn name(&self) -> &str {
        "DistributedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
enum TaskCountAnnotation {
    Desired(usize),
    Maximum(usize),
}

impl TaskCountAnnotation {
    fn as_usize(&self) -> usize {
        match self {
            Self::Desired(desired) => *desired,
            Self::Maximum(maximum) => *maximum,
        }
    }
}

struct AnnotatedPlan {
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<AnnotatedPlan>,
    // annotation fields
    task_count: TaskCountAnnotation,
}

impl Debug for AnnotatedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_dbg(f: &mut Formatter<'_>, plan: &AnnotatedPlan, depth: usize) -> std::fmt::Result {
            writeln!(
                f,
                "{}{}: task_count={:?}",
                " ".repeat(depth * 2),
                plan.plan.name(),
                plan.task_count
            )?;
            for child in plan.children.iter() {
                fmt_dbg(f, child, depth + 1)?;
            }
            Ok(())
        }

        fmt_dbg(f, self, 0)
    }
}

fn annotate_plan(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<AnnotatedPlan, DataFusionError> {
    use TaskCountAnnotation::*;
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    let annotated_children = plan
        .children()
        .iter()
        .map(|child| annotate_plan(Arc::clone(child), cfg))
        .collect::<Result<Vec<_>, _>>()?;

    if plan.children().is_empty() {
        // This is a leaf node, maybe a DataSourceExec, or maybe something else custom from the
        // user. We need to estimate how many tasks are needed for this leaf node, and we'll take
        // this decision into account when deciding how many tasks will be actually used.
        let estimator = &d_cfg.__private_task_estimator;
        if let Some(estimate) = estimator.tasks_for_leaf_node(&plan, cfg) {
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: Desired(estimate.task_count),
            });
        } else {
            // We could not determine how many tasks this leaf node should run on, so
            // assume it cannot be distributed and used just 1 task.
            return Ok(AnnotatedPlan {
                plan,
                children: Vec::new(),
                task_count: Desired(1),
            });
        }
    }

    // The task count for this plan is decided by the biggest task count from the children; unless
    // a child specifies a maximum task count, in that case, the maximum is respected. Some
    // nodes can only run in one task. If there is a subplan with a single node declaring that
    // it can only run in one task, all the rest of the nodes in the stage need to respect it.
    let mut task_count = Desired(1);
    let n_workers = d_cfg.__private_channel_resolver.0.get_urls()?.len().max(1);
    for annotated_child in annotated_children.iter() {
        task_count = match (task_count, &annotated_child.task_count) {
            (Desired(desired), Desired(child)) => Desired(desired.max(*child).min(n_workers)),
            (Maximum(max), Desired(_)) => Maximum(max.min(n_workers)),
            (Desired(_), Maximum(max)) => Maximum((*max).min(n_workers)),
            (Maximum(max_1), Maximum(max_2)) => Maximum(max_1.min(*max_2).min(n_workers)),
        }
    }

    // We cannot partition HashJoinExec nodes yet.
    if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if node.mode == PartitionMode::CollectLeft {
            task_count = Maximum(1);
        }
    }

    // The plan does not need a NetworkBoundary, so just take the biggest task count from
    // the children and annotate the plan with that.
    let mut annotated_plan = AnnotatedPlan {
        plan,
        children: annotated_children,
        task_count,
    };
    let Some(nb_req) = needs_network_boundary_below(&annotated_plan) else {
        return Ok(annotated_plan);
    };

    // The plan needs a NetworkBoundary. At this point we have all the info we need for choosing
    // the right size for the stage below, so what we need to do is take the calculated final
    // task count and propagate to all the children that will eventually be part of the stage.
    fn propagate_task_count(plan: &mut AnnotatedPlan, task_count: &TaskCountAnnotation) {
        plan.task_count = task_count.clone();
        if needs_network_boundary_below(plan).is_none() {
            for child in &mut plan.children {
                propagate_task_count(child, task_count);
            }
        }
    }
    for annotated_child in annotated_plan.children.iter_mut() {
        propagate_task_count(annotated_child, &annotated_plan.task_count);
    }

    // If the current plan that needs a NetworkBoundary boundary below is either a
    // CoalescePartitionsExec or a SortPreservingMergeExec, then we are sure that all the stage
    // that they are going to be part of needs to run in exactly one task.
    if nb_req.type_ == RequiredNetworkBoundaryType::Coalesce {
        annotated_plan.task_count = Maximum(1);
        return Ok(annotated_plan);
    }

    // From now and up in the plan, a new task count needs to be calculated for the next stage.
    // Depending on the number of nodes that reduce/increase cardinality, the task count will be
    // calculated based on the previous task count multiplied by a factor.
    fn calculate_scale_factor(plan: &AnnotatedPlan, f: f64) -> f64 {
        let mut sf = None;

        if needs_network_boundary_below(plan).is_none() {
            for plan in plan.children.iter() {
                sf = match sf {
                    None => Some(calculate_scale_factor(plan, f)),
                    Some(sf) => Some(sf.max(calculate_scale_factor(plan, f))),
                }
            }
        }

        let sf = sf.unwrap_or(1.0);
        match plan.plan.cardinality_effect() {
            CardinalityEffect::LowerEqual => sf / f,
            CardinalityEffect::GreaterEqual => sf * f,
            _ => sf,
        }
    }

    let sf = calculate_scale_factor(
        annotated_plan.children.first().expect("missing child"),
        d_cfg.cardinality_task_count_factor,
    );
    let task_count = annotated_plan.task_count.as_usize() as f64;
    annotated_plan.task_count = Desired((task_count * sf).ceil() as usize);

    Ok(annotated_plan)
}

#[derive(PartialEq)]
enum RequiredNetworkBoundaryType {
    Shuffle,
    Coalesce,
}

struct RequiredNetworkBoundary {
    task_count: usize,
    input_task_count: usize,
    type_: RequiredNetworkBoundaryType,
}

fn distribute_plan(
    annotated_plan: AnnotatedPlan,
    cfg: &ConfigOptions,
    query_id: Uuid,
    stage_id: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    // This is a leaf node, so we need to scale it up with the final task count.
    if annotated_plan.children.is_empty() {
        let scaled_up = d_cfg.__private_task_estimator.scale_up_leaf_node(
            &annotated_plan.plan,
            annotated_plan.task_count.as_usize(),
            cfg,
        );
        return Ok(scaled_up.unwrap_or(annotated_plan.plan));
    }

    let network_boundary_requirement = needs_network_boundary_below(&annotated_plan);
    let one_task_in_parent_and_child = annotated_plan.task_count.as_usize() == 1
        && annotated_plan
            .children
            .iter()
            .all(|v| v.task_count.as_usize() == 1);

    let new_children = annotated_plan
        .children
        .into_iter()
        .map(|child| distribute_plan(child, cfg, query_id, stage_id))
        .collect::<Result<Vec<_>, _>>()?;

    // It does not need a NetworkBoundary, so just keep recursing.
    let Some(nb_req) = network_boundary_requirement else {
        return annotated_plan.plan.with_new_children(new_children);
    };

    // It would need a network boundary, but on both sides of the boundary there is just 1 task,
    // so we are fine with not introducing any network boundary.
    if one_task_in_parent_and_child {
        return annotated_plan.plan.with_new_children(new_children);
    }

    // If the current node has a RepartitionExec below, it needs a shuffle, so put one
    // NetworkShuffleExec boundary in between the RepartitionExec and the current node.
    if nb_req.type_ == RequiredNetworkBoundaryType::Shuffle {
        let new_child = Arc::new(NetworkShuffleExec::try_new(
            require_one_child(new_children)?,
            query_id,
            *stage_id,
            nb_req.task_count,
            nb_req.input_task_count,
        )?);
        stage_id.add_assign(1);
        return annotated_plan.plan.with_new_children(vec![new_child]);
    }

    // If this is a CoalescePartitionsExec or a SortMergePreservingExec, it means that the original
    // plan is trying to merge all partitions into one. We need to go one step ahead and also merge
    // all distributed tasks into one.
    if nb_req.type_ == RequiredNetworkBoundaryType::Coalesce {
        let new_child = Arc::new(NetworkCoalesceExec::try_new(
            require_one_child(new_children)?,
            query_id,
            *stage_id,
            nb_req.task_count,
            nb_req.input_task_count,
        )?);
        stage_id.add_assign(1);
        return annotated_plan.plan.with_new_children(vec![new_child]);
    }

    internal_err!(
        "Unreachable code reached in distribute_plan. Could not determine how to place a network boundary below {}",
        annotated_plan.plan.name()
    )
}

fn push_down_batch_coalescing(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;

    let transformed = plan.transform_up(|plan| {
        let Some(node) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() else {
            return Ok(Transformed::no(plan));
        };

        // Network shuffles imply partitioning each data stream in a lot of different partitions,
        // which means that each resulting stream might contain tiny batches. It's important to
        // have decent sized batches here as this will ultimately be sent over the wire, and the
        // penalty there for sending many tiny batches instead of few big ones is big.
        // TODO: After https://github.com/apache/datafusion/issues/18782 is shipped, the batching
        //  will be integrated in RepartitionExec itself, so we will not need to add a
        //  CoalesceBatchesExec, we just need to tell RepartitionExec to output a
        //  `d_cfg.shuffle_batch_size` batch size.
        //  Tracked by https://github.com/datafusion-contrib/datafusion-distributed/issues/243
        let Some(shuffle) = node.input().as_any().downcast_ref::<NetworkShuffleExec>() else {
            return Ok(Transformed::no(plan));
        };
        // First the child of the NetworkShuffleExec.
        let plan = shuffle.input_stage.plan.decoded()?;
        // Then a CoalesceBatchesExec for sending bigger chunks over the wire.
        let plan = CoalesceBatchesExec::new(Arc::clone(plan), d_cfg.shuffle_batch_size);
        // Then the NetworkShuffleExec itself with the CoalesceBatchesExec as a child.
        let plan = Arc::clone(node.input()).with_new_children(vec![Arc::new(plan)])?;

        Ok(Transformed::yes(plan))
    })?;

    Ok(transformed.data)
}

fn needs_network_boundary_below(parent: &AnnotatedPlan) -> Option<RequiredNetworkBoundary> {
    let child = parent.children.first()?;

    if let Some(r_exec) = child.plan.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(r_exec.partitioning(), Partitioning::Hash(_, _)) {
            return Some(RequiredNetworkBoundary {
                task_count: parent.task_count.as_usize(),
                input_task_count: child.task_count.as_usize(),
                type_: RequiredNetworkBoundaryType::Shuffle,
            });
        }
    }
    if parent.plan.as_any().is::<CoalescePartitionsExec>()
        || parent.plan.as_any().is::<SortPreservingMergeExec>()
    {
        if child.children.is_empty() {
            return None;
        }
        return Some(RequiredNetworkBoundary {
            task_count: parent.task_count.as_usize(),
            input_task_count: child.task_count.as_usize(),
            type_: RequiredNetworkBoundaryType::Coalesce,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{DistributedExt, DistributedPhysicalOptimizerRule};
    use crate::{assert_snapshot, display_plan_ascii};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use itertools::Itertools;
    use std::sync::Arc;
    /* shema for the "weather" table

     MinTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     MaxTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     Rainfall [type=DOUBLE] [repetitiontype=OPTIONAL]
     Evaporation [type=DOUBLE] [repetitiontype=OPTIONAL]
     Sunshine [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustDir [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustSpeed [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir3pm [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Pressure9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Pressure3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     Cloud9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Cloud3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Temp9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Temp3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainToday [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     RISK_MM [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainTomorrow [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
    */

    #[tokio::test]
    async fn test_select_all() {
        let query = r#"
        SELECT * FROM weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @"DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_fewer_workers_than_files() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(2))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_0_workers() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(0))
        })
        .await;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_cardinality_factor() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
                .with_distributed_cardinality_effect_task_scale_factor(3.0)
                .unwrap()
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │           [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3] 
          │ CoalesceBatchesExec: target_batch_size=8192
          │   RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
          │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_a_lot_of_files_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
                .with_distributed_files_per_task(3)
                .unwrap()
        })
        .await;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
                      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        CoalesceBatchesExec: target_batch_size=8192
          HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
            CoalescePartitionsExec
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_left_join_distributed() {
        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'no'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   CoalesceBatchesExec: target_batch_size=8192
        │     HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │       CoalescePartitionsExec
        │         [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        │       ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │           [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
            │     AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │       CoalesceBatchesExec: target_batch_size=8192
            │         FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
            │           RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │             PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │               DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
            └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3] 
          │ CoalesceBatchesExec: target_batch_size=8192
          │   RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
          │     AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
          │           RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
          │             PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │               DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"
        SELECT * FROM weather ORDER BY "MinTemp" DESC
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7] 
            │ CoalesceBatchesExec: target_batch_size=8192
            │   RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │       AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_show_columns() {
        let query = r#"
        SHOW COLUMNS from weather
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[table_catalog@0 as table_catalog, table_schema@1 as table_schema, table_name@2 as table_name, column_name@3 as column_name, data_type@5 as data_type, is_nullable@4 as is_nullable]
          CoalesceBatchesExec: target_batch_size=8192
            FilterExec: table_name@2 = weather
              RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ");
    }

    #[tokio::test]
    #[ignore] // FIXME: fix this test
    async fn test_limited_by_worker() {
        let query = r#"
        SET datafusion.execution.target_partitions=2;
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_channel_resolver(InMemoryChannelResolver::new(2))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ UnionExec
          │   ProjectionExec: expr=[1 as Int64(1)]
          │     PartitionIsolatorExec: t0:[p0,__] t1:[__,p0]
          │       DataSourceExec: file_groups={2 groups: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, file_type=parquet
          │   ProjectionExec: expr=[1 as Int64(1)]
          │     PartitionIsolatorExec: t0:[p0] t1:[__]
          │       DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    async fn sql_to_explain(
        query: &str,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
    ) -> String {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);

        let builder = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_config(config);

        let state = f(builder).build();

        let ctx = SessionContext::new_with_state(state);
        let mut queries = query.split(";").collect_vec();
        let last_query = queries.pop().unwrap();

        for query in queries {
            ctx.sql(query).await.unwrap();
        }

        register_parquet_tables(&ctx).await.unwrap();

        let df = ctx.sql(last_query).await.unwrap();

        let physical_plan = df.create_physical_plan().await.unwrap();
        display_plan_ascii(physical_plan.as_ref(), false)
    }
}
