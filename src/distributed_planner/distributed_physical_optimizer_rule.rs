use crate::common::require_one_child;
use crate::distributed_planner::plan_annotator::{
    AnnotatedPlan, RequiredNetworkBoundary, annotate_plan,
};
use crate::{
    DistributedConfig, DistributedExec, NetworkBroadcastExec, NetworkCoalesceExec,
    NetworkShuffleExec, TaskEstimator,
};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::fmt::Debug;
use std::ops::AddAssign;
use std::sync::Arc;
use uuid::Uuid;

/// Physical optimizer rule that inspects the plan, places the appropriate network
/// boundaries, and breaks it down into stages that can be executed in a distributed manner.
///
/// The rule has three steps:
///
/// 1. Annotate the plan with [annotate_plan]: adds some annotations to each node about how
///    many distributed tasks should be used in the stage containing them, and whether they
///    need a network boundary below or not.
///    For more information about this step, read [annotate_plan] docs.
///
/// 2. Based on the [AnnotatedPlan] returned by [annotate_plan], place all the appropriate
///    network boundaries ([NetworkShuffleExec] and [NetworkCoalesceExec]) with the task count
///    assignation that the annotations required. After this, the plan is already a distributed
///    executable plan.
///
/// 3. Place the [CoalesceBatchesExec] in the appropriate places (just below network boundaries),
///    so that we send fewer and bigger record batches over the wire instead of a lot of small ones.
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

/// Takes an [AnnotatedPlan] and returns a modified [ExecutionPlan] with all the network boundaries
/// appropriately placed. This step performs the following modifications to the original
/// [ExecutionPlan]:
/// - The leaf nodes are scaled up in parallelism based on the number of distributed tasks in
///   which they are going to run. This is configurable by the user via the [TaskEstimator] trait.
/// - The appropriate network boundaries are placed in the plan depending on how it was annotated,
///   so new nodes like [NetworkBroadcastExec], [NetworkCoalesceExec] and [NetworkShuffleExec] will be present.
fn distribute_plan(
    annotated_plan: AnnotatedPlan,
    cfg: &ConfigOptions,
    query_id: Uuid,
    stage_id: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let children = annotated_plan.children;
    let parent_task_count = annotated_plan.task_count.as_usize();

    if children.is_empty() {
        let scaled_up = d_cfg.__private_task_estimator.scale_up_leaf_node(
            &annotated_plan.plan,
            parent_task_count,
            cfg,
        );
        return Ok(scaled_up.unwrap_or(annotated_plan.plan));
    }

    let max_child_task_count = children.iter().map(|v| v.task_count.as_usize()).max();

    // We see significant speed-ups when we introduce network boundaries between nested CollectLeft
    // HashJoinExecs. This doesn't make logical sense as they are the same plans, only differing
    // via addiitional network boundaries which solely cause overhead.
    //
    // Hypothesis: The network boundary operators use a spawn_select_all which just buffers streams
    // in memory leading to a faster even with extra network hops.
    if annotated_plan.required_network_boundary.is_some()
        && parent_task_count == 1
        && max_child_task_count == Some(1)
    {
        let new_children = distribute_children(children, cfg, query_id, stage_id)?;
        return annotated_plan.plan.with_new_children(new_children);
    }

    match annotated_plan.required_network_boundary {
        // No network boundary needed, just recurse on children.
        None => {
            let new_children = distribute_children(children, cfg, query_id, stage_id)?;
            annotated_plan.plan.with_new_children(new_children)
        }
        // If the current node has a RepartitionExec below, it needs a shuffle, so put one
        // NetworkShuffleExec boundary in between the RepartitionExec and the current node.
        Some(RequiredNetworkBoundary::Shuffle) => {
            let new_children = distribute_children(children, cfg, query_id, stage_id)?;
            let new_child = Arc::new(NetworkShuffleExec::try_new(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                parent_task_count,
                max_child_task_count.unwrap_or(1),
            )?);
            stage_id.add_assign(1);
            annotated_plan.plan.with_new_children(vec![new_child])
        }
        // If this is a CoalescePartitionsExec or a SortMergePreservingExec, it means that the original
        // plan is trying to merge all partitions into one. We need to go one step ahead and also merge
        // all distributed tasks into one.
        Some(RequiredNetworkBoundary::Coalesce) => {
            let new_children = distribute_children(children, cfg, query_id, stage_id)?;
            let new_child = Arc::new(NetworkCoalesceExec::try_new(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                parent_task_count,
                max_child_task_count.unwrap_or(1),
            )?);
            stage_id.add_assign(1);
            annotated_plan.plan.with_new_children(vec![new_child])
        }
        // Broadcast boundary is placed on the build child of a CollectLeft HashJoinExec, it means
        // that the build side (this node) is trying to broadcast to all consumers. We need to
        // insert a BroadcastExec and NetworkBroadcastExec.
        Some(RequiredNetworkBoundary::Broadcast) => {
            let new_children = distribute_children(children, cfg, query_id, stage_id)?;
            let new_child = NetworkBroadcastExec::with_inner_broadcast(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                parent_task_count,
                max_child_task_count.unwrap_or(1),
            )?;
            stage_id.add_assign(1);
            annotated_plan.plan.with_new_children(vec![new_child])
        }
    }
}

/// Distributes children and returns the resulting [ExecutionPlan]s.
fn distribute_children(
    children: Vec<AnnotatedPlan>,
    cfg: &ConfigOptions,
    query_id: Uuid,
    stage_id: &mut usize,
) -> Result<Vec<Arc<dyn ExecutionPlan>>, DataFusionError> {
    children
        .into_iter()
        .map(|child| distribute_plan(child, cfg, query_id, stage_id))
        .collect::<Result<Vec<_>, _>>()
}

/// Rearranges the [CoalesceBatchesExec] nodes in the plan so that they are placed right below
/// the network boundaries, so that fewer but bigger record batches are sent over the wire across
/// stages.
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

#[cfg(test)]
mod tests {
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{DistributedExt, DistributedPhysicalOptimizerRule};
    use crate::{assert_snapshot, display_plan_ascii};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use itertools::Itertools;
    use std::sync::Arc;
    /* schema for the "weather" table

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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(2))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(0))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ CoalesceBatchesExec: target_batch_size=8192
          │   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=1, stage_partitions=3, input_tasks=3
          │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8] 
            │ BroadcastExec: input_partitions=1, consumer_tasks=3, output_partitions=3
            │   PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   CoalesceBatchesExec: target_batch_size=8192
        │     HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │       CoalescePartitionsExec
        │         [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=4, stage_partitions=4, input_tasks=2
        │       ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │           [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p4..p7] 
          │ BroadcastExec: input_partitions=4, consumer_tasks=1, output_partitions=4
          │   ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
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
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(2))
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

    #[tokio::test]
    async fn test_unioning_2_tables() {
        let query = r#"
        set distributed.children_isolator_unions=true;
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(6))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=6
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] t3:[p12..p15] t4:[p16..p19] t5:[p20..p23] 
          │ DistributedUnionExec: t0:[c0(0/3)] t1:[c0(1/3)] t2:[c0(2/3)] t3:[c1(0/3)] t4:[c1(1/3)] t5:[c1(2/3)]
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: MinTemp@0 > 10
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
          │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MaxTemp@0 < 30
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
          │           PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
          │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_2_tables_limited_workers() {
        let query = r#"
        set distributed.children_isolator_unions=true;
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] 
          │ DistributedUnionExec: t0:[c0] t1:[c1(0/2)] t2:[c1(1/2)]
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: MinTemp@0 > 10
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MaxTemp@0 < 30
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │           PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
          │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_3_tables() {
        let query = r#"
        set distributed.children_isolator_unions=true;
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] 
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: MinTemp@0 > 10
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MaxTemp@0 < 30
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: Temp9am@0 > 15
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@0 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_5_tables() {
        let query = r#"
        set distributed.children_isolator_unions=true;
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        UNION ALL
        SELECT "Temp3pm", "RainToday" FROM weather WHERE "Temp3pm" < 25.0
        UNION ALL
        SELECT "Rainfall", "RainToday" FROM weather WHERE "Rainfall" > 5.0
        "#;
        let plan = sql_to_explain(query, |b| {
            b.with_distributed_worker_resolver(InMemoryWorkerResolver::new(3))
        })
        .await;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p8..p15] t2:[p16..p23] 
          │ DistributedUnionExec: t0:[c0, c1] t1:[c2, c3] t2:[c4]
          │   CoalesceBatchesExec: target_batch_size=8192
          │     FilterExec: MinTemp@0 > 10
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MaxTemp@0 < 30
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: Temp9am@0 > 15
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@0 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          │   ProjectionExec: expr=[Temp3pm@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: Temp3pm@0 < 25
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp3pm, RainToday], file_type=parquet, predicate=Temp3pm@0 < 25, pruning_predicate=Temp3pm_null_count@1 != row_count@2 AND Temp3pm_min@0 < 25, required_guarantees=[]
          │   ProjectionExec: expr=[Rainfall@0 as MinTemp, RainToday@1 as RainToday]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: Rainfall@0 > 5
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=Rainfall@0 > 5, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 5, required_guarantees=[]
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
