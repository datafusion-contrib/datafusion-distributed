use crate::distributed_planner::inject_network_boundaries::{
    CardinalityBasedNetworkBoundaryBuilder, inject_network_boundaries,
};
use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
use crate::distributed_planner::partial_reduce_below_network_shuffles::partial_reduce_below_network_shuffles;
use crate::distributed_planner::prepare_network_boundaries::prepare_network_boundaries;
use crate::distributed_planner::push_fetch_into_network_coalesce::push_fetch_into_network_coalesce;
use crate::{DistributedConfig, DistributedExec, NetworkBoundaryExt};
use async_trait::async_trait;
use datafusion::common::tree_node::TreeNode;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

/// Transforms a single-node physical plan into a distributed plan by injecting network
/// boundaries between stages.
///
/// The pipeline runs four passes in order:
///
/// 1. **Pre-distribution shaping.** A [CoalescePartitionsExec] is wrapped on top of the plan
///    when it has more than one output partition (so [inject_network_boundaries] later sees a
///    partition-collecting parent and injects a `NetworkCoalesceExec` above its child). Then
///    [insert_broadcast_execs] adds `BroadcastExec` nodes on the build side of `CollectLeft`
///    hash joins so those build sides can later be wrapped in `NetworkBroadcastExec`.
///
/// 2. **Boundary injection.** [inject_network_boundaries] walks the plan, computes a task count
///    for each node, and inserts `NetworkShuffleExec` / `NetworkBroadcastExec` /
///    `NetworkCoalesceExec` above the nodes that delimit a stage (hash `RepartitionExec`s,
///    build-side `BroadcastExec`s, and any node sitting under a `CoalescePartitionsExec` /
///    `SortPreservingMergeExec`).
///
/// 3. **Boundary preparation.** [prepare_network_boundaries] readies each injected boundary
///    for execution: elides ones that aren't actually needed and finalises the survivors. If
///    no boundary survives, this function returns `None`.
///
/// 4. **Shuffle-volume optimization.** [partial_reduce_below_network_shuffles] inserts partial
///    aggregation nodes underneath hash shuffles where it can, so less data crosses the network.
#[derive(Debug)]
pub(crate) struct DistributedQueryPlanner {
    pub(crate) prev: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let original_plan = match &self.prev {
            None => {
                // Use the default physical planner.
                let planner = DefaultPhysicalPlanner::default();
                planner
                    .create_physical_plan(logical_plan, session_state)
                    .await?
            }
            Some(prev) => {
                prev.create_physical_plan(logical_plan, session_state)
                    .await?
            }
        };

        if original_plan.is::<DistributedExec>() {
            return Ok(original_plan);
        }

        let d_cfg = DistributedConfig::from_config_options(session_state.config_options())?;

        // The plan already contains network boundaries set by the user. Just ensure they have nice
        // unique identifiers for each stage, and move forward with it.
        if original_plan.exists(|plan| Ok(plan.is_network_boundary()))? {
            // Ensure the stages in the plan have nice unique identifiers.
            let plan = prepare_network_boundaries(original_plan)?;
            if !plan.exists(|plan| Ok(plan.is_network_boundary()))? {
                return Ok(plan);
            }
            let plan = push_fetch_into_network_coalesce(plan)?;
            return Ok(Arc::new(
                DistributedExec::new(plan).with_metrics_collection(d_cfg.collect_metrics),
            ));
        }

        let mut plan = Arc::clone(&original_plan);

        if plan.output_partitioning().partition_count() > 1 {
            plan = Arc::new(CoalescePartitionsExec::new(plan));
        }

        let cfg = session_state.config_options();

        plan = insert_broadcast_execs(plan, cfg)?;

        plan = inject_network_boundaries(plan, CardinalityBasedNetworkBoundaryBuilder, cfg).await?;

        plan = prepare_network_boundaries(plan)?;
        if !plan.exists(|plan| Ok(plan.is_network_boundary()))? {
            return Ok(original_plan);
        }

        let plan = partial_reduce_below_network_shuffles(plan, cfg)?;
        let plan = push_fetch_into_network_coalesce(plan)?;

        Ok(Arc::new(
            DistributedExec::new(plan).with_metrics_collection(d_cfg.collect_metrics),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::plans::{BuildSideOneTaskEstimator, TestPlanBuilder};
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
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @"DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_fewer_workers_than_files() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_0_workers() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(0)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
          SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
            ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
              AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                    DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_cardinality_factor() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_cardinality_effect_task_scale_factor(3.0)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │     ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
        │       AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3]
          │ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
          │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_high_file_scan_config_bytes_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
          SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
            ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
              AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
                  AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                    DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query = r#"
        SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
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
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        │     ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │       AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │         [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │     FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
            │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
            │         DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
            └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p3] t1:[p0..p3] t2:[p0..p3]
          │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
          │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │     FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@19 = no AND DynamicFilter [ empty ], pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"
        SELECT * FROM weather ORDER BY "MinTemp" DESC
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet, sort_order_for_reorder=[MinTemp@0 DESC], reverse_row_groups=true
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_limit_fetch_pushes_into_network_coalesce_input_stage() {
        let query = r#"
        SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday" LIMIT 10
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
        │   CoalescePartitionsExec: fetch=10
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ LocalLimitExec: fetch=10
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"
        SELECT DISTINCT "RainToday", "WindGustDir" FROM weather
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p3] t1:[p0..p3]
          │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │   [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p0..p7] t2:[p0..p7]
            │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_show_columns() {
        let query = r#"
        SHOW COLUMNS from weather
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .information_schema(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        FilterExec: table_name@2 = weather, projection=[table_catalog@0, table_schema@1, table_name@2, column_name@3, data_type@5, is_nullable@4]
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ");
    }

    #[tokio::test]
    async fn test_limited_by_worker() {
        let query = r#"
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .target_partitions(2)
            .num_workers(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
          │ DistributedUnionExec: t0:[c0] t1:[c1]
          │   DataSourceExec: file_groups={2 groups: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[1 as Int64(1)], file_type=parquet
          │   DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[1 as Int64(1)], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_limited_by_config() {
        let query = r#"
        SELECT 1 FROM weather
        UNION ALL
        SELECT 1 FROM flights_1m
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .distributed_max_tasks_per_stage(2)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5]
          │ DistributedUnionExec: t0:[c0] t1:[c1]
          │   DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[1 as Int64(1)], file_type=parquet
          │   DataSourceExec: file_groups={1 group: [[/testdata/flights-1m.parquet]]}, projection=[1 as Int64(1)], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_2_tables() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .num_workers(6)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=6
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11] t3:[p12..p15] t4:[p16..p19] t5:[p20..p23]
          │ DistributedUnionExec: t0:[c0(0/3)] t1:[c0(1/3)] t2:[c0(2/3)] t3:[c1(0/3)] t4:[c1(1/3)] t5:[c1(2/3)]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_2_tables_limited_workers() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11]
          │ DistributedUnionExec: t0:[c0(0/2)] t1:[c0(1/2)] t2:[c1]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_3_tables() {
        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p4..p7] t2:[p8..p11]
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp9am@0 > 15
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@17 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_unioning_5_tables() {
        let query = r#"
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
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=24, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p7] t1:[p8..p15] t2:[p16..p23]
          │ DistributedUnionExec: t0:[c0, c3] t1:[c1, c4] t2:[c2]
          │   FilterExec: MinTemp@0 > 10
          │     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: MaxTemp@0 < 30
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp9am@0 > 15
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@17 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          │   ProjectionExec: expr=[Temp3pm@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Temp3pm@0 < 25
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp3pm, RainToday], file_type=parquet, predicate=Temp3pm@18 < 25, pruning_predicate=Temp3pm_null_count@1 != row_count@2 AND Temp3pm_min@0 < 25, required_guarantees=[]
          │   ProjectionExec: expr=[Rainfall@0 as MinTemp, RainToday@1 as RainToday]
          │     FilterExec: Rainfall@0 > 5
          │       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 5, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 5, required_guarantees=[]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_broadcast_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26]
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_nested_joins() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp", c."Rainfall"
        FROM weather a
        INNER JOIN weather b ON a."RainToday" = b."RainToday"
        INNER JOIN weather c ON b."RainToday" = c."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@2, RainToday@1)], projection=[MinTemp@0, MaxTemp@1, Rainfall@3]
          │   CoalescePartitionsExec
          │     [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26]
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2, RainToday@3]
            │     CoalescePartitionsExec
            │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
            │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26]
              │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
              │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
              └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_datasource_as_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");

        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=3
          │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p8] t1:[p9..p17] t2:[p18..p26]
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ")
    }

    #[tokio::test]
    async fn test_broadcast_union_children_isolator_plan() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        UNION ALL
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 4] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          │   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │     CoalescePartitionsExec
          │       [Stage 3] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=3
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 3 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_broadcast_one_to_many_plan() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_ascii = TestPlanBuilder::default()
            .broadcast_joins(true)
            .distributed_task_estimator(BuildSideOneTaskEstimator)
            .physical_plan_as_ascii(query, false)
            .await;
        assert_snapshot!(physical_plan_ascii, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8]
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          │   CoalescePartitionsExec
          │     [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=9, input_tasks=1
          │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p8]
            │ BroadcastExec: input_partitions=3, consumer_tasks=3, output_partitions=9
            │   DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }
}
