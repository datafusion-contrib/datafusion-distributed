use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, assert_or_internal_err};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use super::DistributedConfig;
use super::insert_broadcast::is_left_broadcast_safe;

/// Rewrites joins that would otherwise be restricted to a single task into shapes that
/// distribute correctly.
///
/// [insert_broadcast_execs] can only broadcast the build side of joins whose join type never
/// emits build-side rows (see [is_left_broadcast_safe]): a broadcast build side is replicated
/// into every task, so a join type that emits build-side rows would emit them once per task.
/// Without a broadcast, a multi-task stage gives every task only a slice of the collected build
/// side, which silently loses rows. This pass rewrites the affected joins instead of leaving
/// them to run in a single task:
///
/// - CollectLeft [HashJoinExec]s with a build-side-emitting join type (including Full) become
///   [PartitionMode::Partitioned], hash-repartitioning both sides on the join keys. Every
///   row pair that could match then meets in exactly one partition, owned by exactly one
///   task, so matched pairs and unmatched rows — on either side — are decided with complete
///   information and emitted exactly once. This is the same mode swap DataFusion's own
///   JoinSelection performs when the build side crosses the CollectLeft size threshold.
///
/// ```text
///                   ┌──────────────────────┐                                             ┌──────────────────────┐
///                   │       HashJoin       │                                             │       HashJoin       │
///                   │   mode=CollectLeft   │                                             │   mode=Partitioned   │
///                   └────▲────────────▲────┘                                             └────▲────────────▲────┘
///                        │            │                                                       │            │
///              ┌─────────┘            └──────────┐                                  ┌─────────┘            └──────────┐
///         Build Side                        Probe Side                         Build Side                        Probe Side
///              │                                 │                                  │                                 │
///  ┌───────────┴──────────┐          ┌───────────┴──────────┐           ┌───────────┴──────────┐          ┌───────────┴──────────┐
///  │  CoalescePartitions  │          │      DataSource      │ ───────▶  │  Repartition (Hash)  │          │  Repartition (Hash)  │
///  └───▲────▲────▲────▲───┘          └──────────────────────┘           └───▲────▲────▲────▲───┘          └───────────▲──────────┘
///      │    │    │    │                                                     │    │    │    │                          │
///  ┌───┴────┴────┴────┴───┐                                             ┌───┴────┴────┴────┴───┐          ┌───────────┴──────────┐
///  │      DataSource      │                                             │      DataSource      │          │      DataSource      │
///  └──────────────────────┘                                             └──────────────────────┘          └──────────────────────┘
/// ```
///
///   A fetch-less build-side [CoalescePartitionsExec] (CollectLeft's single-partition
///   artifact) is stripped as shown; a fetch-bearing one is retained below the new
///   RepartitionExec (see `collect_left_to_partitioned`).
///
/// - [NestedLoopJoinExec]s with a build-side-emitting join type are swapped (Left becomes
///   Right, LeftSemi becomes RightSemi, and so on), so the emitting side becomes the
///   partitioned probe side and the other side can be broadcast as usual. There is no
///   partitioned fallback for a NestedLoopJoin: its predicate is arbitrary, so no partitioning
///   can co-locate matching rows.
///
/// ```text
///                   ┌──────────────────────┐                                             ┌──────────────────────┐
///                   │    NestedLoopJoin    │                                             │      Projection      │
///                   │   (join_type=Left)   │                                             └───────────▲──────────┘
///                   └────▲────────────▲────┘                                                         │
///                        │            │                                                  ┌───────────┴──────────┐
///              ┌─────────┘            └──────────┐                                       │    NestedLoopJoin    │
///     Build Side (emits)                    Probe Side                                   │  (join_type=Right)   │
///              │                                 │                                       └────▲────────────▲────┘
///  ┌───────────┴──────────┐          ┌───────────┴──────────┐                                 │            │
///  │  CoalescePartitions  │          │    DataSource (R)    │ ───────▶              ┌─────────┘            └──────────┐
///  └───▲────▲────▲────▲───┘          └──────────────────────┘                  Build Side                    Probe Side (emits)
///      │    │    │    │                                                             │                                 │
///  ┌───┴────┴────┴────┴───┐                                             ┌───────────┴──────────┐          ┌───────────┴──────────┐
///  │    DataSource (L)    │                                             │    DataSource (R)    │          │    DataSource (L)    │
///  └──────────────────────┘                                             └──────────────────────┘          └──────────────────────┘
/// ```
///
///   The swap strips a fetch-less build-side [CoalescePartitionsExec] (it would serialize
///   the new probe side), and the Projection restores the pre-swap column order (Semi/Anti/
///   Mark swaps don't need one). [insert_broadcast_execs] later coalesces and broadcasts
///   the new build side.
///
/// Two shapes have no distributed rewrite and are left untouched for
/// [inject_network_boundaries] to cap at a single task:
///
/// - Null-aware anti joins: their NULL-existence checks ("did the probe side contain any
///   NULL at all?") are global facts kept in shared state that is only global while a single
///   build is shared by every probe partition. Per-partition builds lose them, so not even
///   [PartitionMode::Partitioned] is equivalent — this is a semantic restriction, not a
///   distribution one.
/// - Full [NestedLoopJoinExec]s: a NestedLoopJoin only has replication strategies, and a
///   Full join emits unmatched rows from both sides, so every orientation replicates an
///   emitting side.
///
/// And finally, any join with a single partition on both sides is left untouched. DataFusion
/// may apply optimizations when there a single-partition that are not correct for multiple
/// partitions. We maintain correctness by capping them to a single task in
/// [inject_network_boundaries].
///
/// [insert_broadcast_execs]: super::insert_broadcast::insert_broadcast_execs
/// [inject_network_boundaries]: super::inject_network_boundaries::inject_network_boundaries
pub(super) fn normalize_collect_joins(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let target_partitions = cfg.execution.target_partitions;

    plan.transform_down(|node| {
        if let Some(join) = node.downcast_ref::<HashJoinExec>()
            && join.mode == PartitionMode::CollectLeft
            && !is_left_broadcast_safe(join.join_type())
            && !join.null_aware
            && !both_sides_use_single_partition(join.left(), join.right())
        {
            return Ok(Transformed::yes(collect_left_to_partitioned(
                join,
                target_partitions,
            )?));
        }
        if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>()
            // Swapping only helps when the resulting probe-side-emitting join can actually be
            // broadcast; without broadcasts the join runs in a single task either way.
            && d_cfg.broadcast_joins
            && !is_left_broadcast_safe(join.join_type())
            && join.join_type() != &JoinType::Full
            && !both_sides_use_single_partition(join.left(), join.right())
        {
            // A fetch-less build side CoalescePartitionsExec only exists to satisfy the single-partition
            // requirement of the *current* orientation. After the swap that side becomes the
            // partitioned probe side, so strip it or it would serialize the probe;
            // [insert_broadcast_execs] re-coalesces the new build side when it broadcasts it.
            //
            // If the CoalescePartitionsExec has a fetch, then it must be retained for correctness
            let swapped = if let Some(coalesce) =
                join.left().downcast_ref::<CoalescePartitionsExec>()
                && coalesce.fetch().is_none()
            {
                Arc::clone(&node)
                    .with_new_children(vec![
                        Arc::clone(coalesce.input()),
                        Arc::clone(join.right()),
                    ])?
                    .downcast_ref::<NestedLoopJoinExec>()
                    .expect("with_new_children changed the node type")
                    .swap_inputs()?
            } else {
                join.swap_inputs()?
            };
            return Ok(Transformed::yes(swapped));
        }
        Ok(Transformed::no(node))
    })
    .map(|transformed| transformed.data)
}

/// Rebuilds a CollectLeft [HashJoinExec] as a [PartitionMode::Partitioned] one, hash-partitioning
/// both inputs on the join keys.
fn collect_left_to_partitioned(
    join: &HashJoinExec,
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    assert_or_internal_err!(
        join.right().output_partitioning().partition_count() > 1,
        "Single-partition joins are safe and can actually be made incorrect by repartitioning them"
    );

    let (left_keys, right_keys): (Vec<_>, Vec<_>) = join
        .on()
        .iter()
        .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
        .unzip();

    // If the build input is a fetch-less [CoalescePartitionsExec], we can strip it as it's only
    // a remnant of CollectLeft's need to collect input into a single partition. Otherwise, if
    // the build input contains a fetch or is some other node type, we must retain it to ensure
    // correct behavior
    let build_input = if let Some(coalesce) = join.left().downcast_ref::<CoalescePartitionsExec>()
        && coalesce.fetch().is_none()
    {
        coalesce.input()
    } else {
        join.left()
    };
    let left = Arc::new(RepartitionExec::try_new(
        Arc::clone(build_input),
        Partitioning::Hash(left_keys, target_partitions),
    )?);

    let right = Arc::new(RepartitionExec::try_new(
        Arc::clone(join.right()),
        Partitioning::Hash(right_keys, target_partitions),
    )?);

    let new_join = join
        .builder()
        .with_partition_mode(PartitionMode::Partitioned)
        .with_new_children(vec![left, right])?
        .build()?;

    Ok(Arc::new(new_join))
}

fn both_sides_use_single_partition(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> bool {
    left.output_partitioning().partition_count() == 1
        && right.output_partitioning().partition_count() == 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::{TestPlan, TestPlanBuilder};
    use datafusion::physical_plan::displayable;

    #[tokio::test]
    async fn test_left_hash_join_converted_to_partitioned() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("HashJoinExec: mode=Partitioned, join_type=Left"));
        assert_snapshot!(plan, @"
        HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          RepartitionExec: partitioning=Hash([RainToday@1], 3), input_partitions=3
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          RepartitionExec: partitioning=Hash([RainToday@1], 3), input_partitions=3
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");
    }

    #[tokio::test]
    async fn test_nested_loop_left_join_swapped() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."MinTemp" < b."MaxTemp"
        "#;
        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("NestedLoopJoinExec: join_type=Right"));
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[MinTemp@1 as MinTemp, MaxTemp@0 as MaxTemp]
          NestedLoopJoinExec: join_type=Right, filter=MinTemp@0 < MaxTemp@1
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_nested_loop_full_join_untouched() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a FULL JOIN weather b
        ON a."MinTemp" < b."MaxTemp"
        "#;
        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("NestedLoopJoinExec: join_type=Full"));
        assert!(!plan.contains("RepartitionExec: partitioning=Hash"));
    }

    #[tokio::test]
    async fn test_full_hash_join_converted_to_partitioned() {
        // Key co-location gives complete match information on BOTH sides at once, so Full
        // hash joins convert like the other build-side-emitting types. (Contrast with
        // test_nested_loop_full_join_untouched: NLJs only have replication strategies, and
        // Full has an emitting side in every orientation, so Full NLJs stay capped.)
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a FULL JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;

        // Pin the pre-normalization shape: this must start as a CollectLeft Full join, or
        // the conversion assertion below would pass vacuously.
        let raw_plan = TestPlanBuilder::new()
            .target_partitions(3)
            .broadcast_joins(true)
            .build()
            .await
            .physical_plan_as_string(query)
            .await;
        assert!(raw_plan.contains("HashJoinExec: mode=CollectLeft, join_type=Full"));

        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("HashJoinExec: mode=Partitioned, join_type=Full"));
        assert!(plan.contains("RepartitionExec: partitioning=Hash"));
    }

    #[tokio::test]
    async fn test_inner_collect_left_join_untouched() {
        // Inner joins are broadcast-safe, so they keep their CollectLeft shape and get a
        // broadcast from insert_broadcast_execs instead.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("HashJoinExec: mode=CollectLeft, join_type=Inner"));
    }

    #[tokio::test]
    async fn test_nested_loop_left_join_untouched_without_broadcasts() {
        // Without broadcast joins the swapped join could not be broadcast either, so the
        // rewrite is skipped and the join runs in a single task.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."MinTemp" < b."MaxTemp"
        "#;
        let plan = sql_to_normalized_plan(query, false).await;
        assert!(plan.contains("NestedLoopJoinExec: join_type=Left"));
    }

    #[tokio::test]
    async fn collect_left_to_partitioned_preserves_dynamic_filter() {
        // A LEFT join is used because it is both build-side-emitting (so it gets converted)
        // and probe-side-preserved w.r.t. the ON clause (so DataFusion attaches a dynamic
        // filter to it). A FULL join would convert too, but never carries a dynamic filter:
        // probe rows skipped by the filter would still need to be emitted as unmatched.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let test_plan = test_plan(false).await;
        let ctx = test_plan.get_ctx();
        let plan = test_plan.physical_plan(query).await;
        let original = plan.downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(original.mode, PartitionMode::CollectLeft);

        let normalized = normalize_collect_joins(
            plan.clone(),
            ctx.state_ref().read().config_options().as_ref(),
        )
        .unwrap();
        let converted = normalized.downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(converted.mode, PartitionMode::Partitioned);

        // DynamicFilterPhysicalExpr equality is pointer-based on the shared inner state, so
        // this holds only if the converted join kept the original filter (the same instance
        // the probe-side scan subscribes to), not a lookalike replacement.
        let original_dynamic = original.dynamic_expressions_produced();
        let converted_dynamic = converted.dynamic_expressions_produced();
        assert_eq!(original_dynamic.len(), 1);
        assert_eq!(converted_dynamic.len(), 1);
        assert!(Arc::ptr_eq(&original_dynamic[0], &converted_dynamic[0]));
    }

    async fn test_plan(broadcast_enabled: bool) -> TestPlan {
        TestPlanBuilder::new()
            .target_partitions(3)
            .broadcast_joins(broadcast_enabled)
            .build()
            .await
    }

    async fn sql_to_normalized_plan(query: &str, broadcast_enabled: bool) -> String {
        let test_plan = test_plan(broadcast_enabled).await;
        let ctx = test_plan.get_ctx();
        let plan = test_plan.physical_plan(query).await;
        let plan = normalize_collect_joins(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to normalize collect joins");
        format!("{}", displayable(plan.as_ref()).indent(true))
    }
}
