use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;

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
/// - [NestedLoopJoinExec]s with a build-side-emitting join type are swapped (Left becomes
///   Right, LeftSemi becomes RightSemi, and so on), so the emitting side becomes the
///   partitioned probe side and the other side can be broadcast as usual. There is no
///   partitioned fallback for a NestedLoopJoin: its predicate is arbitrary, so no partitioning
///   can co-locate matching rows.
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
        {
            // The build side's CoalescePartitionsExec only exists to satisfy the single-partition
            // requirement of the *current* orientation. After the swap that side becomes the
            // partitioned probe side, so strip it or it would serialize the probe;
            // [insert_broadcast_execs] re-coalesces the new build side when it broadcasts it.
            let swapped = match join.left().downcast_ref::<CoalescePartitionsExec>() {
                Some(coalesce) => Arc::clone(&node)
                    .with_new_children(vec![
                        Arc::clone(coalesce.input()),
                        Arc::clone(join.right()),
                    ])?
                    .downcast_ref::<NestedLoopJoinExec>()
                    .expect("with_new_children changed the node type")
                    .swap_inputs()?,
                None => join.swap_inputs()?,
            };
            return Ok(Transformed::yes(swapped));
        }
        Ok(Transformed::no(node))
    })
    .map(|transformed| transformed.data)
}

/// Rebuilds a CollectLeft [HashJoinExec] as a [PartitionMode::Partitioned] one, hash-partitioning
/// both inputs on the join keys. The build side's [CoalescePartitionsExec] (the artifact of
/// CollectLeft's single-partition requirement) is stripped, as the hash repartition replaces it.
fn collect_left_to_partitioned(
    join: &HashJoinExec,
    target_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let (left_keys, right_keys): (Vec<_>, Vec<_>) = join
        .on()
        .iter()
        .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
        .unzip();

    let build_input = join
        .left()
        .downcast_ref::<CoalescePartitionsExec>()
        .map_or_else(|| Arc::clone(join.left()), |c| Arc::clone(c.input()));

    let left = Arc::new(RepartitionExec::try_new(
        build_input,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr;

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
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
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

    #[test]
    fn collect_left_to_partitioned_preserves_every_field() {
        // Tripwire: `collect_left_to_partitioned` rebuilds the join through `try_new`,
        // copying each field explicitly — a forgotten field silently resets to its default.
        // Every field below is set to a NON-default value so any reset fails an assertion.
        // If `HashJoinExec` grows a new field, thread it through the conversion and set it
        // to a non-default value here.
        //
        // The join is a LeftAnti with a single key because that is the one shape where all
        // fields can be non-default at once (`null_aware` is only valid there). The caller's
        // guard never converts null-aware joins — this exercises the helper directly so the
        // policy stays in one place while the helper remains faithful.
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{JoinSide, NullEquality, ScalarValue};
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));

        // Keep direct handles to the original inputs to assert identity after the rewrite.
        let build_input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let probe_input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let build_side: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&build_input)));

        let key = |name: &str| -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new_with_schema(name, &schema).unwrap())
        };
        // Residual filter over one intermediate column: left-side `b` > 5.
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("b", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
            )),
            vec![ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            }],
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true)])),
        );
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            Vec::new(),
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));

        let original = HashJoinExec::try_new(
            build_side,
            Arc::clone(&probe_input),
            vec![(key("a"), key("a"))],
            Some(filter),
            &JoinType::LeftAnti,
            Some(vec![0]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
            true,
        )
        .unwrap();
        let original = original.with_dynamic_filter_expr(dynamic_filter).unwrap();
        let original = original.with_fetch(Some(7)).unwrap();
        let original = original.downcast_ref::<HashJoinExec>().unwrap();

        let converted = collect_left_to_partitioned(original, 7).unwrap();
        let converted = converted
            .downcast_ref::<HashJoinExec>()
            .expect("conversion must produce a HashJoinExec");

        // The one intentional change:
        assert_eq!(converted.mode, PartitionMode::Partitioned);

        // Everything else must survive verbatim.
        // left_fut, random_state, and column_indices are unreachable by any public API
        // equivalence properties (HashJoinExec.cache) necessarily change.

        assert_eq!(converted.on, original.on);
        // using debug output as means of getting around lack of PartialEq
        assert_eq!(
            format!("{:?}", converted.filter),
            format!("{:?}", original.filter)
        );
        assert_eq!(converted.join_schema(), original.join_schema());
        assert_eq!(
            format!("{:?}", converted.metrics()),
            format!("{:?}", original.metrics())
        );
        assert_eq!(converted.projection, original.projection);
        assert_eq!(converted.null_equality, original.null_equality);
        assert_eq!(
            converted.dynamic_filter_expr(),
            original.dynamic_filter_expr()
        );
        assert_eq!(converted.fetch(), original.fetch());

        // Both inputs get re-wrapped in hash repartitions on the join keys with the
        // requested partition count; the build side's CoalescePartitionsExec is stripped,
        // and the ORIGINAL input nodes sit directly underneath (same Arcs, not rebuilt).
        for (child, original_input) in [
            (converted.left(), &build_input),
            (converted.right(), &probe_input),
        ] {
            let repartition = child
                .downcast_ref::<RepartitionExec>()
                .expect("both inputs must be re-wrapped in a RepartitionExec");
            assert!(matches!(
                repartition.partitioning(),
                Partitioning::Hash(_, 7)
            ));
            assert!(Arc::ptr_eq(repartition.input(), original_input));
        }
    }

    async fn sql_to_normalized_plan(query: &str, broadcast_enabled: bool) -> String {
        let test_plan = TestPlanBuilder::new()
            .target_partitions(3)
            .broadcast_joins(broadcast_enabled)
            .build()
            .await;
        let ctx = test_plan.get_ctx();
        let plan = test_plan.physical_plan(query).await;
        let plan = normalize_collect_joins(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to normalize collect joins");
        format!("{}", displayable(plan.as_ref()).indent(true))
    }
}
