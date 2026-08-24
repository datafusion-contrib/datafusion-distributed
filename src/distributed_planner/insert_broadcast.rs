use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};

use crate::BroadcastExec;

use super::DistributedConfig;

/// This is a top-down traversal of a [ExecutionPlan] that inserts [BroadcastExec] operators where
/// appropriate.
///
/// # What is it doing?
/// The pass searches for joins whose left input can be broadcast without duplicating output rows:
/// CollectLeft [HashJoinExec]s, [NestedLoopJoinExec]s, and [CrossJoinExec]s. Then it does one of
/// two things:
///     1. If the build child is a fetch-less [CoalescePartitionsExec] -> Insert a
///        [BroadcastExec] directly below it. A fetch-bearing coalesce stays below the broadcast
///        so its global limit is applied before the rows are replicated to consumers.
///     2. Otherwise (means it is already single partitioned going into the join) -> Insert a
///        [BroadcastExec] -> [CoalescePartitionsExec] below the join but above its
///        original build child.
/// ```text
///                  ┌──────────────────────┐                                                    ┌──────────────────────┐
///                  │   CoalesceBatches    │                                                    │   CoalesceBatches    │
///                  └───────────▲──────────┘                                                    └───────────▲──────────┘
///                              │                                                                           │
///                  ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                  │       HashJoin       │                                                    │       HashJoin       │
///                  │    (CollectLeft)     │                                                    │    (CollectLeft)     │
///                  └────▲────────────▲────┘                                                    └────▲────────────▲────┘
///                       │            │                                                              │            │
///             ┌─────────┘            └──────────┐                                         ┌─────────┘            └──────────┐
///        Build Side                        Probe Side                                Build Side                        Probe Side
///             │                                 │                                         │                                 │
/// ┌───────────┴──────────┐          ┌───────────┴──────────┐                  ┌───────────┴──────────┐          ┌───────────┴──────────┐
/// │  CoalescePartitions  │          │      Projection      │                  │  CoalescePartitions  │          │      Projection      │
/// └───▲────▲────▲────▲───┘          └───────────▲──────────┘                  └───────────▲──────────┘          └───────────▲──────────┘
///     │    │    │    │                          │                                         │                                 │
/// ┌───┴────┴────┴────┴───┐          ┌───────────┴──────────┐                  ┌───────────┴──────────┐          ┌───────────┴──────────┐
/// │      DataSource      │          │     Aggregation      │ ───────────────▶ │    BroadcastExec     │          │     Aggregation      │
/// └──────────────────────┘          └───────────▲──────────┘                  └──▲────▲────▲────▲────┘          └───────────▲──────────┘
///                                               │                                │    │    │    │                           │
///                                   ┌───────────┴──────────┐                  ┌──┴────┴────┴────┴────┐          ┌───────────┴──────────┐
///                                   │     Repartition      │                  │      DataSource      │          │     Repartition      │
///                                   └───────────▲──────────┘                  └──────────────────────┘          └───────────▲──────────┘
///                                               │                                                                           │
///                                   ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                                   │     Aggregation      │                                                    │     Aggregation      │
///                                   │      (Partial)       │                                                    │      (Partial)       │
///                                   └───────────▲──────────┘                                                    └───────────▲──────────┘
///                                               │                                                                           │
///                                   ┌───────────┴──────────┐                                                    ┌───────────┴──────────┐
///                                   │      DataSource      │                                                    │      DataSource      │
///                                   └──────────────────────┘                                                    └──────────────────────┘
/// ```
///
/// # Why a Right or Inner join type?
/// Left and Full join types allow build side rows to be emitted. This creates complicatioins when
/// broadcasting your build side to all workers as it can cause incorrect and duplicate data. Here
/// is an example:
///
/// Say there is arbitrary tables containing information on customers and their orders.
/// ```text
///                                         Probe Side
///                                 ┌──────────┬─────────────┐
///         Build Side              │ order_id │ customer_id │
/// ┌─────────────┬─────────┐       ├──────────┼─────────────┤
/// │ customer_id │  Name   │       │   100    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      1      │  John   │       │   200    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      2      │  Alice  │       │   300    │      1      │
/// ├─────────────┼─────────┤       ├──────────┼─────────────┤
/// │      3      │   Bob   │       │   400    │      2      │
/// └─────────────┴─────────┘       ├──────────┼─────────────┤
///                                 │   500    │      2      │
///                                 └──────────┴─────────────┘
/// ```
/// Then want to execute the query:
/// SELECT * FROM customers c
/// WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
///
/// This query is selecting all customers that have not made an order. It does this by using a
/// LeftAnti join which will emit all the rows from our build side which do not have a matching
/// join key (in this case customer_id) on the probe side.
///
/// In a single node this would produce the correct result: (3, Bob)
///
/// In a distributed context broadcasting the build table would create incorrect results:
/// ```text
/// ┌──────────────────────────────────────────────────────────┐    ┌──────────────────────────────────────────────────────────┐
/// │                         Worker 1                         │    │                         Worker 1                         │
/// │  ┌─────────────┬─────────┐   ┌──────────┬─────────────┐  │    │  ┌─────────────┬─────────┐                               │
/// │  │ customer_id │  Name   │   │ order_id │ customer_id │  │    │  │ customer_id │  Name   │   ┌──────────┬─────────────┐  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │ order_id │ customer_id │  │
/// │  │      1      │  John   │   │   100    │      1      │  │    │  │      1      │  John   │   ├──────────┼─────────────┤  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │   400    │      2      │  │
/// │  │      2      │  Alice  │   │   200    │      1      │  │    │  │      2      │  Alice  │   ├──────────┼─────────────┤  │
/// │  ├─────────────┼─────────┤   ├──────────┼─────────────┤  │    │  ├─────────────┼─────────┤   │   500    │      2      │  │
/// │  │      3      │   Bob   │   │   300    │      1      │  │    │  │      3      │   Bob   │   └──────────┴─────────────┘  │
/// │  └─────────────┴─────────┘   └──────────┴─────────────┘  │    │  └─────────────┴─────────┘                               │
/// └──────────────────────────────────────────────────────────┘    └──────────────────────────────────────────────────────────┘
/// ```
/// Worker 1 would emit: (2, Alice), (3, Bob)
/// Worker 2 would emit: (1, John), (3, Bob)
/// Thus when unioning results: (2, Alice), (3, Bob), (1, John), (3, Bob)
/// ```
///
/// ```
pub(super) fn insert_broadcast_execs(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    if !d_cfg.broadcast_joins {
        return Ok(plan);
    }

    plan.transform_down(|node| {
        if !can_broadcast_left_input(node.as_ref()) {
            return Ok(Transformed::no(node));
        }

        let children = node.children();
        let Some(build_child) = children.first() else {
            return Ok(Transformed::no(node));
        };

        let broadcast_input = build_child
            .downcast_ref::<CoalescePartitionsExec>()
            .filter(|coalesce| coalesce.fetch().is_none())
            .map_or_else(
                || Arc::clone(build_child),
                |coalesce| Arc::clone(coalesce.input()),
            );

        // consumer_task_count=1 is a placeholder and will be corrected during optimizer rule.
        let broadcast: Arc<dyn ExecutionPlan> = Arc::new(BroadcastExec::new(broadcast_input, 1));
        let new_build_child: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(broadcast));

        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = children.into_iter().cloned().collect();
        new_children[0] = new_build_child;
        Ok(Transformed::yes(node.with_new_children(new_children)?))
    })
    .map(|transformed| transformed.data)
}

fn can_broadcast_left_input(plan: &dyn ExecutionPlan) -> bool {
    if let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() {
        return hash_join.partition_mode() == &PartitionMode::CollectLeft
            && is_left_broadcast_safe(hash_join.join_type());
    }

    if let Some(nested_loop_join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        return is_left_broadcast_safe(nested_loop_join.join_type());
    }

    plan.downcast_ref::<CrossJoinExec>().is_some()
}

pub(super) fn is_left_broadcast_safe(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;
    use datafusion::physical_plan::displayable;

    #[tokio::test]
    async fn test_insert_broadcast_with_existing_coalesce_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_string = TestPlanBuilder::default()
            .num_workers(4)
            .physical_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_string, @r"
        DistributedExec
          CoalescePartitionsExec
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
              CoalescePartitionsExec
                [Stage 1] => NetworkCoalesceExec: output_partitions=12, input_tasks=4
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
              DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        let plan = sql_to_plan_with_broadcast(query, true, 4).await;
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_without_existing_coalesce_build_child() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let physical_plan_string = TestPlanBuilder::new()
            .target_partitions(1)
            .num_workers(4)
            .physical_plan_as_string(query)
            .await;
        assert_snapshot!(physical_plan_string, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        let plan = sql_to_plan_with_broadcast(query, true, 1).await;
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            BroadcastExec: input_partitions=1, consumer_tasks=1, output_partitions=1
              DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
    }

    #[tokio::test]
    async fn test_no_broadcast_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_plan_with_broadcast(query, true, 4).await;
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
    }

    #[tokio::test]
    async fn test_no_broadcast_when_disabled() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_plan_with_broadcast(query, false, 4).await;
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_keeps_fetch_bearing_coalesce_below_broadcast() {
        // A LIMIT on the build side is carried by CoalescePartitionsExec::fetch. That
        // coalesce must stay *below* BroadcastExec so the limit is applied once before
        // fan-out rather than independently in every consumer task.
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM (SELECT "MinTemp", "RainToday" FROM weather OFFSET 0 LIMIT 50) a
        INNER JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_plan_with_broadcast_opts(query, true, 4, true).await;
        assert_snapshot!(plan, @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
          CoalescePartitionsExec
            BroadcastExec: input_partitions=1, consumer_tasks=1, output_partitions=1
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], limit=50, file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_cross_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a CROSS JOIN weather b
        "#;
        let plan = sql_to_plan_with_broadcast(query, true, 4).await;
        assert_snapshot!(plan, @"
        CrossJoinExec
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_insert_broadcast_nested_loop_inner_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#;
        let plan = sql_to_plan_with_broadcast(query, true, 4).await;
        assert_snapshot!(plan, @"
        NestedLoopJoinExec: join_type=Inner, filter=MinTemp@0 > MaxTemp@1
          CoalescePartitionsExec
            BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
              DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    #[tokio::test]
    async fn test_no_broadcast_nested_loop_left_join() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b ON a."MinTemp" > b."MaxTemp"
        "#;
        let plan = sql_to_plan_with_broadcast(query, true, 4).await;
        assert_snapshot!(plan, @"
        NestedLoopJoinExec: join_type=Left, filter=MinTemp@0 > MaxTemp@1
          CoalescePartitionsExec
            DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
          DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp], file_type=parquet
        ");
    }

    async fn sql_to_plan_with_broadcast(
        query: &str,
        broadcast_enabled: bool,
        target_partitions: usize,
    ) -> String {
        sql_to_plan_with_broadcast_opts(query, broadcast_enabled, target_partitions, false).await
    }

    async fn sql_to_plan_with_broadcast_opts(
        query: &str,
        broadcast_enabled: bool,
        target_partitions: usize,
        disable_dynamic_filters: bool,
    ) -> String {
        let test_plan = TestPlanBuilder::new()
            .target_partitions(target_partitions)
            .broadcast_joins(broadcast_enabled)
            .build()
            .await;
        let ctx = test_plan.get_ctx();
        if disable_dynamic_filters {
            let state_ref = ctx.state_ref();
            let mut state = state_ref.write();
            let options = state.config_mut().options_mut();
            options
                .set(
                    "datafusion.optimizer.enable_dynamic_filter_pushdown",
                    "false",
                )
                .expect("static dynamic-filter setting should be valid");
            options
                .set("datafusion.execution.parquet.pruning", "false")
                .expect("static parquet pruning setting should be valid");
        }
        let plan = test_plan.physical_plan(query).await;
        let plan = insert_broadcast_execs(plan, ctx.state_ref().read().config_options().as_ref())
            .expect("failed to insert broadcasts");
        format!("{}", displayable(plan.as_ref()).indent(true))
    }
}
