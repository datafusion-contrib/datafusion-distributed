use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use crate::BroadcastExec;

use super::DistributedConfig;

/// This is a top-down traversal of a [ExecutionPlan] that inserts [BroadcastExec] opeerators where
/// appropriate.
///
/// # What is it doing?
/// The pass searches for CollectLeft [HashJoinExec]s that are either "Right" or Inner join type.
/// Then does one of two things:
///     1. If the build child is a [CoalescePartitionsExec] -> Insert a [BroadcastExec] directly
///        below it.
///     2. Otherwise (means its already single partitioned going into the join) -> Insert a
///        [BroadcastExec] -> [CoalescePartitionsExec] below the [HashJoinExec] but above its
///        orginal build child.
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
        let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if hash_join.partition_mode() != &PartitionMode::CollectLeft {
            return Ok(Transformed::no(node));
        }

        // Only broadcast when output is driven by the probe side.
        // Joins that can emit build-side rows (left/left-semi/left-anti/left-mark/full) would
        // duplicate output if the build is broadcast, thus are excluded.
        let join_type = hash_join.join_type();
        if !matches!(
            join_type,
            JoinType::Inner
                | JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark
        ) {
            return Ok(Transformed::no(node));
        }

        let children = node.children();
        let Some(build_child) = children.first() else {
            return Ok(Transformed::no(node));
        };

        // If build child is CoalescePartitionsExec get its input
        // Otherwise, use the build child directly (DataSourceExec)
        let broadcast_input = if let Some(coalesce) = build_child
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            Arc::clone(coalesce.input())
        } else {
            Arc::clone(build_child)
        };

        // Insert BroadcastExec. consumer_task_count=1 is a placeholder and
        // will be corrected during optimizer rule.
        let broadcast = Arc::new(BroadcastExec::new(
            broadcast_input,
            1, // placeholder
        ));

        // Always wrap with CoalescePartitionsExec
        let new_build_child: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(broadcast));

        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = children.into_iter().cloned().collect();
        new_children[0] = new_build_child;
        Ok(Transformed::yes(node.with_new_children(new_children)?))
    })
    .map(|transformed| transformed.data)
}
