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

        // Don't apply broadcast for certain join types that don't work correctly with
        // broadcast build and partitioned probe:
        //
        // 1. Semi/Anti joins: These output rows from only the build side (not probe).
        //    When the build is broadcast and probe is partitioned, the same build row
        //    can match probe rows in different partitions, causing duplicate outputs.
        //
        // 2. Full outer joins: These output unmatched rows from both sides. When the
        //    build is broadcast, each consumer outputs the same unmatched build rows,
        //    causing duplication. Probe-side unmatched rows are only seen by one consumer.
        //
        // For Inner/Left/Right joins, the probe side drives the output, so partitioning
        // the probe side doesn't cause duplication.
        let join_type = hash_join.join_type();
        if matches!(
            join_type,
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::Full
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
