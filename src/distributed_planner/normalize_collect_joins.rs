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
/// - CollectLeft [HashJoinExec]s with a build-side-emitting join type become
///   [PartitionMode::Partitioned], hash-repartitioning both sides on the join keys. Every
///   matching row pair then meets in exactly one partition, owned by exactly one task, so each
///   build-side row is emitted exactly once with complete match information.
/// - [NestedLoopJoinExec]s with a build-side-emitting join type are swapped (Left becomes
///   Right, LeftSemi becomes RightSemi, and so on), so the emitting side becomes the
///   partitioned probe side and the other side can be broadcast as usual. There is no
///   partitioned fallback for a NestedLoopJoin: its predicate is arbitrary, so no partitioning
///   can co-locate matching rows.
///
/// Two shapes have no distributed rewrite and are left untouched for
/// [inject_network_boundaries] to cap at a single task:
///
/// - Null-aware anti joins: their NULL-existence checks require global knowledge held in
///   process-local shared state, which cannot span tasks in any orientation.
/// - Full joins: they emit unmatched rows from both sides, so they need global match knowledge
///   on both sides at once.
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
            && join.join_type() != &JoinType::Full
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

    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        join.on().to_vec(),
        join.filter().cloned(),
        join.join_type(),
        join.projection.as_ref().map(|p| p.to_vec()),
        PartitionMode::Partitioned,
        join.null_equality(),
        join.null_aware,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;
    use datafusion::physical_plan::displayable;

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

    #[tokio::test]
    async fn test_left_hash_join_converted_to_partitioned() {
        let query = r#"
        SELECT a."MinTemp", b."MaxTemp"
        FROM weather a LEFT JOIN weather b
        ON a."RainToday" = b."RainToday"
        "#;
        let plan = sql_to_normalized_plan(query, true).await;
        assert!(plan.contains("HashJoinExec: mode=Partitioned, join_type=Left"));
        assert_snapshot!(plan);
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
        assert_snapshot!(plan);
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
}
