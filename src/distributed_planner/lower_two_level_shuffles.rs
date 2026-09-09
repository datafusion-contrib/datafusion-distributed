use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{NetworkBoundaryExt, NetworkShuffleExec};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::sync::Arc;

// A salt that makes the second DataFusion hash pass use the first hash as its seed.
pub(crate) const REMOTE_SHUFFLE_SALT: u64 = 0x9e37_79b9_7f4a_7c15;

/// Splits planner-generated hash shuffles into a task-level network shuffle and
/// a process-local hash repartition.
///
/// The network boundary exposes one input partition per producer task, and its producer
/// head creates `consumer_tasks` buckets instead of `consumer_tasks * local_partitions`.
/// The outer repartition restores the original local partitioning contract.
pub(crate) fn lower_two_level_shuffles(
    plan: Arc<dyn ExecutionPlan>,
    consumer_tasks: usize,
    min_fanout: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if min_fanout == 0 {
        return Ok(plan);
    }
    lower_region(plan, consumer_tasks, min_fanout)
}

// Changing only one join input can route matching rows to different tasks.
// Check the whole stage together. Isolated union children have separate task
// allocations and are checked independently.
fn lower_region(
    plan: Arc<dyn ExecutionPlan>,
    task_count: usize,
    min_fanout: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut can_lower_region = true;
    plan.apply(|node| {
        if node.is::<ChildrenIsolatorUnionExec>() {
            return Ok(TreeNodeRecursion::Jump);
        }
        if let Some(shuffle) = node.downcast_ref::<NetworkShuffleExec>() {
            let fanout = shuffle.input_stage.task_count()
                .saturating_mul(task_count)
                .saturating_mul(shuffle.properties().output_partitioning().partition_count());
            // Low fanout may not repay the receiver repartition.
            // That repartition restores hashing, not ordering.
            can_lower_region &= fanout >= min_fanout
                && shuffle.properties().output_ordering().is_none()
                && matches!(shuffle.properties().output_partitioning(), Partitioning::Hash(_, k) if *k > 1);
        }
        Ok(if node.is_network_boundary() { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue })
    })?;

    Ok(plan
        .transform_down(|node| {
            if let Some(union) = node.downcast_ref::<ChildrenIsolatorUnionExec>() {
                let counts = union.child_task_counts();
                let children = node
                    .children()
                    .into_iter()
                    .zip(counts)
                    .map(|(child, tasks)| lower_region(Arc::clone(child), tasks, min_fanout))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Transformed::new(
                    node.replace_children(
                        children,
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )?,
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            let Some(boundary) = node.as_network_boundary() else {
                return Ok(Transformed::no(node));
            };
            // Static planning still contains local producer stages. AQE's dispatched
            // stages are Remote leaves and must not be traversed or rewritten.
            let producer_tasks = boundary.input_stage().task_count();
            let node = if let Some(input) = boundary.input_stage().local_plan() {
                Arc::clone(&node).replace_children(
                    vec![lower_region(Arc::clone(input), producer_tasks, min_fanout)?],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?
            } else {
                node
            };
            let mut result = Arc::clone(&node);
            if can_lower_region && let Some(shuffle) = node.downcast_ref::<NetworkShuffleExec>() {
                let Partitioning::Hash(expressions, local_partitions) =
                    shuffle.properties().output_partitioning()
                else {
                    unreachable!()
                };
                let mut remote_expressions = expressions.clone();
                remote_expressions.push(Arc::new(Literal::new(ScalarValue::UInt64(Some(
                    REMOTE_SHUFFLE_SALT,
                )))));
                let remote_shuffle = Arc::new(NetworkShuffleExec::try_new_two_level(
                    shuffle.input_stage.clone(),
                    Arc::clone(shuffle.properties()),
                    Partitioning::Hash(remote_expressions, 1),
                )?);
                result = Arc::new(RepartitionExec::try_new(
                    remote_shuffle,
                    Partitioning::Hash(expressions.clone(), *local_partitions),
                )?);
            }
            Ok(Transformed::new(result, true, TreeNodeRecursion::Jump))
        })?
        .data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::DistributedExec;
    use crate::distributed_planner::prepare_network_boundaries::prepare_network_boundaries;
    use crate::test_utils::localhost::start_localhost_context;
    use crate::test_utils::property_based::compare_result_set;
    use crate::{DefaultSessionBuilder, NetworkCoalesceExec};
    use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::hash_utils::create_hashes;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{collect, displayable};

    // Result equality cannot detect correlated hashes: queries can be correct
    // while most task/lane pairs remain idle. Keep this one focused kernel test.
    #[test]
    fn numeric_salt_decorrelates_power_of_two_levels() -> Result<()> {
        let keys: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..4096));
        let salt: ArrayRef = Arc::new(UInt64Array::from_value(REMOTE_SHUFFLE_SALT, 4096));
        let mut remote_hashes = vec![0; 4096];
        let mut local_hashes = vec![0; 4096];
        create_hashes(
            [keys.as_ref(), salt.as_ref()],
            REPARTITION_RANDOM_STATE.random_state(),
            &mut remote_hashes,
        )?;
        create_hashes(
            [keys.as_ref()],
            REPARTITION_RANDOM_STATE.random_state(),
            &mut local_hashes,
        )?;
        let mut occupancy = [[false; 4]; 4];
        for (remote, local) in remote_hashes.into_iter().zip(local_hashes) {
            occupancy[(remote % 4) as usize][(local % 4) as usize] = true;
        }
        assert!(occupancy.into_iter().flatten().all(|occupied| occupied));
        Ok(())
    }

    // Lowering only the unordered join input would give its rows different task
    // ownership. Exercise the entire distributed execution, not just the flag.
    #[tokio::test]
    async fn asymmetric_ordered_join_keeps_both_inputs_single() -> Result<()> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from_iter_values(0..128))],
        )?;
        let expected = collect(ordered_join(&batch, false)?, ctx.task_ctx()).await?;
        let lowered = lower_two_level_shuffles(ordered_join(&batch, true)?, 2, 128)?;
        let shape = displayable(lowered.as_ref()).indent(true).to_string();
        assert!(!shape.contains("mode=two-level"), "{shape}");
        assert_eq!(shape.matches("NetworkShuffleExec:").count(), 2);
        let head = Arc::new(CoalescePartitionsExec::new(Arc::new(
            NetworkCoalesceExec::try_new(lowered, 2, 1)?,
        )));
        let plan = Arc::new(DistributedExec::new(prepare_network_boundaries(head)?));
        let actual = collect(plan, ctx.task_ctx()).await?;
        compare_result_set(&Ok(actual), &Ok(expected))
    }

    fn ordered_join(batch: &RecordBatch, distributed: bool) -> Result<Arc<dyn ExecutionPlan>> {
        let mut inputs = vec![];
        for ordered in [true, false] {
            let mut input: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
            if ordered {
                input = Arc::new(SortExec::new(
                    LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
                        "key", 0,
                    )))])
                    .unwrap(),
                    input,
                ));
            }
            input = Arc::new(RepartitionExec::try_new(
                input,
                // N=1, M=2, K=64 meets the fanout floor on both join inputs.
                Partitioning::Hash(vec![Arc::new(Column::new("key", 0))], 64),
            )?);
            assert_eq!(input.properties().output_ordering().is_some(), ordered);
            inputs.push(if distributed {
                Arc::new(NetworkShuffleExec::try_new(input, 1)?) as Arc<dyn ExecutionPlan>
            } else {
                input
            });
        }
        Ok(Arc::new(HashJoinExec::try_new(
            inputs.remove(0),
            inputs.remove(0),
            vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }
}
