use crate::execution_plans::PartitionIsolatorExec;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, internal_err};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Builds one physical plan per task for a stage.
///
/// The current specialization pass rewrites `PartitionIsolatorExec` nodes into
/// task-fixed `PartitionIsolatorExec::for_task(...)` nodes, fixing the partition
/// group at plan build time for each task.
pub(crate) fn build_task_specialized_stage_plans(
    plan: &Arc<dyn ExecutionPlan>,
    task_count: usize,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    (0..task_count)
        .map(|task_index| specialize_plan_for_task(plan, task_index, task_count))
        .collect()
}

fn specialize_plan_for_task(
    plan: &Arc<dyn ExecutionPlan>,
    task_index: usize,
    task_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    Arc::clone(plan)
        .transform_up(|node| {
            let Some(isolator) = node.as_any().downcast_ref::<PartitionIsolatorExec>() else {
                return Ok(Transformed::no(node));
            };

            let input = Arc::clone(&isolator.input);
            let input_partitions = input.output_partitioning().partition_count();
            if task_index >= task_count {
                return internal_err!(
                    "invalid task specialization index {task_index} >= {task_count}"
                );
            }
            let partition_group =
                PartitionIsolatorExec::partition_group(input_partitions, task_index, task_count);
            let specialized: Arc<dyn ExecutionPlan> =
                Arc::new(PartitionIsolatorExec::for_task(input, partition_group));
            Ok(Transformed::yes(specialized))
        })
        .data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::PartitionIsolatorExec;
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::datatypes::Schema;

    #[test]
    fn returns_one_plan_per_task() {
        let exec: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new_partitioned(
            vec![vec![], vec![], vec![], vec![]],
            Arc::new(Schema::empty()),
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(PartitionIsolatorExec::new(exec, 2));

        let plans = build_task_specialized_stage_plans(&plan, 2).unwrap();

        assert_eq!(plans.len(), 2);
        assert!(
            plans[0]
                .as_any()
                .downcast_ref::<PartitionIsolatorExec>()
                .is_some()
        );
        assert!(
            plans[1]
                .as_any()
                .downcast_ref::<PartitionIsolatorExec>()
                .is_some()
        );
    }
}