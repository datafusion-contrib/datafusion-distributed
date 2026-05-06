use crate::PartitionIsolatorExec;
use crate::distributed_planner::TaskEstimator;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Returns a per-task specialization of `plan`. Walks the plan top-down and, for every
/// [PartitionIsolatorExec] found, asks the registered [TaskEstimator]s to specialize the
/// wrapped leaf so it only carries the per-partition state assigned to
/// `(task_index, task_count)`. The [PartitionIsolatorExec] wrapper is preserved (so plan
/// shape across coordinator and worker stays in sync — important for metrics rewriting)
/// and the runtime skipping path keeps working: skipped partitions just yield empty
/// streams now that their state has been dropped from the payload.
///
/// When no estimator handles a particular [PartitionIsolatorExec], the original leaf is
/// left in place and the runtime skipping behavior is preserved as a safe fallback.
pub(crate) fn specialize_plan_for_task(
    plan: &Arc<dyn ExecutionPlan>,
    task_index: usize,
    task_count: usize,
    estimator: &dyn TaskEstimator,
    cfg: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if task_count <= 1 {
        return Ok(Arc::clone(plan));
    }
    let result = Arc::clone(plan).transform_down(|node| {
        let Some(isolator) = node.as_any().downcast_ref::<PartitionIsolatorExec>() else {
            return Ok(Transformed::no(node));
        };
        let input_partitions = isolator.input.output_partitioning().partition_count();
        let partition_group =
            PartitionIsolatorExec::partition_group(input_partitions, task_index, task_count);
        match estimator.specialize_leaf(&isolator.input, &partition_group, cfg) {
            Some(specialized_child) => {
                let new_isolator = Arc::new(PartitionIsolatorExec::new(
                    specialized_child,
                    isolator.n_tasks,
                ));
                Ok(Transformed::yes(new_isolator as Arc<dyn ExecutionPlan>))
            }
            None => Ok(Transformed::no(node)),
        }
    })?;
    Ok(result.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_planner::{TaskEstimation, TaskRoutingContext};
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::error::DataFusionError;
    use std::sync::Mutex;

    fn build_mock_leaf(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let data = (0..partitions).map(|_| vec![]).collect();
        Arc::new(MockExec::new_partitioned(data, schema))
    }

    fn build_isolated(partitions: usize, n_tasks: usize) -> Arc<dyn ExecutionPlan> {
        let leaf = build_mock_leaf(partitions);
        Arc::new(PartitionIsolatorExec::new(leaf, n_tasks))
    }

    fn count_isolators(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        let _ = plan.apply(|node| {
            if node.as_any().is::<PartitionIsolatorExec>() {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        });
        count
    }

    /// A test estimator whose `specialize_leaf` returns a smaller [MockExec] sized to the
    /// passed-in partition group. Records the partition groups it received per task.
    struct RecordingEstimator {
        seen: Mutex<Vec<Vec<usize>>>,
    }

    impl RecordingEstimator {
        fn new() -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
            }
        }
        fn calls(&self) -> Vec<Vec<usize>> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl TaskEstimator for RecordingEstimator {
        fn task_estimation(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _cfg: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            None
        }
        fn scale_up_leaf_node(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _task_count: usize,
            _cfg: &ConfigOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            None
        }
        fn specialize_leaf(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            partition_group: &[usize],
            _cfg: &ConfigOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            self.seen.lock().unwrap().push(partition_group.to_vec());
            // Preserve the input partition count; just return a same-shape mock leaf.
            Some(build_mock_leaf(
                plan.output_partitioning().partition_count(),
            ))
        }
        fn route_tasks(
            &self,
            _routing_ctx: &TaskRoutingContext<'_>,
        ) -> Result<Option<Vec<url::Url>>> {
            Ok(None)
        }
    }

    /// An estimator that never specializes — used to verify the safe fallback.
    struct NoopEstimator;
    impl TaskEstimator for NoopEstimator {
        fn task_estimation(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _cfg: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            None
        }
        fn scale_up_leaf_node(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _task_count: usize,
            _cfg: &ConfigOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            None
        }
    }

    #[test]
    fn test_preserves_isolator_when_estimator_specializes() -> Result<(), DataFusionError> {
        // 6 partitions, 3 tasks → each task should see partitions [0,1], [2,3], [4,5].
        let plan = build_isolated(6, 3);
        assert_eq!(count_isolators(&plan), 1);

        let estimator = RecordingEstimator::new();
        let cfg = ConfigOptions::default();

        for task_i in 0..3 {
            let specialized = specialize_plan_for_task(&plan, task_i, 3, &estimator, &cfg)?;
            assert_eq!(
                count_isolators(&specialized),
                1,
                "task {task_i}: PartitionIsolatorExec wrapper should be preserved \
                 so plan shape stays in sync with the coordinator's view"
            );
            // Output partition count from the isolator wrapper is the task's group size.
            assert_eq!(
                specialized.output_partitioning().partition_count(),
                2,
                "task {task_i}: isolator should expose the task's 2 partitions"
            );
        }

        // Each task saw exactly its own partition group.
        assert_eq!(
            estimator.calls(),
            vec![vec![0, 1], vec![2, 3], vec![4, 5]],
            "specialize_leaf must receive disjoint, ordered partition groups"
        );
        Ok(())
    }

    #[test]
    fn test_preserves_isolator_when_estimator_returns_none() -> Result<(), DataFusionError> {
        // Without a specializer, the isolator must remain so the runtime fallback handles it.
        let plan = build_isolated(6, 3);
        assert_eq!(count_isolators(&plan), 1);

        let estimator = NoopEstimator;
        let cfg = ConfigOptions::default();

        for task_i in 0..3 {
            let specialized = specialize_plan_for_task(&plan, task_i, 3, &estimator, &cfg)?;
            assert_eq!(count_isolators(&specialized), 1, "task {task_i}");
            // PartitionIsolatorExec exposes partition_group.len() partitions to upper nodes.
            assert_eq!(specialized.output_partitioning().partition_count(), 2);
        }
        Ok(())
    }

    #[test]
    fn test_noop_for_single_task() -> Result<(), DataFusionError> {
        // task_count <= 1: bypass entirely, return the input as-is.
        let plan = build_isolated(6, 3);
        let estimator = RecordingEstimator::new();
        let cfg = ConfigOptions::default();
        let specialized = specialize_plan_for_task(&plan, 0, 1, &estimator, &cfg)?;
        assert_eq!(count_isolators(&specialized), 1);
        assert!(estimator.calls().is_empty(), "estimator should not be invoked");
        Ok(())
    }
}
