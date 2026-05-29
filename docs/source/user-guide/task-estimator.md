# Building a TaskEstimator

The `TaskEstimator` trait controls how many distributed tasks the planner allocates to each stage of the query plan.

The number of tasks is assigned to the different stages in a bottom-up fashion. You can refer to the
[Plan Annotation docs](https://github.com/datafusion-contrib/datafusion-distributed/blob/75b4e73e9052c6596b9d1744ce2bdfa6cbc010d3/src/distributed_planner/plan_annotator.rs)
for an explanation on how this works. A `TaskEstimator` is what hints this process how many tasks should be
used.

While a default implementation exists for file-based `DataSourceExec` nodes (those backed by `FileScanConfig`), you
can provide custom `TaskEstimator` implementations for your own `ExecutionPlan` types.

## Providing your own TaskEstimator

Providing a `TaskEstimator` allows you to do two things:

1. Tell the distributed planner how many tasks should be used for your own `ExecutionPlan`s
   (`task_estimation`).
2. Prepare the leaf node at planning time for distributed execution (`scale_up_leaf_node`).
   The recommended approach is to return a `DistributedLeafExec` wrapping your original plan
   along with `N` per-task variants (one per task). `DistributedLeafExec` is transparent to
   network boundaries—it exposes the same partition count as the original—and the task spawner
   automatically replaces it with the appropriate per-task variant before serialising the plan
   and sending it to a worker.

   If your leaf node already handles task dispatch internally (e.g., by reading
   `DistributedTaskContext.task_index` in `execute()`), you can omit `DistributedLeafExec` and
   simply return the prepared plan directly from `scale_up_leaf_node`.

There's a complete example in the `examples/` folder:

- [custom_execution_plan.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_execution_plan.rs) -
  A complete example showing how to implement a custom execution plan (`numbers(start, end)` table function)
  that works with distributed DataFusion, including a custom codec and TaskEstimator.
