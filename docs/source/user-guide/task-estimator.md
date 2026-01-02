# Building a TaskEstimator

The `TaskEstimator` trait controls how many distributed tasks the planner allocates to each stage of the query plan.

The number of tasks is assigned to the different stages in a bottom-to-top fashion. You can refer to the
[Plan Annotation docs](https://github.com/datafusion-contrib/datafusion-distributed/blob/75b4e73e9052c6596b9d1744ce2bdfa6cbc010d3/src/distributed_planner/plan_annotator.rs)
for an explanation on how this works. A `TaskEstimator` is what hints this process how many tasks should be
used.

While a default implementation exists for file-based `DataSourceExec` nodes (those backed by `FileScanConfig`), you
can provide custom `TaskEstimator` implementations for your own `ExecutionPlan` types.

## Providing your own TaskEstimator

Providing a `TaskEstimator` allows you to do two things:

1. Tell the distributed planner how many tasks should be used for your own `ExecutionPlan`s.
2. Tell the distributed planner how to "scale up" your `ExecutionPlan` in order to account for it running in
   multiple distributed tasks.

If your custom nodes will execute in a distributed manner, you must handle this during execution. When your
`TaskEstimator` specifies N tasks for a node, your execution logic must respond to the
[DistributedTaskContext](https://github.com/datafusion-contrib/datafusion-distributed/blob/75b4e73e9052c6596b9d1744ce2bdfa6cbc010d3/src/stage.rs#L137-L137)
present in DataFusion's `TaskContext` to determine which subset of data this task should process.

There's an example of how to do that in the `examples/` folder:

- [custom_execution_plan.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_execution_plan.rs) -
  A complete example showing how to implement a custom execution plan (`numbers(start, end)` table function)
  that works with distributed DataFusion, including a custom codec and TaskEstimator.
