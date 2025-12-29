# Building a TaskEstimator

A `TaskEstimator` is a trait that tells the distributed planner how many distributed tasks should be used in the
different stages of the plan.

The number of tasks is assigned to the different stages in a bottom-to-top fashion. You can refer the
[Plan Annotation docs](https://github.com/datafusion-contrib/datafusion-distributed/blob/75b4e73e9052c6596b9d1744ce2bdfa6cbc010d3/src/distributed_planner/plan_annotator.rs)
for an explanation on how this works. A `TaskEstimator` is what hints this process how many tasks should be
used.

There is a default implementation that acts on `DataSourceExec` nodes baked by `FileScanConfig`s, however, as a user,
you may want to provide your own `TaskEstimator` implementation for your own `ExecutionPlan`s.

## Providing your own TaskEstimator

Providing a `TaskEstimator` allows you to do two things:

1. Tell the distributed planner how many tasks should be used for your own `ExecutionPlan`s.
2. Tell the distributed planner how to "scale up" your `ExecutionPlan` in order to account for it running in
   multiple distributed tasks.

Note that if your custom nodes are going to run distributed, you need to account for it at execution time.
If you build a `TaskEstimator` that tells the distributed planner that your node should run in N tasks, then
you need to react to the presence of a
[DistributedTaskCtx](https://github.com/datafusion-contrib/datafusion-distributed/blob/75b4e73e9052c6596b9d1744ce2bdfa6cbc010d3/src/stage.rs#L137-L137)
during execution.

There's an example of how to do that in the `examples/` folder:

- TODO: link to example
