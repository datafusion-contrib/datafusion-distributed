# Building a TaskEstimator

The `TaskEstimator` trait controls how many distributed tasks the planner allocates to each stage of the query plan.

The number of tasks is assigned to the different stages in a bottom-up fashion. See
[How a Distributed Plan is Built](how-a-distributed-plan-is-built.md) for the overall picture, and the
[
`inject_network_boundaries`](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/src/distributed_planner/inject_network_boundaries.rs)
source for the details. A `TaskEstimator` is what hints this process how many tasks should be used.

While a default implementation exists for file-based `DataSourceExec` nodes (those backed by `FileScanConfig`), you
can provide custom `TaskEstimator` implementations for your own `ExecutionPlan` types.

## Providing your own TaskEstimator

Providing a `TaskEstimator` allows you to do three things:

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
3. Route each task to a specific worker URL (`route_tasks`) — see
   [Routing tasks to specific workers](#routing-tasks-to-specific-workers) below.

There's a complete example in the `examples/` folder:

- [custom_execution_plan.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_execution_plan.rs) -
  A complete example showing how to implement a custom execution plan (`numbers(start, end)` table function)
  that works with distributed DataFusion, including a custom codec and TaskEstimator.

> A `TaskEstimator` decides a leaf node's work **at planning time**. If your leaf's units of work are
> only discovered **at runtime** (paginated APIs, queues, progressive discovery), see
> [Work Unit Feeds](work-unit-feeds.md) for the complementary mechanism.

## Routing tasks to specific workers

By default, the planner spreads a stage's tasks across the available workers round-robin. When a task's
data has a *home* — a worker that already holds it in a cache or on local disk — you can send the task
**there** instead by implementing `TaskEstimator::route_tasks`, which returns the worker URL for each task
(returning `None`, the default, keeps the round-robin behaviour). Routing pairs naturally with
`scale_up_leaf_node`: that decides *what* data task `i` reads, and `route_tasks` decides *where* it runs.

For a complete, runnable walkthrough — parquet files consistently routed to workers (by rendezvous
hashing of the file path) so each worker can serve them from an in-memory cache on repeat queries — see
the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
