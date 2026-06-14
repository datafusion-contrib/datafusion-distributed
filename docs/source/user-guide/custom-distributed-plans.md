# Building Custom Distributed Plans

By default, the [distributed planner](how-a-distributed-plan-is-built.md) decides where to place network
boundaries: it walks your single-node physical plan and injects `NetworkShuffleExec`,
`NetworkCoalesceExec`, and `NetworkBroadcastExec` nodes at the points it thinks make sense.

You don't have to leave that decision to the planner. `NetworkShuffleExec` and `NetworkCoalesceExec` are
**public, constructible nodes**, so you can place network boundaries yourself and build whatever stage
topology your workload needs.

## How it works

The distributed planner runs as a query planner that post-processes the physical plan. Its first check
is whether the plan **already** contains network boundaries:

- If it does, the planner assumes you placed them deliberately. It does **not** try to distribute the
  plan itself — it only finalises your boundaries (giving each stage a unique id and eliding any
  boundary that turns out to connect a single producer task to a single consumer task) and wraps the
  result in a `DistributedExec`.
- If it doesn't, the planner runs its normal automatic distribution passes.

So building a custom distributed plan is a matter of getting your network boundaries into the physical
plan before the distributed planner sees it. The idiomatic way to do that is a DataFusion
[`PhysicalOptimizerRule`](https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html),
which runs while the physical plan is being built:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    // Your rule injects NetworkShuffleExec / NetworkCoalesceExec into the plan...
    .with_physical_optimizer_rule(Arc::new(MyBoundaryInjectionRule))
    // ...and the distributed planner finalises whatever boundaries it finds.
    .with_distributed_worker_resolver(/* ... */)
    .with_distributed_planner()
    .with_distributed_user_codec(MyLeafCodec)
    .build();
```

## The two boundary nodes

Both nodes connect a *producer* stage (below, running on `producer_tasks` tasks) to a *consumer* stage
(above). The placeholder stage ids they are constructed with are filled in for you during finalisation.

### `NetworkShuffleExec`

```rust
NetworkShuffleExec::try_new(input: Arc<dyn ExecutionPlan>, producer_tasks: usize) -> Result<Self>
```

A network shuffle — the distributed equivalent of a `RepartitionExec`. It **repartitions** data across
tasks: each consumer task gathers data from every producer task, hash-partitioned by key. Its `input`
**must** be a `RepartitionExec` with `Hash` partitioning. Use it for shuffle-based aggregations and
partitioned joins.

### `NetworkCoalesceExec`

```rust
NetworkCoalesceExec::try_new(
    input: Arc<dyn ExecutionPlan>,
    producer_tasks: usize,
    consumer_tasks: usize,
) -> Result<Self>
```

A network coalesce — the distributed equivalent of a `CoalescePartitionsExec`. It **concatenates**
partitions across tasks **without** repartitioning: each consumer task reads a contiguous group of
producer tasks. It is meant to sit right above a partition-collecting node such as
`CoalescePartitionsExec` or `SortPreservingMergeExec`. Use it to reduce the width of a stage (gather
many tasks into fewer).

> The caller is responsible for keeping the counts consistent: the `consumer_tasks` of one boundary must
> match the `producer_tasks` of the boundary immediately above it.

## Leaf data splitting still happens automatically

Even when you inject the boundaries yourself, the distributed planner runs the registered
[`TaskEstimator`](task-estimator.md) over each stage's leaves and calls `scale_up_leaf_node` with the
stage's task count. So a parquet `DataSourceExec` is wrapped in a `DistributedLeafExec` (with one
per-task file-group variant) by the default file-scan estimator, and a custom leaf is split by whatever
`TaskEstimator` you registered for it — exactly as in the automatic path. You only place the boundaries;
the leaves are scaled for you.

This means a custom leaf node still needs its `TaskEstimator` (and its `scale_up_leaf_node` /
`DistributedTaskContext`-based dispatch) registered, just as it would for automatic planning — you do
**not** need to hand-build `DistributedLeafExec` in your boundary-injection rule.

## Example: a progressive partial-reduction tree

A common reason to build the topology yourself is to control how a stage fans in. Instead of gathering
`N` leaf tasks into a single node with one wide coalesce, you can build a **reduction tree** that shrinks
the data at every level:

```text
 Final            (1 task)   — finishes the aggregation
   NetworkCoalesceExec  M → 1
 PartialReduce    (M tasks)  — merges partial states, shrinking the data again
   NetworkCoalesceExec  N → M
 Partial          (N tasks)  — first partial reduce, one task per slice of the input
   DistributedLeafExec
```

The node that makes this *progressive* (rather than just a fan-in of concatenations) is
`AggregateExec` with `AggregateMode::PartialReduce`: it merges partial-aggregate states into fewer
partial-aggregate states, so the volume crossing each network hop keeps dropping. A single `Final`
aggregation on the root finishes the job — and because the merge happens on partial states, stateful
aggregates like `avg(...)` stay correct.

There is a complete, runnable example in the `examples/` folder:

- [custom_distributed_partial_reduction_tree.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_distributed_partial_reduction_tree.rs) —
  a `PhysicalOptimizerRule` that rewrites a `GROUP BY` aggregation over a parquet table into the tree
  above (`Partial → NetworkCoalesce → PartialReduce → NetworkCoalesce → Final`). It only injects the
  boundaries; the planner's `TaskEstimator` splits the parquet leaf across the leaf-stage tasks
  automatically.
