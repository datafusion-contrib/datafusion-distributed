# Custom Distributed Plan Example

Demonstrates how to **build your own distributed plan by injecting network boundaries directly**,
instead of relying on the automatic distributed planner to decide where the stages go.

See the [Custom Distributed Plans user guide](../docs/source/user-guide/custom-distributed-plans.md)
for the concepts.

## What it shows

`NetworkShuffleExec` and `NetworkCoalesceExec` are public, constructible nodes. If a physical plan
already contains network boundaries when it reaches the distributed planner, the planner does **not**
distribute it on its own — it just finalises the boundaries you placed and wraps the result in a
`DistributedExec`. The natural place to inject them is a `PhysicalOptimizerRule`.

This example builds a **progressive partial-reduction tree** for a `GROUP BY` aggregation over the
`weather` parquet table. Instead of gathering every leaf task into a single node with one wide coalesce,
it reduces the data at every level of the tree:

```text
 Final            (1 task)   <- finishes the aggregation
   NetworkCoalesceExec  M -> 1
 PartialReduce    (M tasks)  <- merges partial states, shrinking the data again
   NetworkCoalesceExec  N -> M
 Partial          (N tasks)  <- first partial reduce, one task per slice of files
   DistributedLeafExec(weather)
```

The key node is `AggregateExec(mode=PartialReduce)`: unlike a plain coalesce, which only concatenates
partition streams, `PartialReduce` merges partial-aggregate states into fewer partial-aggregate states,
so less data crosses each network hop. A single `Final` aggregation on the root finishes the job. This
works for stateful aggregates too — e.g. `avg(...)` merges its (sum, count) states correctly through
the tree.

## Components

**PartialReductionTreeRule** – a `PhysicalOptimizerRule` that matches the finalising aggregate of a
two-phase aggregation, reuses the group-by / aggregate expressions from the existing `Partial` node,
and rebuilds the pipeline as `Partial → NetworkCoalesce(N→M) → PartialReduce → NetworkCoalesce(M→1) → Final`.

**Leaf splitting (automatic)** – the rule leaves the parquet scan as a plain `DataSourceExec`. The
distributed planner runs the registered `TaskEstimator` over each stage's leaves, so the default
file-scan estimator wraps the leaf in a `DistributedLeafExec` and hands each leaf-stage task its own
slice of the parquet files — no manual leaf handling in the example.

## Usage

This example uses the in-memory cluster used in integration testing, so it needs `--features integration`.
Run it from the repo root so the `testdata/weather` files resolve.

```bash
cargo run \
  --features integration \
  --example custom_distributed_partial_reduction_tree \
  'SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday"' \
  --show-distributed-plan
```

```
┌───── DistributedExec
│ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
│   AggregateExec: mode=Final, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
│     CoalescePartitionsExec
│       [Stage 2] => NetworkCoalesceExec: output_partitions=2, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── tasks=2, partitions=2
  │ AggregateExec: mode=PartialReduce, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
  │   CoalescePartitionsExec
  │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── tasks=3, partitions=9
    │ AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
    │   DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[.../weather/result-000000.parquet], [.../weather/result-000001.parquet], [.../weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
    └──────────────────────────────────────────────────
```

Stage 1 is the first partial reduce on 3 tasks; note the `DistributedLeafExec` — the planner's
`TaskEstimator` wrapped the parquet scan automatically, even though we only injected the boundaries.
Stage 2 gathers 3 → 2 and partially reduces again; the root gathers 2 → 1 and finishes the aggregation.
Running it produces the answer:

```bash
cargo run --features integration --example custom_distributed_partial_reduction_tree \
  'SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday"'
```

```
+-----------+----------+
| RainToday | count(*) |
+-----------+----------+
| No        | 300      |
| Yes       | 66       |
+-----------+----------+
```

The width of the tree is configurable, and a stateful aggregate works too:

```bash
cargo run \
  --features integration \
  --example custom_distributed_partial_reduction_tree \
  'SELECT "RainToday", avg("MinTemp") FROM weather GROUP BY "RainToday"' \
  --leaf-tasks 3 --mid-tasks 2
```
