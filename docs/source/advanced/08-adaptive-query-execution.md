# Using Adaptive Query Execution

Adaptive Query Execution (AQE) lets Distributed DataFusion choose the number of
tasks for each stage while the query is running. Instead of assigning every
stage a task count statically at planning time, the coordinator samples each
producer at its stage boundary and uses the observed data to size the stage
above it.

AQE currently adapts **distributed task counts**. It does not rerun DataFusion's
physical optimizer or replace joins, aggregates, or other operators during
execution.
See [How Adaptive Query Execution Works](../learn/03-how-adaptive-query-execution-works.md)
for the execution flow and sampling model.

## Enable AQE

Enable AQE on the coordinating session with
`with_distributed_dynamic_task_count(true)`:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(worker_resolver)
    .with_distributed_planner()
+   .with_distributed_dynamic_task_count(true)?
    .build();
```

## Configuration

The following settings affect AQE decisions:

| Setting                                   | Default | Effect                                                                                                                                                                                       |
|-------------------------------------------|--------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `distributed.dynamic_task_count`          | `false` | Enables runtime task-count selection.                                                                                                                                                        |
| `distributed.dynamic_bytes_per_partition` |  16 MiB | Sets the compute-cost budget per output partition. Lower values generally produce more tasks; higher values produce fewer. Configure it with `with_distributed_dynamic_bytes_per_partition`. |

## Custom data sources

AQE depends on both planning-time statistics and execution-time metrics. A
custom leaf should provide all of the following:

- A registered `TaskEstimator` that implements `scale_up_leaf_node` for the task
  count selected by AQE. Without an estimator, the planner treats an unknown
  leaf as limited to one task.
- A useful implementation of `ExecutionPlan::partition_statistics` in the custom
  data source. The more accurate the statistics are, the better the decisions
  AQE can make.
- Standard DataFusion execution metrics for the custom data source, including an
  `output_rows` metric. AQE uses the output-row count of leaves on the stage's
  driver path to measure sampling progress. If that metric is absent or not
  updated as batches flow, the coordinator cannot reliably extrapolate the final
  stage output.

Most of these requirements benefit the custom data source even when Distributed
DataFusion is not involved, so taking the time to provide good implementations
is always worthwhile.

## Considerations

There are a few things to take into account when using AQE:

### Visualizing physical plans

Plan visualization works out of the box with queries using AQE, but visualizing
an unexecuted plan is not very useful. Task-count decisions have not yet been
made, and network boundaries have not yet been injected, so you will effectively
be visualizing the single-node plan before distribution.

This is not a bug; it is how AQE works: the final physical plan is decided
dynamically at runtime.

When using AQE, prefer reading the final plan after the query has executed and
all metrics have been collected from remote workers. See
[Collecting metrics from workers](../user-guide/05-metrics.md).

### Distributed UNION operations

AQE relies on runtime sampling to estimate the size of the different stages
involved in a query. When a `UNION` has multiple children and they all pull data
from remote stages, the stage cannot eagerly yield data from faster children.
It must wait for every child to be sampled before it can start.

This typically happens in systems that abuse `UNION` operations to model range
partitioning. There are many reasons to move away from this pattern, including
schema mismatches between children that silently introduce data loss, discarded
partitioning information, too many Tokio tasks caused by large child counts,
huge plans that are expensive to serialize, and unreadable `EXPLAIN` output. If
your system relies on this pattern, note that one consequence is an increased
time to first batch, even if the total query duration is unaffected.
