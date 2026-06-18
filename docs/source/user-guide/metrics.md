# Collecting Runtime Metrics

In vanilla DataFusion, the runtime metrics of an `ExecutionPlan` (rows produced, time spent, bytes,
etc.) live on each node and can be inspected after execution. In a distributed query most of the plan
runs on remote workers, so those metrics need to be gathered back to the coordinator before you can
display them.

Distributed DataFusion does this for you, and exposes two functions you can use to build your own
EXPLAIN ANALYZE in application code.

## Enabling collection

Metrics collection across network boundaries is **on by default**. You can toggle it explicitly:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(/* ... */)
    .with_distributed_planner()
    .with_distributed_metrics_collection(true) // default is true
    .build();
```

When enabled, each worker streams the metrics for its tasks back to the coordinator on a dedicated
channel, so they are not lost even if the result stream is dropped early (for example by a `LIMIT`).

## Rendering a plan with metrics

Two functions, both exported from the crate root, do the work:

- `rewrite_distributed_plan_with_metrics(plan, format)` — folds every task's metrics back into the
  coordinator's copy of the plan. It waits for all worker metrics to arrive, so the result is always
  complete. The `format` is a `DistributedMetricsFormat`:
    - `Aggregated` — metrics from all tasks of a stage are summed/aggregated into one value per node.
    - `PerTask` — metric names are suffixed with the task id (`output_rows_0`, `output_rows_1`, …) so
      you can see each task individually.
- `display_plan_ascii(plan, show_metrics)` — renders the plan tree. Pass `true` to include the metrics
  attached to each node.

The order of operations matters: **the plan must be fully executed before its metrics are available.**

```rust
use datafusion::physical_plan::execute_stream;
use datafusion_distributed::{
    DistributedMetricsFormat, display_plan_ascii, rewrite_distributed_plan_with_metrics,
};
use futures::TryStreamExt;

// 1. Plan the query.
let plan = ctx.sql(sql).await?.create_physical_plan().await?;

// 2. Execute it to completion (collect, or otherwise fully drain the stream).
execute_stream(plan.clone(), ctx.task_ctx())?
    .try_collect::<Vec<_>>()
    .await?;

// 3. Fold the per-task metrics back into the plan...
let plan =
    rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated).await?;

// 4. ...and render it.
println!("{}", display_plan_ascii(plan.as_ref(), true));
```

This produces an EXPLAIN ANALYZE that spans the whole cluster — every stage and every node carries its
runtime metrics, including network-level metrics on the boundaries:

```
┌───── DistributedExec ── Tasks: t0:[p0] plan_bytes_sent_0=8.07 KB, plan_send_latency_avg_0=22.63ms, ...
│ SortPreservingMergeExec: [count(*)@0 DESC], fetch=5, metrics=[output_rows=5, elapsed_compute=391.83µs, ...]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=32, input_tasks=2, metrics=[elapsed_compute=5.86ms, bytes_transferred=20.1 KB, network_latency_p50=366.00µs, network_latency_p95=603.43µs, ...]
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p0..p15] plan_added_at_0=25.78ms, plan_finished_at_0=38.35ms, ...
  │ AggregateExec: mode=FinalPartitioned, gby=[MinTemp@0 as MinTemp], aggr=[count(Int64(1))], metrics=[output_rows=180, elapsed_compute=5.44ms, ...]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=2, metrics=[bytes_transferred=15.0 KB, ...]
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── Tasks: t0:[p0..p31] t1:[p0..p31] ...
    │ AggregateExec: mode=Partial, gby=[MinTemp@0 as MinTemp], aggr=[count(Int64(1))], metrics=[output_rows=249, ...]
    │     DistributedLeafExec: DataSourceExec: ..., metrics=[output_rows=366, bytes_scanned=5.40 K, ...]
    └──────────────────────────────────────────────────
```

> If `plan` is not a distributed plan (its root is not a `DistributedExec`),
> `rewrite_distributed_plan_with_metrics` returns it unchanged, so it is always safe to call.
