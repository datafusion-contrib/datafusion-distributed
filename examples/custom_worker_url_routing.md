# Custom Worker URL Routing Example (cache affinity)

Demonstrates **custom task routing** for **cache affinity**: consistently routing each parquet file
to the *same* worker so that worker can serve it from an in-memory cache on repeat queries. This is
the
[`TaskEstimator::route_tasks`](../docs/source/user-guide/task-estimator.md#routing-tasks-to-specific-workers)
API.

## Scenario

A team runs repeated analytical queries over a set of regional parquet files (e.g. for a dashboard
or recurring report). Without cache affinity every query re-reads every file from disk. With it,
each file is pinned to a worker, so the second and subsequent queries are served entirely from
in-memory cache.

## What it shows

Rather than a custom execution plan, the example wraps DataFusion's native `FileScanConfig` inside
a `CachedFileScanConfig` data source using a **`PhysicalOptimizerRule`**. Any `register_parquet`
table gains caching transparently, with no changes to the table registration.

Routing is a two-step pipeline:

- `scale_up_leaf_node` flattens all files, hashes each file's path to a slot
  (`hash(path) % task_count`), and builds one `DataSourceExec(CachedFileScanConfig)` variant per
  slot. The same file always hashes to the same slot.
- `route_tasks` sorts the available worker URLs and maps slot `i` to `sorted_urls[i % n_workers]`.
  Each slot therefore always reaches the same worker.

Together these guarantee that each worker consistently reads the same set of files and its cache
stays warm across queries.

The worker-level cache is an `Arc<DashMap>` injected as a **session extension** at worker startup.
Since session extensions survive across task invocations (the `Arc` is cloned, not recreated), the
cache is warm on the second query even though the plan is serialised and sent fresh each time.

## Components

**`CachedFileScanConfig`** — a `DataSource` wrapper around `FileScanConfig`. `open()` checks the
worker's session extension cache first; on a miss it reads via the inner config and populates the
cache asynchronously through a `RecordBatchReceiverStreamBuilder`.

**`CachedFileScanConfigTaskEstimator`** — the `TaskEstimator`. `scale_up_leaf_node` produces one
variant per task slot; `route_tasks` pins each slot to a worker by sorted-URL index.

**`CachedFileScanCodec`** — a `PhysicalExtensionCodec` that round-trips `CachedFileScanConfig` by
encoding the inner `FileScanConfig` as a plain `DataSourceExec` using DataFusion's default codec,
then re-wrapping on decode. No custom proto schema is needed.

**`CachedFileScanConfigRule`** — a `PhysicalOptimizerRule` that rewrites every leaf
`DataSourceExec(FileScanConfig)` to `DataSourceExec(CachedFileScanConfig)`.

Workers are spawned with `spawn_worker_service` and discovered with `LocalHostWorkerResolver`, both
from the `integration` feature.

## Usage

```bash
cargo run \
  --features integration \
  --example custom_worker_url_routing \
  'SELECT "RainToday", COUNT(*) AS days, AVG("Rainfall") AS avg_mm FROM weather GROUP BY "RainToday"'
```

```
=== cold pass done after 121ms ===
+-----------+------+----------------------+
| RainToday | days | avg_mm               |
+-----------+------+----------------------+
| Yes       | 66   | 7.663636363636365    |
| No        | 300  | 0.056666666666666664 |
+-----------+------+----------------------+
=== warm pass done after 9ms ===
+-----------+------+----------------------+
| RainToday | days | avg_mm               |
+-----------+------+----------------------+
| Yes       | 66   | 7.663636363636365    |
| No        | 300  | 0.056666666666666664 |
+-----------+------+----------------------+
```

The warm pass returns the same rows in ~10× less time because `open()` returns them from the
worker's `DashMap` cache rather than reading parquet off disk. Each worker owns a subset of the
parquet files from `testdata/weather`, and the same files always route there on every query because
the hash-based slot assignment is stable.

To inspect the distributed plan:

```bash
cargo run --features integration --example custom_worker_url_routing \
    'SELECT "RainToday", COUNT(*) AS days, AVG("Rainfall") AS avg_mm FROM weather GROUP BY "RainToday"' \
    --show-distributed-plan
```

```
┌───── DistributedExec
│ CoalescePartitionsExec
│   [Stage 2] => NetworkCoalesceExec: output_partitions=32, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── tasks=2, partitions=16
  │ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as days, avg(weather.Rainfall)@2 as avg_mm]
  │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1)), avg(weather.Rainfall)]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=3
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── tasks=3, partitions=32
    │ RepartitionExec: partitioning=Hash([RainToday@0], 32), input_partitions=1
    │   AggregateExec: mode=Partial, gby=[RainToday@1 as RainToday], aggr=[count(Int64(1)), avg(weather.Rainfall)]
    │     DistributedLeafExec: DataSourceExec: file_groups={3 groups: [...]}, projection=[Rainfall, RainToday], file_type=parquet
    └──────────────────────────────────────────────────
```

Stage 1 runs on all three workers (one parquet file per task, each pinned to its worker by the
hash-based routing). Stage 2 runs the final aggregation on two workers after a hash-shuffle.
