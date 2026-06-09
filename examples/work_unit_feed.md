# Work Unit Feed Example

Demonstrates a **work unit feed**: a distributed leaf node whose work is discovered on the
coordinator *at runtime* and streamed to the workers as the query executes, instead of being fully
known at planning time.

See the [Work Unit Feeds user guide](../docs/source/user-guide/work-unit-feeds.md) for the concepts.

## Scenario

The example implements a `scan(...)` table function that models an external, paginated data source
(think a message queue, a paginated HTTP API, or a catalog that hands out object-store keys on demand).
While the query runs, the coordinator "discovers" chunks of work and streams a small descriptor for
each chunk to whichever worker owns that partition. The worker turns each descriptor into rows.

## Components

**ChunkFeedProvider** – the coordinator-side `WorkUnitFeedProvider`. Its `feed(partition, ctx)` returns
a stream of `Chunk` descriptors for that partition. (Here it sleeps briefly between chunks to simulate
runtime discovery; a real connector would poll an external system.)

**RemoteScanExec** – a custom leaf `ExecutionPlan` that holds a `WorkUnitFeed<ChunkFeedProvider>` and,
in `execute()`, consumes `feed.feed(partition, ctx)?` and turns each `Chunk` into a `RecordBatch`.

**RemoteScanExecCodec** – a `PhysicalExtensionCodec` that serializes the node across the network. Note
it encodes only the feed *handle* via `WorkUnitFeed::to_proto()`; the provider stays on the coordinator.

**RemoteScanTaskEstimator** – a `TaskEstimator` that tells the planner how many tasks the leaf stage
should be distributed across.

The feed is registered with `with_distributed_work_unit_feed(|exec: &RemoteScanExec| Some(&exec.feed))`
so the planner can find it inside the plan and wire coordinator → worker delivery.

## Usage

This example uses the in-memory cluster used in integration testing, so it needs `--features integration`.

The `scan(task_count, 'chunks_p0', 'chunks_p1', ...)` arguments are: the desired task count, then one
comma-separated list of chunk sizes per partition. The number of partition arguments must be a multiple
of the task count. The scan emits one row per row in each chunk, tagged with the producing task and
partition.

### Render the distributed plan

```bash
cargo run \
  --features integration \
  --example work_unit_feed \
  "SELECT * FROM scan(2, '3,1', '2', '4', '1,1') ORDER BY task, partition" \
  --show-distributed-plan
```

```
┌───── DistributedExec ── Tasks: t0:[p0]
│ SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST]
│   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
  │ SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST], preserve_partitioning=[true]
  │   RemoteScanExec: tasks=2, partition_chunks=[[3, 1], [2], [4], [1, 1]]
  └──────────────────────────────────────────────────
```

The four partition arguments are split across the two requested tasks (two partitions each).

### Execute a query

```bash
cargo run \
  --features integration \
  --example work_unit_feed \
  "SELECT count(*) as cnt, task FROM scan(2, '3,1', '2', '4', '1,1') GROUP BY task ORDER BY task"
```

```
+-----+------+
| cnt | task |
+-----+------+
| 6   | 0    |
| 6   | 1    |
+-----+------+
```

Task 0 owns partitions `[3,1]` and `[2]` (3+1+2 = 6 rows); task 1 owns `[4]` and `[1,1]` (4+1+1 = 6 rows).
Every one of those rows is produced from a chunk descriptor the coordinator streamed to the worker while
the query was running.
