# Custom Execution Plan Example

Demonstrates how to create a distributed custom execution plan with a `numbers(start, end)` table function.

## Components

**NumbersTableFunction** – Table function callable in SQL: `SELECT * FROM numbers(1, 100)`

**NumbersExec** – Execution plan with `ranges_per_task: Vec<Range<i64>>` storing one range per task.
Uses `DistributedTaskContext` to determine which range to generate.

**NumbersExecCodec** – Protobuf-based serialization implementing `PhysicalExtensionCodec`.
Must be registered in the `SessionStateBuilder` that initiates the query as well as the one used by `Worker`s.

**NumbersTaskEstimator** – Controls distributed parallelism:

- `task_estimation()` - Returns how many tasks needed based on range size and config
- `scale_up_leaf_node()` - Splits single range of numbers into N per-task ranges

**NumbersConfig** – Custom config extension for controlling distributed parallelism (`numbers_per_task: usize`)

## Usage

This example imports the `InMemoryWorkerResolver` and the `InMemoryChannelResolver` used during integration testing
of this project, so it needs the `--features integration` flag on.

This example demonstrates how the bigger the range of numbers is queried, the more tasks are used in executing
the query, for example:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 10) ORDER BY number" \
  --show-distributed-plan
```

```
SortPreservingMergeExec: [number@0 ASC NULLS LAST]
  SortExec: expr=[number@0 ASC NULLS LAST], preserve_partitioning=[true]
    AggregateExec: mode=FinalPartitioned, gby=[number@0 as number], aggr=[]
      CoalesceBatchesExec: target_batch_size=8192
        RepartitionExec: partitioning=Hash([number@0], 16), input_partitions=16
          AggregateExec: mode=Partial, gby=[number@0 as number], aggr=[]
            RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
              CooperativeExec
                NumbersExec: t0:[0-10)
```

This will print a non-distributed plan, as the range of numbers we are querying (`numbers(0, 10)`) is small.

The config parameter `numbers.numbers_per_task` is the one that controls how many distributed tasks are used in the
query, and it's default value is `10`, so querying 10 numbers will not distribute the plan.

However, if we try to query 11 numbers:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 11) ORDER BY number" \
  --show-distributed-plan
```

```
┌───── DistributedExec ── Tasks: t0:[p0] 
│ SortPreservingMergeExec: [number@0 ASC NULLS LAST]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=32, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p0..p15] 
  │ SortExec: expr=[number@0 ASC NULLS LAST], preserve_partitioning=[true]
  │   AggregateExec: mode=FinalPartitioned, gby=[number@0 as number], aggr=[]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=2
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── Tasks: t0:[p0..p31] t1:[p0..p31] 
    │ CoalesceBatchesExec: target_batch_size=8192
    │   RepartitionExec: partitioning=Hash([number@0], 32), input_partitions=16
    │     AggregateExec: mode=Partial, gby=[number@0 as number], aggr=[]
    │       RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
    │         CooperativeExec
    │           NumbersExec: t0:[0-6), t1:[6-11)
    └──────────────────────────────────────────────────
```

The distribution rule kicks in, and the plan gets distributed.

Note that the parallelism in the plan has an upper threshold, so for example, if we query 100 numbers:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" \
  --show-distributed-plan
```

```
┌───── DistributedExec ── Tasks: t0:[p0] 
│ SortPreservingMergeExec: [number@0 ASC NULLS LAST]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=48, input_tasks=3
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p0..p15] t2:[p0..p15] 
  │ SortExec: expr=[number@0 ASC NULLS LAST], preserve_partitioning=[true]
  │   AggregateExec: mode=FinalPartitioned, gby=[number@0 as number], aggr=[]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=4
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── Tasks: t0:[p0..p47] t1:[p0..p47] t2:[p0..p47] t3:[p0..p47] 
    │ CoalesceBatchesExec: target_batch_size=8192
    │   RepartitionExec: partitioning=Hash([number@0], 48), input_partitions=16
    │     AggregateExec: mode=Partial, gby=[number@0 as number], aggr=[]
    │       RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
    │         CooperativeExec
    │           NumbersExec: t0:[0-25), t1:[25-50), t2:[50-75), t3:[75-100)
    └──────────────────────────────────────────────────
```

We do not get 100/10 = 10 distributed tasks, we just get 4. This is because the example is configured by default to
simulate a 4-worker cluster. If we increase the worker count, we get a highly distributed plan out with 10 tasks:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" \
  --workers 10 \
  --show-distributed-plan
```

```
┌───── DistributedExec ── Tasks: t0:[p0] 
│ SortPreservingMergeExec: [number@0 ASC NULLS LAST]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=112, input_tasks=7
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── Tasks: t0:[p0..p15] t1:[p0..p15] t2:[p0..p15] t3:[p0..p15] t4:[p0..p15] t5:[p0..p15] t6:[p0..p15] 
  │ SortExec: expr=[number@0 ASC NULLS LAST], preserve_partitioning=[true]
  │   AggregateExec: mode=FinalPartitioned, gby=[number@0 as number], aggr=[]
  │     [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=10
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── Tasks: t0:[p0..p111] t1:[p0..p111] t2:[p0..p111] t3:[p0..p111] t4:[p0..p111] t5:[p0..p111] t6:[p0..p111] t7:[p0..p111] t8:[p0..p111] t9:[p0..p111] 
    │ CoalesceBatchesExec: target_batch_size=8192
    │   RepartitionExec: partitioning=Hash([number@0], 112), input_partitions=16
    │     AggregateExec: mode=Partial, gby=[number@0 as number], aggr=[]
    │       RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1
    │         CooperativeExec
    │           NumbersExec: t0:[0-10), t1:[10-20), t2:[20-30), t3:[30-40), t4:[40-50), t5:[50-60), t6:[60-70), t7:[70-80), t8:[80-90), t9:[90-100)
    └──────────────────────────────────────────────────
```

