# How a Distributed Plan Is Built

DataFusion Distributed installs a `QueryPlanner` with
`with_distributed_planner()`. After DataFusion creates a normal physical plan,
the distributed planner prepares that plan, splits it into stages at network
boundaries, and wraps the result in `DistributedExec`.

A **stage** is a connected fragment of the physical plan. A **task** is one
instance of a stage. Each task is assigned to a worker URL before execution;
multiple tasks may be assigned to the same worker. A **partition** is a
DataFusion execution partition inside a task's plan. Partitions are units of
parallel execution, not dedicated threads.

## The same query before and after distribution

Consider this query over the three-file `weather` table:

```sql
SELECT count(*), "RainToday"
FROM weather
GROUP BY "RainToday"
ORDER BY count(*)
```

DataFusion first creates the single-node physical plan:

```text
SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
  SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
    ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
      AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
          AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            DataSourceExec: file_groups={3 groups: [...]}, projection=[RainToday], file_type=parquet
```

With the configuration used by the planner's aggregation snapshot test, the
abridged distributed plan is:

```text
DistributedExec
  SortPreservingMergeExec
    [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2

Stage 2: tasks=2, partitions=4
  SortExec
    ProjectionExec
      AggregateExec: mode=FinalPartitioned
        [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3

Stage 1: tasks=3, partitions=8
  RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=3
    AggregateExec: mode=Partial
      DistributedLeafExec
        t0: DataSourceExec: file_groups={3 groups: [...]}
        t1: DataSourceExec: file_groups={3 groups: [...]}
        t2: DataSourceExec: file_groups={3 groups: [...]}
```

That snapshot uses four DataFusion target partitions, three available worker
URLs, a file-scan byte target small enough to request three leaf tasks, and a
`distributed.cardinality_task_count_factor` of `1.5`. The factor defaults to
`1.5` in tests and integration builds, but to `1.0` in a normal library build.
Task and partition counts therefore depend on the session configuration,
statistics, handlers, and available workers; they are not properties of the
SQL query alone.

### Reading the distributed plan

- `Stage 1: tasks=3, partitions=8` describes three instances of the stage. The
  stage plan produces eight DataFusion partitions. Worker assignment happens
  later, so this line does not mean three machines.
- `NetworkShuffleExec: output_partitions=4, input_tasks=3` is a network
  boundary that reads from the three producer tasks and exposes four output
  partitions to the consumer plan. It preserves the hash partitioning required
  by the final aggregate.
- `NetworkCoalesceExec` gathers partitions from its input tasks without
  repartitioning their rows. Here, `SortPreservingMergeExec` consumes the
  gathered partitions in the coordinator's head stage.
- `DistributedLeafExec` is one wrapper containing a plan variant for each task,
  shown as `t0` through `t2`. Before a task is serialized, the coordinator
  replaces the wrapper with only that task's `DataSourceExec` variant.
- `DistributedExec` is the root executed by the client. It runs the head plan
  locally and coordinates the remote producer stages behind the network
  boundaries.

## Planning pipeline

### 1. Prepare a valid single-node plan

The planner first applies distribution-oriented rewrites that still leave a
valid single-node plan:

1. Normalize `CollectLeft` joins.
2. Add a `CoalescePartitionsExec` above a multi-partition root so the head can
   later receive a coalescing network boundary.
3. Insert `BroadcastExec` markers on eligible join build sides.
4. Replace unions with `ChildrenIsolatorUnionExec` placeholders so their
   children can be assigned across task slots.

These steps identify how the plan may be distributed without yet assigning
worker URLs.

### 2. Choose task counts and specialize leaves

In static planning, `DesiredTaskCountHandler` implementations provide task
count hints for leaves. The built-in file-scan handler estimates a desired
count from file sizes and
`distributed.file_scan_config_bytes_per_partition`. Custom data sources can
register their own desired-task-count and scale-up handlers.

The planner combines child hints while walking upward. Operators' cardinality
effects and `distributed.cardinality_task_count_factor` can increase or reduce
the desired count for an upper stage. Counts are capped by
`distributed.max_tasks_per_stage`, or by the number of resolved worker URLs
when that option is zero.

Once a stage count is known, the count is propagated back through the stage.
Leaf scale-up handlers create the per-task variants held by
`DistributedLeafExec`. This is why the logical distributed plan contains one
leaf wrapper rather than a separate independent leaf subtree for every task.

### 3. Insert stage boundaries

The boundary pass keeps the prepared plan's topology and inserts one of three
network nodes above a producer stage:

- `NetworkShuffleExec` above a hash `RepartitionExec` distributes hash ranges
  among consumer tasks.
- `NetworkBroadcastExec` above an eligible `BroadcastExec` makes the build-side
  data available to each consumer task.
- `NetworkCoalesceExec` below `CoalescePartitionsExec` or
  `SortPreservingMergeExec` gathers producer partitions into the consumer.

The planner does not add a batch-coalescing execution node before a shuffle.
Worker stages inherit `datafusion.execution.batch_size` by default. Setting
`distributed.shuffle_batch_size` to a nonzero value overrides that worker
session value; `RepartitionExec` uses it through its internal
`LimitedBatchCoalescer` when producing shuffle batches.

### 4. Finalize the static plan

After boundary insertion, the planner:

1. prepares the boundaries, assigns stage identifiers, and removes boundaries
   that are not needed;
2. optionally inserts partial reductions below hash shuffles when
   `distributed.partial_reduce` is enabled; and
3. pushes a root fetch limit into a network coalesce when doing so is safe.

If no network boundary remains, the original single-node plan is returned.
Otherwise, the finalized plan is wrapped in `DistributedExec`.

## Static and dynamic task sizing

With the default `distributed.dynamic_task_count=false`, task counts and all
network boundaries are present in the physical plan before execution. The
coordinator then routes every stage task and sends a task-specialized subplan
to the selected worker URL.

With dynamic task sizing enabled, the query planner performs the initial
single-node preparation but defers boundary injection. During execution it
builds and runs producer stages from the leaves upward, inserts a `SamplerExec`
at each producer head, collects runtime row and byte statistics, and uses those
statistics with `distributed.dynamic_bytes_per_partition` to choose the next
stage's task count. The complete distributed shape is therefore known only
after execution has progressed through all stages.

## Routing and transport

For each remote stage, the coordinator requests one worker URL per task from
the registered `RouteTasksHandler` chain. For a single task, the built-in
handlers first try to colocate it with the coordinator or a child stage. The
fallback uses the URLs returned by `WorkerResolver` in round-robin order from a
randomized starting offset. A URL can appear more than once, so tasks are
execution slots, not workers.

Before sending a task, the coordinator specializes
`ChildrenIsolatorUnionExec` and `DistributedLeafExec` for that task index,
serializes the resulting plan, and sends it over `WorkerChannel`. The transport
is abstracted by that trait. With the built-in gRPC implementation:

1. `WorkerService.CoordinatorChannel` carries the serialized plan, work-unit
   messages, load reports, and final metrics.
2. A network boundary calls `WorkerService.ExecuteTask` with a task key,
   partition range, and producer-head description.
3. `ExecuteTask` returns a server-side gRPC stream of Arrow Flight
   `FlightData`. Application metadata identifies each message's original
   partition so the receiving boundary can reconstruct the per-partition
   streams.

The wire format uses Arrow Flight's `FlightData`, but execution is performed by
the project's `WorkerService` protocol rather than by an Arrow Flight service.
