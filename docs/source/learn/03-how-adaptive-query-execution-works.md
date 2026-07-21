# How Adaptive Query Execution Works

The normal distributed planner decides all stage boundaries and task counts
before execution begins. Adaptive Query Execution (AQE) delays those decisions:
it starts at the leaves, executes enough of each producer stage to measure its
output, and uses those measurements to size the next stage.

AQE sizes each stage using a cost model that combines the amount of data flowing
through each node with the estimated compute cost of that node. A stage that
moves a large amount of data through fully streaming operators with little
per-row computation may need only a few tasks. In contrast, a stage containing
compute-intensive operators may be assigned more tasks even when relatively
little data flows through it.

AQE estimates the amount of data flowing through a stage in two ways:

1. For leaf stages, it uses the statistics returned by
   `ExecutionPlan::partition_statistics`, relying on DataFusion's upstream
   statistics machinery.
2. For intermediate stages, it infers statistics by sampling data at runtime as
   the stages below them execute.

## Building the plan from the leaves up

Unlike the static distributed planner, AQE does not build every stage before
execution starts. It begins with the original DataFusion physical plan, where no
network boundaries or distributed task counts have been assigned yet. A
simplified plan might look like this:

```text
SortPreservingMergeExec
  SortExec
    AggregateExec: Final
      RepartitionExec
        AggregateExec: Partial
          DataSourceExec
```

The coordinator walks this plan from the leaves upward. When it reaches the
first point where a network boundary is required, it has enough information to
close the plan below that boundary as the first stage. Because this stage
contains a data-source leaf, its cost is calculated from
`ExecutionPlan::partition_statistics`. The cost model and the registered
`TaskEstimator` are then used to choose its task count:

```text
SortPreservingMergeExec
  SortExec
    AggregateExec: Final
      ▲
      │ future network boundary
      │
┌───── Stage 1 ── tasks=4 ──────────┐
│ RepartitionExec                   │
│   AggregateExec: Partial          │
│     DataSourceExec                │
└───────────────────────────────────┘
          ▲
          └── sized from leaf statistics
```

The coordinator inserts a `SamplerExec` directly below the producer-head
`RepartitionExec` and sends Stage 1 to its workers. The workers begin executing
it before the stage above has been sized. The sampler holds on to the batches
that reach it and reports information about them back to the coordinator:

```text
             coordinator
                  ▲
                  │ sampled rows, bytes,
                  │ distinct values, and nulls
                  │
┌───── Stage 1 ── tasks=4 ──────────┐
│ RepartitionExec                   │
│   SamplerExec                     │◀── injected by AQE
│     AggregateExec: Partial        │
│       DataSourceExec              │
└───────────────────────────────────┘
```

The sampled batches are not discarded. They remain buffered until the consumer
stage starts and are then returned as part of the normal execution stream.

The coordinator turns the sample into DataFusion `Statistics` and attaches them
to the network boundary that represents Stage 1. From the point of view of the
operators above it, that boundary now behaves like a leaf with runtime-derived
statistics. AQE can therefore calculate the cost of Stage 2 and choose its task
count using the runtime behavior observed from Stage 1:

```text
┌───── Stage 2 ── tasks=2 ──────────┐
│ SortExec                          │
│   AggregateExec: Final            │
│     [Stage 1] NetworkShuffleExec  │◀── runtime statistics
└───────────────────────────────────┘
                  ▲
                  │ reads from
                  │
┌───── Stage 1 ── tasks=4 ──────────┐
│ RepartitionExec                   │
│   SamplerExec                     │
│     AggregateExec: Partial        │
│       DataSourceExec              │
└───────────────────────────────────┘
```

Stage 2 is then sent to its workers. In this example, Stage 2 feeds a
`NetworkCoalesceExec`, whose consumer is the single-task head stage. The head
task count is already fixed, so Stage 2 does not need to be sampled. It begins
normal execution when the head requests its output. In plans with more
intermediate stages, the sampling process repeats until the coordinator reaches
such a final coalescing boundary.

The resulting distributed plan might look like this, with each task count
decided using the best statistics available at that point:

```text
┌───── Head stage ── tasks=1 ───────┐
│ SortPreservingMergeExec           │
│   [Stage 2] NetworkCoalesceExec   │
└───────────────────────────────────┘
  ┌───── Stage 2 ── tasks=2 ──────────┐
  │ SortExec                          │
  │   AggregateExec: Final            │
  │     [Stage 1] NetworkShuffleExec  │
  └───────────────────────────────────┘
    ┌───── Stage 1 ── tasks=4 ──────────┐
    │ RepartitionExec                   │
    │   SamplerExec                     │
    │     AggregateExec: Partial        │
    │       DataSourceExec              │
    └───────────────────────────────────┘
```

The task counts in this example are illustrative. Depending on the observed
data and the operators in each stage, AQE may make an intermediate stage wider
or narrower than the stage below it.

## Implications of progressive planning

Interleaving planning, sampling, and execution has several consequences:

1. Plan fragments are sent to workers progressively. A producer stage is sent
   and started first, but fragments that consume its output are not sent until
   sampling has produced the runtime statistics needed to size them.
2. Producer tasks may start before they have consumers. The sampler buffers the
   batches produced during this period and releases them when the downstream
   fragment starts consuming the stage.
3. The final distributed plan does not exist at the beginning of the query.
   Network boundaries, task counts, and worker assignments for later stages are
   decided as execution moves up the plan.
4. Independent branches can be sampled concurrently, but a stage that consumes
   several branches must wait until the required runtime statistics are
   available from all of them.

## Deep dive into sampling

Sampling needs to answer two separate questions:

1. How much output has the stage produced so far?
2. How far has the stage progressed through the input that drives that output?

`SamplerExec` answers the first question from the batches that reach it. It
buffers those batches and measures their row count, total and per-column byte
sizes, distinct-value percentages, and null percentages.

The leaves on the stage's **driver path** answer the second question. Their
`output_rows` metrics report how many input rows have been pulled so far, while
`ExecutionPlan::partition_statistics` provides an estimate of how many rows
they will produce in total:

```text
┌───── Stage 1 ─────────────────────────────────────────────┐
│ RepartitionExec                                           │
│   SamplerExec                 ◀── output produced so far  │
│     AggregateExec: Partial                                │
│       DataSourceExec          ◀── driver rows consumed    │
└───────────────────────────────────────────────────────────┘
                                         ▲
                                         └── estimated total rows
                                             from partition_statistics
```

These measurements deliberately count rows at different points in the plan.
An aggregate or filter may consume many input rows while yielding only a small
number of output rows. AQE therefore does not compare the sampler's output row
count directly with the leaf estimate. Instead, it uses the leaf measurements
to estimate progress, then applies that progress to the output observed by the
sampler.

### The driver path

The driver path contains the operators whose continued input makes the stage
produce more output. For most operators, it follows every child. For
pipeline-breaking joins such as `HashJoinExec`, `NestedLoopJoinExec`, and
`CrossJoinExec`, it follows only the right-hand probe side:

```text
SamplerExec
  HashJoinExec
    left:  build side    ── ignored for progress
    right: probe side    ◀─ driver path
      DataSourceExec
```

The build side is excluded because it must be materialized before the join can
yield its first output batch. Consuming build-side rows is setup work, not an
indication of how far the join has progressed through the stream that drives
its output. When a driver path reaches several leaves, their estimated and
consumed row counts are added together.

### Reporting `LoadInfo`

Each partition inside a `SamplerExec` produces one `LoadInfo` report containing
the measurements needed by the coordinator:

```text
LoadInfo
├── partition
├── rows_ready
├── per_column_bytes_ready
├── per_column_ndv_percentage
├── per_column_null_percentage
├── rows_pulled_from_leaf
└── reached_eos
```

Every task sends its partition reports through its worker-to-coordinator
channel. The coordinator merges reports from all tasks as they arrive:

```text
Worker A / task 0
  sampler partition 0 ── LoadInfo ──┐
  sampler partition 1 ── LoadInfo ──┤
                                    ├──▶ coordinator
Worker B / task 1                   │    merge reports
  sampler partition 0 ── LoadInfo ──┤    stop at threshold
  sampler partition 1 ── LoadInfo ──┘
```

The total number of sampler partitions is the number of partitions per task
multiplied by the number of tasks in the stage. The coordinator collects
reports until enough partitions have produced non-empty output, or until every
partition has reported:

```text
total sampler partitions = partitions per task × stage tasks

enough reports = sampling threshold reached
              or all sampler partitions have reported
```

Because sampling may stop before every partition reports, the observed rows,
bytes, and consumed driver rows are first scaled from the number of reporting
partitions to the total partition count.

The coordinator then estimates how much of the driver input has been consumed
and uses that fraction to extrapolate the sampler output:

```text
estimated completion = consumed driver rows / estimated driver rows
estimated stage output = normalized sampled output / estimated completion
```

The resulting row counts, byte sizes, distinct counts, and null counts become
the runtime `Statistics` exposed by the network boundary to the stage above. If
every sampled partition has reached end-of-stream, the sample is treated as
100% complete. If the driver leaves do not provide a row-count estimate, AQE
falls back to a default completion estimate.
