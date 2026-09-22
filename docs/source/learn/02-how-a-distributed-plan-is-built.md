# How a Distributed Plan is Built

This page walks through how the distributed DataFusion planner transforms a
query into a distributed execution plan.

The transformation runs as a DataFusion `QueryPlanner` (registered by
`with_distributed_planner()`) **after**
normal physical planning. It takes the single-node physical plan, finds the
points where data needs to be repartitioned in vanilla DataFusion, and
inserts a network boundary node there, splitting the plan into *stages*
that run on different *tasks* (machines).

## The same plan, before and after

Let's assume we have a table with weather data, something like this:

| RainToday (Utf8View) | MinTemp (Float64) | MaxTemp (Float64) |
|----------------------|-------------------|-------------------|
| No                   | 8.0               | 24.3              |
| Yes                  | 14.0              | 26.9              |
| Yes                  | 13.7              | 23.4              |
| No                   | 8.8               | 19.5              |
| ...                  |                   |                   |

And we issue the following query:

```sql
SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
```

[DataFusion fiddle link](https://datafusion-fiddle.vercel.app?q=eyJzdGF0ZW1lbnQiOiJTRUxFQ1QgY291bnQoKiksIFwiUmFpblRvZGF5XCIgRlJPTSB3ZWF0aGVyIEdST1VQIEJZIFwiUmFpblRvZGF5XCIgT1JERVIgQlkgY291bnQoKikifQ==)

The resulting physical plan will look like this:

```sql
SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
  SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
    ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
      AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        RepartitionExec: partitioning=Hash([RainToday@0], 2), input_partitions=2
          AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            DataSourceExec: file_groups={2 groups: [[/api/parquet/weather/part-0.parquet, /api/parquet/weather/part-1.parquet], [/api/parquet/weather/part-2.parquet, /api/parquet/weather/part-3.parquet]]}, projection=[RainToday], file_type=parquet
```

If we now issue the same query with a low value for
`file_scan_config_bytes_per_partition` in order to force distribution:

```sql
SET distributed.file_scan_config_bytes_per_partition = 50000;
SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)
```

[DataFusion fiddle link](https://datafusion-fiddle.vercel.app?q=eyJzdGF0ZW1lbnQiOiJTRVQgZGlzdHJpYnV0ZWQuZmlsZV9zY2FuX2NvbmZpZ19ieXRlc19wZXJfcGFydGl0aW9uID0gNTAwMDA7XG5TRUxFQ1QgY291bnQoKiksIFwiUmFpblRvZGF5XCIgRlJPTSB3ZWF0aGVyIEdST1VQIEJZIFwiUmFpblRvZGF5XCIgT1JERVIgQlkgY291bnQoKikifQ==)

We get the equivalent distributed plan:

```sql
┌───── DistributedExec
│ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
│   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── tasks=2, partitions=2
  │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
  │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
  │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
  │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── tasks=2, partitions=4
    │ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
    │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
    │     DistributedLeafExec:
    │       t0: DataSourceExec: file_groups={4 groups: [[/api/parquet/weather/part-0.parquet:0..21645], [/api/parquet/weather/part-0.parquet:43290..43674, /api/parquet/weather/part-1.parquet:0..21261], [/api/parquet/weather/part-1.parquet:42906..43392, /api/parquet/weather/part-2.parquet:0..21159], [/api/parquet/weather/part-2.parquet:42804..43052, /api/parquet/weather/part-3.parquet:0..21397]]}, projection=[RainToday], file_type=parquet
    │       t1: DataSourceExec: file_groups={4 groups: [[/api/parquet/weather/part-0.parquet:21645..43290], [/api/parquet/weather/part-1.parquet:21261..42906], [/api/parquet/weather/part-2.parquet:21159..42804], [/api/parquet/weather/part-3.parquet:21397..43040]]}, projection=[RainToday], file_type=parquet
    └──────────────────────────────────────────────────
```

The key differences are:

- inserted a **`NetworkShuffleExec`** above the hash `RepartitionExec` (the
  shuffle now fans data across machines),
- inserted a **`NetworkCoalesceExec`** at the top to gather all tasks into the
  single head task,
- wrapped the leaf in a **`DistributedLeafExec`** so each task scans its own
  slice of the files, and
- grew the output partitions of `RepartitionExec` from 2 to 4, to account for
  the two machines above with two partitions each.

### Reading the output

- `┌───── Stage 1 ── tasks=2, partitions=4` — a stage running on **2 tasks**,
  each on a different worker, together spanning **8 partitions**. Tasks are
  machines; partitions are the threads within a task.
- `[Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2` — a
  **network boundary**: this node streams the output of Stage 1 over the network
  using a Flight stream. `input_tasks` is how many tasks produced the data;
  `output_partitions` is how many partitions it exposes to its parent.
- `DistributedExec` — the root and the only node the client executes. It hosts
  the **head stage**, which always runs on a single task (the coordinator).
- `DistributedLeafExec` — a transparent wrapper that carries the different
  `DataSourceExec` variants that should be executed in the different workers;
  `DistributedExec` swaps in the right per-task variant before sending the stage
  to a worker.

## Step by step

The rest of this page walks the same transformation visually, on a four-file
aggregation. To better understand what happens with the plan in the distribution
process, we'll use some schematic drawings:

```
              ▲            
              │            
              │            
┌──────────┬──┴─┬─────────┐
│  ┌───────▶ P0 ◀──────┐  │
│  │       └────┘      │  │
│  │SortPreservingMerge│  │
└──┼───────────────────┼──┘
   │                   │   
   │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││
│└─▲──┘             └──▲─┘│
│  │       Sort        │  │
└──┼───────────────────┼──┘
   │                   │   
   │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││
│└─▲──┘             └──▲─┘│
│  │    Projection     │  │
└──┼───────────────────┼──┘
   │                   │   
   │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││
│└▲─▲─┘             └─▲─▲┘│
│ │  Aggregate(final)   │ │
└───┼─────────────────┼───┘
  │  ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐│  
    ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘    
┌┬┴┴──┬─────────────┬──┴┴┬┐
││ P0 │             │ P1 ││
│└─▲──┘             └──▲─┘│
│  │  RepartitionExec  │  │
└──┼───────────────────┼──┘
   │                   │   
   │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││
│└─▲──┘             └──▲─┘│
│  │Aggregate(partial) │  │
└──┼───────────────────┼──┘
   │                   │   
   │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││
│└────┘             └────┘│
│     DataSourceExec      │
└─────────────────────────┘
```

Note how the data is distributed locally across 2 partitions, each one with
its own data stream.

The distributed planner starts walking the plan in a bottom-up fashion, starting
with the leaf nodes.

The number of tasks that will be used for executing leaf nodes is determined by
`DesiredTaskCountHandler` implementations. Default handlers exist for file-based
`DataSourceExec` nodes. However, since `DataSourceExec` can be customized to
represent any data source, users with custom implementations should also provide
corresponding desired task-count and leaf-scale handlers.

With this example setup, the leaf node will be scaled up to 2 tasks, each one
running its own set of partitions.

```
   ▲                   ▲            ▲                   ▲   
   │                   │            │                   │   
   │                   │            │                   │   
   │                   │            │                   │   
   │                   │            │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└────┘             └────┘│      │└────┘             └────┘│
│     DataSourceExec      │      │     DataSourceExec      │
└─────────────────────────┘      └─────────────────────────┘
```

Once the leaf nodes are figured out, the planner walks up the next node,
adhering to the established parallelism imposed by the leaf node.

```
   ▲                   ▲            ▲                   ▲   
   │                   │            │                   │   
   │                   │            │                   │   
   │                   │            │                   │   
   │                   │            │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│ <- added
│  │Aggregate(partial) │  │      │  │Aggregate(partial) │  │
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘
   │                   │            │                   │   
   │                   │            │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└────┘             └────┘│      │└────┘             └────┘│
│     DataSourceExec      │      │     DataSourceExec      │
└─────────────────────────┘      └─────────────────────────┘
```

Nothing special to consider for now—the partial aggregation can simply be
executed in parallel across different workers without further considerations.

Let's keep constructing the plan:

```
  ▲▲▲▲               ▲▲▲▲          ▲▲▲▲               ▲▲▲▲  
  ││││               ││││          ││││               ││││  
                                                            
  │││├ ─ ─ ─ ─ ─ ─ ─ ┤││├ ─ ─ ─ ─ ─│││├ ─ ─ ─ ─ ─ ─ ─ ┤│││  
     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─      
┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐      ┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│ <- added
│  │  RepartitionExec  │  │      │  │  RepartitionExec  │  │
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘
   │                   │            │                   │   
   │                   │            │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│
│  │Aggregate(partial) │  │      │  │Aggregate(partial) │  │
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘
   │                   │            │                   │   
   │                   │            │                   │   
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││
│└────┘             └────┘│      │└────┘             └────┘│
│     DataSourceExec      │      │     DataSourceExec      │
└─────────────────────────┘      └─────────────────────────┘
```

At this point, the plan encounters a `RepartitionExec` node, which requires
repartitioning data so each partition handles a non-overlapping subset of
grouping keys for the aggregation.

`RepartitionExec` is typically used for redistributing work across partitions
in the same machine, so typically the number of output partitions adheres to
the `datafusion.execution.target_partitions` setting, but in this case it needs
to fan out to `datafusion.execution.target_partitions * task_count` partitions.

After this, we are ready to perform the shuffle over the network. For that, a
new `ExecutionPlan` implementation is provided: `NetworkShuffleExec`:

```
   ▲                   ▲            ▲                   ▲               
   │                   │            │                   │               
   │                   │            │                   │               
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└▲▲▲▲┘             └▲▲▲▲┘│      │└▲▲▲▲┘             └▲▲▲▲┘│ <- added
│ ││││NetworkShuffle ││││ │      │ ││││NetworkShuffle ││││ │            
└─────────────────────────┘      └─────────────────────────┘            
  │││├ ─ ─ ─ ─ ─ ─ ─ ┤││├ ─ ─ ─ ─ ─│││├ ─ ─ ─ ─ ─ ─ ─ ┤│││              
     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─        ■         
┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐      ┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│            
│  │  RepartitionExec  │  │      │  │  RepartitionExec  │  │  │         
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │     │         
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐  │         
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││    Stage 1 
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│  │         
│  │Aggregate(partial) │  │      │  │Aggregate(partial) │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘  │         
   │                   │            │                   │               
   │                   │            │                   │     │         
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└────┘             └────┘│      │└────┘             └────┘│            
│     DataSourceExec      │      │     DataSourceExec      │  │         
└─────────────────────────┘      └─────────────────────────┘  ■         
```

A `NetworkShuffleExec`, instead of calling `execute()` on its child node, will
execute it remotely through the network, and each `NetworkShuffleExec` instance
will know from which partitions and machines it should gather data.

Note how this means that we have just built the first stage, as the first
network boundary was introduced. We are now in the process of building the
second stage. The process above is repeated until the next network boundary

```
   ▲                   ▲            ▲                   ▲               
   │                   │            │                   │               
   │                   │            │                   │               
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│ <- added   
│  │       Sort        │  │      │  │       Sort        │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│ <- added   
│  │    Projection     │  │      │  │    Projection     │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│ <- added   
│  │    Aggr(final)    │  │      │  │    Aggr(final)    │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└▲▲▲▲┘             └▲▲▲▲┘│      │└▲▲▲▲┘             └▲▲▲▲┘│            
│ ││││NetworkShuffle ││││ │      │ ││││NetworkShuffle ││││ │            
└─────────────────────────┘      └─────────────────────────┘            
  │││├ ─ ─ ─ ─ ─ ─ ─ ┤││├ ─ ─ ─ ─ ─│││├ ─ ─ ─ ─ ─ ─ ─ ┤│││              
     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─        ■         
┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐      ┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│            
│  │  RepartitionExec  │  │      │  │  RepartitionExec  │  │  │         
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │     │         
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐  │         
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││    Stage 1 
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│  │         
│  │Aggregate(partial) │  │      │  │Aggregate(partial) │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘  │         
   │                   │            │                   │               
   │                   │            │                   │     │         
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└────┘             └────┘│      │└────┘             └────┘│            
│     DataSourceExec      │      │     DataSourceExec      │  │         
└─────────────────────────┘      └─────────────────────────┘  ■         
```

One final step remains: the plan's head is currently distributed across two
machines, but the final result must be consolidated on a single one. In the
same way that vanilla DataFusion coalesces all partitions into one in the head
node for the user, we also need to do that, but not only across partitions on a
single machine, but across tasks on different machines.

For that, the `NetworkCoalesceExec` network boundary is introduced: it coalesces
P partitions across N tasks into N*P partitions in one task. This does not imply
repartitioning, or shuffling, or anything like that. The partitions are the same
but joined into a single task:

Note how at this point, what the user sees is just an `ExecutionPlan` that can
be executed as any other normal plan, but it will happen to be distributed under
the hood:

```
                              ▲                                         
                              │                                         
                              │                                         
                 ┌─────────┬──┴─┬─────────┐                             
                 │         │ P0 │         │                             
                 │         └────┘         │   <- added                       
                 │  SortPreservingMerge   │                             
                 └──▲─────▲──────▲─────▲──┘                             
                    │     │      │     │                                
                    │     │      │     │                                
                 ┌┬─┴──┬┬─┴──┬┬──┴─┬┬──┴─┬┐                             
                 ││ P0 ││ P1 ││ P2 ││ P3 ││                             
                 │└────┘└────┘└────┘└────┘│   <- added                  
                 │    NetworkCoalesce     │                             
                 └──▲─────▲─────▲──────▲──┘                             
                    │     │     │      │                                
   ┌────────────────┘     │     │      └────────────────┐               
   │                   ┌──┘     └───┐                   │               
   │                   │            │                   │               
   │                   │            │                   │               
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐  ■         
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│  │         
│  │       Sort        │  │      │  │       Sort        │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘  │         
   │                   │            │                   │               
   │                   │            │                   │     │         
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│            
│  │    Projection     │  │      │  │    Projection     │  │  │         
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │     │ Stage 2 
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐  │         
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││            
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│  │         
│  │    Aggr(final)    │  │      │  │    Aggr(final)    │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘  │         
   │                   │            │                   │               
   │                   │            │                   │     │         
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└▲▲▲▲┘             └▲▲▲▲┘│      │└▲▲▲▲┘             └▲▲▲▲┘│            
│ ││││NetworkShuffle ││││ │      │ ││││NetworkShuffle ││││ │  │         
└─────────────────────────┘      └─────────────────────────┘  ■         
  │││├ ─ ─ ─ ─ ─ ─ ─ ┤││├ ─ ─ ─ ─ ─│││├ ─ ─ ─ ─ ─ ─ ─ ┤│││              
     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─        ■         
┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐      ┌┬┴┴┴┴┬─────────────┬┴┴┴┴┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│            
│  │  RepartitionExec  │  │      │  │  RepartitionExec  │  │  │         
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘            
   │                   │            │                   │     │         
   │                   │            │                   │               
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐  │         
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││    Stage 1 
│└─▲──┘             └──▲─┘│      │└─▲──┘             └──▲─┘│  │         
│  │Aggregate(partial) │  │      │  │Aggregate(partial) │  │            
└──┼───────────────────┼──┘      └──┼───────────────────┼──┘  │         
   │                   │            │                   │               
   │                   │            │                   │     │         
┌┬─┴──┬─────────────┬──┴─┬┐      ┌┬─┴──┬─────────────┬──┴─┬┐            
││ P0 │             │ P1 ││      ││ P2 │             │ P3 ││  │         
│└────┘             └────┘│      │└────┘             └────┘│            
│     DataSourceExec      │      │     DataSourceExec      │  │         
└─────────────────────────┘      └─────────────────────────┘  ■         
```
