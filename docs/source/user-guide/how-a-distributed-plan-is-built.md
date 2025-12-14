# How a Distributed Plan is Built

This page walks through the steps the distributed DataFusion planner takes to build a distributed query plan.

Everything starts with a simple single-node plan, for example:

```shell
ProjectionExec: expr=[...]
  AggregateExec: mode=FinalPartitioned, gby=[...], aggr=[...]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([...], 12), input_partitions=12
        AggregateExec: mode=Partial, gby=[...], aggr=[...]
          DataSourceExec: files=[data1, data2, data3, data4]
```

To better understand what happens with the plan in the distribution process, we will represent it graphically:

![img.png](../_static/images/img.png)

Note how the underlying `DataSourceExec` contains multiple non-overlapping pieces of data, represented with colors
in the image. Depending on the underlaying `DataSource` implementation in the `DataSourceExec` node, these can
represent multiple things, like different parquet files, or an external API from which we can gather data for different
time ranges.

The first step is to split the leaf node into different tasks:

![img_2.png](../_static/images/img_2.png)

Each task will handle a different non-overlapping piece of data.

The number of tasks that will be used for executing leaf nodes is determined by a `TaskEstimator` implementation.
There is one default implementation for a file-based `DataSourceExec`, but as a `DataSourceExec` in DataFusion can
be anything the user wants to implement, is the user who should also provide a custom `TaskEstimator` in case they have
a custom `DataSourceExec`.

In the case above, a `TaskEstimator` decided to use four tasks for the leaf node.

After that, we can continue reconstruction the plan:

![img_3.png](../_static/images/img_3.png)

Nothing special to take into account for now, the partial aggregation can just be executed in parallel in different
workers without further considerations.

Let's keep constructing the plan:

![img_4.png](../_static/images/img_4.png)

Things change at this point: a `RepartitionExec` implies that we want to repartition data so that each partition handles
a non-overlapping set of the grouping keys of the ongoing aggregation.

If a `RepartitionExec` in a single-node context re-distributes data across threads in the same machine, a
`RepartitionExec`
in a distributed context re-distributes data across different threads across different machines. This means that we
need to perform a shuffle over the network.

As we are about to send data over the network, it's convenient to coalesce smaller batches into larger ones to avoid
the overhead of sending many small messages and send bigger but fewer instead:

![img_5.png](../_static/images/img_5.png)

After this, we are ready to perform the shuffle over the network. For that, a new `ExecutionPlan` implementation is
provided: `NetworkShuffleExec`:

![img_6.png](../_static/images/img_6.png)

A `NetworkShuffleExec`, instead of calling `execute()` on its child node, it will execute it remotely through Arrow
Flight, and each `NetworkShuffleExec` instance will know which partitions from which machines it should gather data
from.

Note how this means that we have just built the first stage, as the first network boundary was introduced. We are now
in the process of building the second stage, and note how it has just two tasks.

If the number of tasks in a leaf stage is driven by the hints given by `TaskEstimator`s, the number of tasks in upper
stages is driven by the number of nodes in between that recude or increase the cardinality of the data.

In this case, the leaf stage is performing a partial aggregation before sending data to the next stage, so we can
assume that fewer compute will need to happen, and therefore, we can reduce the width of the next stage to just two
tasks.

The rest of the plan can be formed as normal:

![img_7.png](../_static/images/img_7.png)

There's just one last step: the head of the plan is currently spread across two different machines, but we want it
in one. In the same way that vanilla DataFusion coalesces all partitions into one in the head node for the user, we also
need to do that, but not only across partitions in a single machine, but across tasks in different machines.

For that, the `NetworkCoalesceExec` network boundary is introduced: it coalesces P partitions across N tasks into
N*P partitions in one task. This does not imply repartitioning, or shuffling, or anything like that. The partitions
are the same but joined into a single task:

![img_8.png](../_static/images/img_8.png)

Note how at this point, what the user sees is just an `ExecutionPlan` that can be executed as any other normal plan,
but it will happen to be distributed under the hood.
