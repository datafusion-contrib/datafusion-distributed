# How a Distributed Plan is Built

This page walks through how the distributed DataFusion planner transforms a query into a distributed execution plan.

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
in the image. Depending on the underlying `DataSource` implementation in the `DataSourceExec` node, these can
represent different things, such as different parquet files, or an external API from which we can gather data for different
time ranges.

The first step is to split the leaf node into different tasks:

![img_2.png](../_static/images/img_2.png)

Each task will handle a different non-overlapping piece of data.

The number of tasks that will be used for executing leaf nodes is determined by a `TaskEstimator` implementation.
A default implementation exists for file-based `DataSourceExec` nodes. However, since `DataSourceExec` can be
customized to represent any data source, users with custom implementations should also provide a corresponding
`TaskEstimator`.

In the case above, a `TaskEstimator` decided to use four tasks for the leaf node. Note that even if we are distributing
the data across different tasks, each task will also distribute its data across partitions using the vanilla DataFusion
partitioning mechanism. A partition is a split of data processed by a single thread on a single machine, whereas a 
task is a split of data processed by an entire machine within a cluster.

After that, we can continue reconstructing the plan:

![img_3.png](../_static/images/img_3.png)

Nothing special to consider for now—the partial aggregation can simply be executed in parallel across different
workers without further considerations.

Let's keep constructing the plan:

![img_4.png](../_static/images/img_4.png)

At this point, the plan encounters a `RepartitionExec` node, which requires repartitioning data so each partition
handles a non-overlapping subset of grouping keys for the aggregation.

While `RepartitionExec` redistributes data across threads on a single machine in vanilla DataFusion, it redistributes
data across threads on different machines in the distributed context—requiring a network shuffle.

As we are about to send data over the network, it's convenient to coalesce smaller batches into larger ones to avoid
the overhead of sending many small messages, and instead send fewer but larger messages:

![img_5.png](../_static/images/img_5.png)

After this, we are ready to perform the shuffle over the network. For that, a new `ExecutionPlan` implementation is
provided: `NetworkShuffleExec`:

![img_6.png](../_static/images/img_6.png)

A `NetworkShuffleExec`, instead of calling `execute()` on its child node, will execute it remotely through Arrow
Flight, and each `NetworkShuffleExec` instance will know from which partitions and machines it should gather data.

Note how this means that we have just built the first stage, as the first network boundary was introduced. We are now
in the process of building the second stage, and note how it has just two tasks.

If the number of tasks in a leaf stage is driven by the hints given by `TaskEstimator`s, the number of tasks in upper
stages is driven by the nodes in between that reduce or increase the cardinality of the data.

In this case, the leaf stage is performing a partial aggregation before sending data to the next stage, so we can
assume that less compute will be needed, and therefore, we can reduce the width of the next stage to just two
tasks.

The rest of the plan can be formed as normal:

![img_7.png](../_static/images/img_7.png)

One final step remains: the plan's head is currently distributed across two machines, but the final result must be
consolidated on a single node. In the same way that vanilla DataFusion coalesces all partitions into one in the head
node for the user, we also need to do that, but not only across partitions on a single machine, but across tasks on
different machines.

For that, the `NetworkCoalesceExec` network boundary is introduced: it coalesces P partitions across N tasks into
N*P partitions in one task. This does not imply repartitioning, or shuffling, or anything like that. The partitions
are the same but joined into a single task:

![img_8.png](../_static/images/img_8.png)

Note how at this point, what the user sees is just an `ExecutionPlan` that can be executed as any other normal plan,
but it will happen to be distributed under the hood.
