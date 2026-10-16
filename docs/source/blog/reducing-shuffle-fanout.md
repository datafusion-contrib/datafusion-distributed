---
layout: post
title: "Making Distributed Shuffles Scale in DataFusion"
date: 2026-10-14
author: "Richard Baah (@Rich-T-kid)"
categories: [features]
---

# Making Distributed Shuffles Scale in DataFusion

*October 14, 2026 · Richard Baah ([@Rich-T-kid])*

```{contents}
:local:
:depth: 2
```

In Distributed DataFusion (DFD), a shuffle moves rows between stages so that rows with the same key end up on the same task. When one shuffle has to feed many partitions, the producing side pays a cost for every one of them, and at high fanout that cost shows up as memory pressure and latency spikes. This post explains where the cost comes from and how we reduced it by hashing into fewer buckets. On a 60-worker cluster, that **halved aggregate runtime across the 21 TPC-H scale factor 100 queries that completed on both versions**. For background on DFD, see [Concepts](../learn/01-concepts.md).

Two terms before we start. DFD splits a plan into **stages**, and each stage runs as a set of **tasks**. A **shuffle** is the boundary between one stage and the next: the tasks sending data across it are **producing tasks**, and the tasks receiving it are **consuming tasks**.

```{image} ../_static/images/reducing-shuffle-fanout/stages-tasks-shuffle-overview.svg
:alt: Three distributed execution stages containing tasks, with shuffle connections carrying data between stages.
:width: 68%
:align: center
```

## Background

A **partition**, in DataFusion, is an independent slice of a table's data: a stream of record batches that can be processed without coordinating with any other partition. That independence is what makes parallelism possible. DataFusion commonly targets roughly one partition per available CPU core, providing parallelism without excessive scheduling overhead.

The problem is that the partitioning you start with rarely matches the parallelism you want. A scan's partition count comes from how DataFusion groups and splits its input files and ranges, which may have little to do with the number of available cores. `RepartitionExec` exists to close that gap: it takes N input partitions and remaps them into M output partitions. When it is configured to partition by the hash of a key, it also guarantees that rows sharing that key land together. Joins and aggregations depend on that, since they can only combine rows that are already on the same partition.

`NetworkShuffleExec` exists for the same reason, one level up. Once a query spans more than one machine, "together" can no longer mean just a partition. It has to mean a task. A single-node plan already has hash-partitioned `RepartitionExec` operators sitting exactly where the query needs matching keys to meet, right before joins and aggregations. So when DFD distributes a plan, it places a shuffle directly above each of those operators. Rows that needed to share a partition on a single machine now need to share a task as well.

To move data between tasks, DFD sends record batches using Arrow IPC over Arrow Flight. Arrow IPC preserves Arrow's columnar layout on the wire, so a consuming task can reconstruct arrays without row-wise serialization or conversion. This keeps shuffle encoding overhead low rather than turning every shuffle into a serialization bottleneck.

### How a shuffle maps partitions

Take the shuffle between two stages. Stage 0 scans the data with four tasks, P0 through P3, and Stage 1 does the aggregation work with eight tasks, C0 through C7. Every task runs with 16 partitions internally, one per CPU core.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-scaled-plan.svg
:alt: A distributed plan with four producing tasks in stage 0 and eight consuming tasks in stage 1, highlighting how DataFusion RepartitionExec maps to a DFD network shuffle.
:width: 100%

**Figure 1**
```

Each producing task creates one output partition for every partition in the next stage. The next stage has eight tasks with 16 partitions each, which is 128 partitions in total, so every producing task turns its 16 input partitions into 128 output partitions. Each consuming partition then reads its slice of the data directly from the producing tasks.[^network-shuffle-layer]

## Motivation

We call the number of output partitions a producing task has to create its **fanout**. In the example above it is 128, the number of consuming tasks times the number of partitions in each. This relationship matters: *giving the consuming stage more tasks so a query finishes faster makes every producing task fan out wider*.

Queries over metrics data can produce stages with tens or even hundreds of consuming tasks. A direct shuffle multiplies that task count by the partitions inside each task, commonly a power of two such as 16, 32, or 64. The product grows quickly: 32 consuming tasks with 32 partitions each already gives every producer 1,024 output partitions.

The cost comes from how `RepartitionExec` works. Each output partition owns channel state, a memory reservation, a `BatchCoalescer` that buffers rows until it has enough to send a full batch onward, and row-index buffers used to take its rows from each input batch. None of these allocations is large in isolation, but they are duplicated for every output partition. At a fanout of 256, a producing task holds 256 sets of this state at once, so even the small fixed costs add up before accounting for the rows being buffered.

Two problems follow from this. The first is **memory**. Every partition's `BatchCoalescer` holds rows until a batch fills, so memory use grows with the fanout, and at high fanout it ended in out-of-memory failures. The second is **latency**. Each input batch is split across all of the output partitions, so each partition receives only a small piece of it, and the data ends up in many small allocations. For example, with the default batch size of 8192 rows and keys spread evenly, an output partition gets about 32 rows from each incoming batch, so it takes roughly 256 incoming batches before its buffer is full. Rows sit in these per-partition buffers for that long before they move on, which shows up as latency spikes.

At high enough fanout, this stopped being only a performance problem. In repeated 60-worker TPC-H scale factor 100 benchmark runs, the baseline failed to complete several queries that the reduced-fanout version completed: [six queries in one run](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5661842281) and [seven in another](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5675878399). Even the [final comparison](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5780896732) had one query fail only on the baseline. High fanout was therefore a reliability problem as well as a memory and latency problem.

Figure 2 doubles the consuming stage to 16 tasks, increasing each producer's fanout from 128 to 256.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-partial-buffers.svg
:alt: Four producing tasks each maintain 256 output partitions. Sixteen routes from every producer terminate at the sixteen partitions in every consuming task, forming 1,024 routes overall.
:width: 100%
:name: shuffle-fanout-partial-buffers

**Figure 2**
```

## Design

### Approaches we considered

The cost comes from the number of output partitions a producing task has to back with resources, so any fix has to reduce that number or reduce what each output partition costs. We considered two ways to express the same two-step design.

#### Extend the existing shuffle

The first approach keeps the existing `NetworkShuffleExec` boundary but changes its routing. Instead of hashing each row into one of (consuming tasks × partitions per task) buckets, the producer hashes it into one bucket per consuming task. Each consumer then splits the rows it receives across its own partitions with a local `RepartitionExec`. In our example this takes a producing task from 128 output partitions down to eight.

**The trade-off** is that every row is now repartitioned *twice*, once on the producer and once on the consumer. That extra work only pays for itself when the fanout is high, so we put the new behavior behind a minimum fanout.

#### Rewrite eligible shuffles in a dedicated planner pass

An alternative implemented the same two-step routing in a dedicated planner pass. The pass rewrote each eligible hash shuffle as a task-level `NetworkShuffleExec` followed by a local `RepartitionExec`. The network shuffle sent each row to one bucket per consuming task, and the local repartition then hashed those rows again into the task's final partitions.

In our example, each producer created eight network partitions instead of 128. Each consuming task received one stream from each of the four producers, then its local `RepartitionExec` split those streams across 16 local partitions. The producer-side hash included a salt so task assignment and local partition assignment would not become correlated.

This version needed a new serialized plan variant, which meant coordinators and workers had to be upgraded together.

To understand where the extra local repartition paid off, we benchmarked the prototype with thresholds of 128 and 256 on TPC-H.

<div class="blog-numbered-table">

| TPC-H scale factor | Prototype threshold | Before | After | Speedup |
|---:|---:|---:|---:|---:|
| 10 | 128 | 18.1 s | 16.9 s | [1.07x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5619119816) |
| 100 | 128 | 78.6 s | 75.5 s | [1.04x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5622868492) |
| 10 | 256 | 17.2 s | 17.0 s | [1.01x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5619303159) |

</div>

<div class="table-number">Table 1</div>

One detail matters when reading these results. The prototype counted fanout across the entire exchange, including every producing task. The final design instead measures how many output partitions each individual producer creates, so the prototype's threshold values are not directly comparable with the final default below.

These runs used a cluster of 12 workers with 7 CPUs each. The gains were small, between 1% and 7% in total runtime, and none of the runs regressed in total. For scale, the baseline commit itself varied by about 5% between two runs of the scale factor 10 suite, so most of this is *within noise*. A cluster with 84 CPUs in total keeps the fanout modest, which leaves little producer overhead to remove.

### The final approach

We shipped the first approach as a **two-step shuffle**.

**On the producer,** the hash now maps each row to one of the consuming tasks, instead of to one of the consuming partitions. The producer's `RepartitionExec` therefore has one output partition per consuming task. In our example that is eight output partitions per producing task instead of 128, a reduction by a factor of the number of partitions per task. Each output partition now receives an eighth of the data instead of a 128th, so with the same 8192-row batches each buffer gets about 1024 rows per incoming batch and fills after roughly eight batches instead of 128.

**On the consumer,** the planner injects a local `RepartitionExec` above `NetworkShuffleExec`. It splits the rows the task received across the task's partitions, the same kind of repartition a single-node plan already does. Each consuming task fetches exactly one partition from each producing task.

The two hash steps need to be independent. Without a salt, their results can become correlated, especially when the task and partition counts are powers of two, leaving some task-partition combinations underused. We salt the producer-side hash to break that correlation and avoid skew across the local partitions.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-two-step-design.svg
:alt: Four producing tasks each repartition 16 local partitions into eight consumer-task buckets. Eight consuming tasks each receive one bucket from each producer and locally repartition the data into 16 partitions.
:width: 100%
:name: shuffle-fanout-two-step-design

**Figure 3**
```

The second repartition costs something, so the two-step shuffle *isn't always the right choice*. The planner uses it only when the producer-side fanout—consuming task count × partitions per task—meets a [configurable threshold](https://github.com/datafusion-contrib/datafusion-distributed/blob/6dfa394bbd32c32c74989ff13f66abc0a0419806/src/distributed_planner/distributed_config.rs#L61). The default is 128, so our example of eight consuming tasks with 16 partitions each takes the two-step path. The threshold can be tuned for different cluster sizes and workloads.

## Benchmarks

We ran all 22 TPC-H scale factor 100 queries with one warmup iteration and five measured iterations per query. The selected runs used c5n.4xlarge workers with 15 CPUs each and covered 50- and 60-worker clusters. Because the PR evolved during review, each row compares the base and PR revisions captured for that run. The revision links open the full results.

The runtime and speedup columns include only queries that completed on both revisions. The **Completed** column shows the reliability result across the full suite, which matters because several base and intermediate PR revisions did not finish all 22 queries.

<div class="blog-numbered-table">

| PR revision | Workers | Completed (base → PR) | Runtime (base → PR) | Speedup |
|---|---:|---:|---:|---:|
| [c69add](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5661842281) (2 runs) | 60 | 16 → 22 | 66.8–67.0 s → 28.2–30.1 s | 2.22–2.37x |
| [44d3a4](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5672965814) | 50 | 11 → 22 | 40.0 s → 21.9 s | 1.83x |
| [c898db](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5674396432) | 50 | 18 → 22 | 58.9 s → 33.5 s | 1.76x |
| [c898db](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5675878399) | 60 | 15 → 22 | 61.6 s → 28.9 s | 2.13x |
| [9a2cb9](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5686077845) | 60 | 16 → 22 | 69.2 s → 32.9 s | 2.10x |
| [22ea4d](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5703344446) | 60 | 17 → 22 | 72.5 s → 34.6 s | 2.10x |
| [51a698](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5780896732) | 60 | 21 → 22 | 91.0 s → 45.5 s | **2.00x** |

</div>

<div class="table-number">Table 2</div>

The c69add SF100 range combines [two runs](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5661842281) ([repeat](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5662067248)).

The failures were part of the fanout problem, not just benchmark noise. As [Gabriel explained in the PR](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5662537593), the wider producer fanout forced more data to accumulate before each partition reached its target batch size. That increased memory pressure until worker pods ran out of memory. Reducing the producer-side partition count let batches fill and flush sooner. In several runs, the base completed only 11 to 18 queries while the PR completed all 22.

In the last run above, the PR completed all 22 queries while the base completed 21. **Every comparable query got faster**, from 1.08x on q6 to 2.62x on q4, and their aggregate runtime fell from 91.0 to 45.5 seconds. Query 12 failed on the base commit and completed with the change.

Together, these runs show both sides of the result: reducing producer fanout lowered runtime at scale and reduced memory pressure enough for the highest-fanout queries to complete reliably.

## Conclusion

The cost of a distributed shuffle is not just the number of bytes sent over the network. Every producing task also pays for each output partition it manages. By routing rows to consuming tasks first and splitting them into local partitions afterward, DFD reduces that producer-side fanout from (consuming tasks × partitions per task) to just the number of consuming tasks. A salt keeps the two hash steps from becoming correlated, while the fanout threshold limits the extra local repartition to the cases where it is likely to pay off.

The planner leaves low-fanout shuffles on the direct path. Once the fanout crosses the threshold and the two-step shuffle takes effect, the benefit grows with the number of output partitions a producer would otherwise manage. On the 60-worker TPC-H scale factor 100 run, aggregate runtime across the 21 queries that completed on both revisions fell from 91.0 to 45.5 seconds, and Query 12 went from failing on the base commit to completing with the change.

**What's next.** The default threshold of 128 is a starting point and still needs tuning across more cluster sizes and workloads. Each `RepartitionExec` currently copies rows once while partitioning them and again while coalescing output batches. Reducing those two copies to one is the next target, tracked in [Improve RepartitionExec for better query performance](https://github.com/apache/datafusion/issues/7001#issuecomment-5801503499). DFD also does not yet support range repartitioning, so extending distributed planning beyond hash shuffles remains a separate gap.

Thanks to Gene Bordegaray ([@gene-bordegaray]), Gabriel ([@gabotechs]), and Richard Baah ([@Rich-T-kid]) for the design, implementation, and reviews behind this change.

[@Rich-T-kid]: https://github.com/Rich-T-kid
[@gene-bordegaray]: https://github.com/gene-bordegaray
[@gabotechs]: https://github.com/gabotechs

[^network-shuffle-layer]: Figures 1 and 3 draw `NetworkShuffleExec` as a separate layer to make the network boundary visible. In the physical plan, it is attached to each consuming task.
