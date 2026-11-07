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

In Distributed DataFusion (DFD), shuffles move rows between stages so equal keys reach the same task. High fanout—one producer feeding many partitions—adds per-destination state, causing memory pressure and latency spikes. Hashing into fewer buckets **halved aggregate runtime for the 21 TPC-H scale factor 100 queries that completed on both versions** in a 60-worker cluster. For DFD background, see [Concepts](../learn/01-concepts.md).

DFD splits a plan into **stages**, each running as a set of **tasks**. A **shuffle** separates stages; its senders are **producing tasks** and its receivers are **consuming tasks**.

```{image} ../_static/images/reducing-shuffle-fanout/stages-tasks-shuffle-overview.svg
:alt: Three distributed execution stages containing tasks, with shuffle connections carrying data between stages.
:width: 68%
:align: center
```

## Background

A DataFusion **partition** is an independent stream of record batches that can be processed concurrently. `datafusion.execution.target_partitions` sets the target parallelism, including when scans group and split input files and ranges.

Before a hash aggregation or join, `RepartitionExec` also enforces correctness: it remaps N inputs into P non-overlapping hash partitions, bringing equal grouping or join keys together while processing different keys concurrently.

Across machines, `NetworkShuffleExec` extends that guarantee to tasks: equal keys must reach the same task and partition. DFD therefore places a shuffle above each hash-partitioned `RepartitionExec` preceding a join or aggregation.

DFD sends record batches between tasks using Arrow IPC over Arrow Flight. IPC preserves Arrow's columnar layout, so consumers reconstruct arrays without row-wise serialization or conversion, avoiding a serialization bottleneck.

### How a shuffle maps partitions

Let T be the number of consuming tasks and P the partitions per task. `RepartitionExec` fans out to P partitions on one machine, but each producer in a direct distributed shuffle fans out to T × P. Figure 1 uses T = 4 and P = 4.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-direct-plan.svg
:alt: Matching plans show RepartitionExec fanning out to P partitions on one machine. In Distributed DataFusion, 16 animated red arrows enter NetworkShuffleExec and 16 leave it, with four entering the bottom of each consuming task.
:width: 100%

**Figure 1**
```

Each producer creates one output per destination partition: 16 for the 4 × 4 diagram and 128 for eight tasks with 16 partitions each. Each consuming partition reads its slice directly from the producers.[^network-shuffle-layer]

## Motivation

Producer **fanout** is T × P, so adding consuming tasks to finish a query faster also widens every producer. Metrics queries can have tens or hundreds of tasks, each commonly using 16, 32, or 64 partitions; 32 tasks × 32 partitions already gives each producer 1,024 outputs.

Each output owns channel state, a memory reservation, a `BatchCoalescer`, and row-index buffers used to select rows from input batches. These small costs multiply with fanout: at 256, a producer holds 256 copies plus the buffered rows.

This causes **memory** and **latency** problems. Each `BatchCoalescer` retains rows until its batch fills, and enough concurrent buffers caused out-of-memory failures. Splitting a default 8192-row input batch evenly across 256 outputs gives each only about 32 rows, requiring roughly 256 inputs to fill a buffer. Those small allocations retain rows and delay delivery.

The problem also affected reliability. In repeated 60-worker TPC-H scale factor 100 runs, reduced fanout completed [six queries](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5661842281) that the baseline missed in one run and [seven](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5675878399) in another. In the [final comparison](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5780896732), only the baseline failed a query.

Figure 2 shows one producer at T = 4 and P = 4; fanout 256 repeats this pattern 16 times.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-direct-buffers.svg
:alt: One producing task splits each input batch across 16 labeled output buffers. A labeled dashed line marks the network transfer before 16 animated red routes enter NetworkShuffleExec, then 16 more routes fan out to matching partitions across four consuming tasks.
:width: 100%
:name: shuffle-fanout-partial-buffers

**Figure 2**
```

## Design

### Approaches we considered

Because the cost scales with producer outputs, a fix must reduce their number or per-output state. We considered two forms of the same two-step design.

#### Extend the existing shuffle

The first approach keeps `NetworkShuffleExec` but hashes each row into one bucket per consuming task instead of T × P buckets. A local `RepartitionExec` then splits each task's rows into P partitions. With eight tasks and 16 partitions each, producer fanout falls from 128 to eight.

Every row is now repartitioned *twice*, on the producer and consumer. This only pays off at high fanout, so the behavior needs a minimum threshold.

#### Rewrite eligible shuffles in a dedicated planner pass

An alternative planner pass rewrote eligible hash shuffles as a task-level `NetworkShuffleExec` followed by local `RepartitionExec`. Each producer again created eight network partitions instead of 128; each consumer received one stream per producer and split them into 16 local partitions. A producer-side salt prevented task and partition assignments from correlating. This approach required a new serialized plan variant and lockstep coordinator/worker upgrades.

We benchmarked this prototype on TPC-H with thresholds of 128 and 256.

<div class="blog-numbered-table">

| TPC-H scale factor | Prototype threshold | Before | After | Speedup |
|---:|---:|---:|---:|---:|
| 10 | 128 | 18.1 s | 16.9 s | [1.07x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5619119816) |
| 100 | 128 | 78.6 s | 75.5 s | [1.04x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5622868492) |
| 10 | 256 | 17.2 s | 17.0 s | [1.01x](https://github.com/datafusion-contrib/datafusion-distributed/pull/719#issuecomment-5619303159) |

</div>

<div class="table-number">Table 1</div>

The prototype counted exchange-wide fanout, including every producer; the final design measures outputs per producer. Its thresholds are therefore not directly comparable with the final default below.

These runs used 12 workers with 7 CPUs each. Total runtime improved 1–7% with no regressions, but the scale factor 10 baseline varied about 5%, putting most gains *within noise*. With only 84 CPUs, fanout and its removable overhead stayed modest.

### The final approach

We shipped the first approach as a **two-step shuffle**.

Rather than add a DFD-specific repartition operator, we composed the existing `NetworkShuffleExec` with DataFusion's `RepartitionExec`. This keeps the plan close to vanilla DataFusion—a core DFD goal—while avoiding another serialized plan variant.

**On the producer,** the hash maps rows to tasks, not final partitions, so `RepartitionExec` has T outputs. With T = 8 and P = 16, fanout drops from 128 to eight, a P-fold reduction. Each 8192-row batch then contributes about 1024 rows per output instead of 64, filling buffers after roughly eight inputs instead of 128.

**On the consumer,** the planner injects a local `RepartitionExec` above `NetworkShuffleExec`, splitting received rows as a single-node plan would. Each consumer fetches exactly one partition from each producer.

The two hashes must be independent. Without a salt they can correlate, especially when task and partition counts are powers of two, underusing some task-partition pairs. Salting the producer hash avoids that skew.

```{figure} ../_static/images/reducing-shuffle-fanout/shuffle-fanout-two-step-design-routed.svg
:alt: One producing task creates four task buckets instead of 16 partition buckets. A labeled dashed line marks the network transfer before NetworkShuffleExec. Four animated routes bend above the consuming tasks and enter them from the top, then each task locally repartitions into four partitions.
:width: 100%
:name: shuffle-fanout-two-step-design

**Figure 3**
```

Because the second repartition has a cost, the planner uses it only when T × P meets a [configurable threshold](https://github.com/datafusion-contrib/datafusion-distributed/blob/6dfa394bbd32c32c74989ff13f66abc0a0419806/src/distributed_planner/distributed_config.rs#L61). The default is 128, so eight tasks with 16 partitions take the two-step path; deployments can tune it for their cluster and workload.

## Benchmarks

We ran all 22 TPC-H scale factor 100 queries with one warmup and five measured iterations on 50- and 60-worker clusters of c5n.4xlarge instances with 15 CPUs each. As the PR evolved, each row captured its current base and PR revisions; the links contain full results. Runtime and speedup include only queries completed by both revisions, while **Completed** covers the full suite, including failures.

<div class="blog-numbered-table">

| PR revision | Workers | Completed (base → PR) | Runtime (base → PR) | Speedup |
|---|---:|---:|---:|---:|
| [c69add](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5661842281) | 60 | 16 → 22 | 67.0 s → 28.2 s | 2.37x |
| [44d3a4](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5672965814) | 50 | 11 → 22 | 40.0 s → 21.9 s | 1.83x |
| [c898db](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5674396432) | 50 | 18 → 22 | 58.9 s → 33.5 s | 1.76x |
| [c898db](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5675878399) | 60 | 15 → 22 | 61.6 s → 28.9 s | 2.13x |
| [9a2cb9](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5686077845) | 60 | 16 → 22 | 69.2 s → 32.9 s | 2.10x |
| [22ea4d](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5703344446) | 60 | 17 → 22 | 72.5 s → 34.6 s | 2.10x |
| [51a698](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5780896732) | 60 | 21 → 22 | 91.0 s → 45.5 s | **2.00x** |

</div>

<div class="table-number">Table 2</div>

The failures were part of the fanout problem, not benchmark noise. As [pointed out in the PR](https://github.com/datafusion-contrib/datafusion-distributed/pull/717#issuecomment-5662537593), wider fanout retained more data while partitions filled, exhausting worker memory. Reduced fanout filled and flushed batches sooner: several baselines completed only 11–18 queries while the PR completed all 22.

In the final run, the PR completed all 22 queries while the base completed 21. **Every comparable query got faster**, from 1.08x on q6 to 2.62x on q4; aggregate runtime fell from 91.0 to 45.5 seconds, and Query 12 changed from failure to completion. Reduced fanout therefore improved both runtime and reliability.

## Conclusion

Distributed shuffle cost includes per-output state, not just network bytes. Routing to T tasks before splitting into P local partitions reduces producer fanout from T × P to T, with larger gains as more outputs are avoided. A salt keeps the hashes independent, and a threshold leaves low-fanout shuffles on the direct path. In the final 60-worker run, comparable-query runtime fell from 91.0 to 45.5 seconds and Query 12 changed from failure to completion.

**What's next.** The default threshold of 128 still needs tuning across cluster sizes and workloads. `RepartitionExec` copies rows while partitioning and again while coalescing; reducing those two copies to one is tracked in [Improve RepartitionExec for better query performance](https://github.com/apache/datafusion/issues/7001#issuecomment-5801503499). DFD also lacks range repartitioning, a separate gap beyond hash shuffles.

Thanks to Gene Bordegaray ([@gene-bordegaray]), Gabriel ([@gabotechs]), and Richard Baah ([@Rich-T-kid]) for the design, implementation, and reviews behind this change.

[@Rich-T-kid]: https://github.com/Rich-T-kid
[@gene-bordegaray]: https://github.com/gene-bordegaray
[@gabotechs]: https://github.com/gabotechs

[^network-shuffle-layer]: Figures 1 and 3 draw `NetworkShuffleExec` as a separate layer to make the network boundary visible. In the physical plan, it is attached to each consuming task.
