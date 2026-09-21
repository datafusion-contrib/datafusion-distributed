---
layout: post
title: "Distributed Dynamic Filters: Passing Runtime Information Between DataFusion Workers"
date: 2026-09-20
author: Jayant Shrivastava
categories: [features]
---

# Distributed Dynamic Filters: Passing Runtime Information Between DataFusion Workers

*September 20, 2026 · Jayant Shrivastava*

```{contents}
:local:
:depth: 2
```

:::{note}
This is a draft. The benchmark measurements and charts will be added once the
results are available.
:::

## Motivation: Avoid Reading Rows That Cannot Join

Consider a familiar analytics query: join a large fact table to a much smaller,
filtered dimension table.

```sql
SELECT f.*
FROM fact f
JOIN (
    SELECT d_key
    FROM dimension
    WHERE region = 'EMEA'
) d
ON f.d_key = d.d_key;
```

A hash join first reads the small input, called the **build side**, and creates a
hash table from its join keys. It then reads the large **probe side** and looks
up each probe key in that table. If the dimension filter leaves only keys `B`
and `D`, every fact row whose key is not `B` or `D` will eventually be rejected
by the join.

The straightforward plan still reads those rows, decodes their columns, moves
them through the execution pipeline, and tests them against the hash table:

```text
             ┌─────────────────────┐
             │    HashJoinExec     │
             │ build hash table,   │
             │ then probe every row│
             └──────────┬──────────┘
                  ▲             ▲
                  │             │
       ┌──────────┴──────┐  ┌───┴──────────────────┐
       │ dimension scan  │  │ fact scan            │
       │ region = 'EMEA' │  │ reads A, B, C, D, ...│
       └─────────────────┘  └──────────────────────┘
```

**Figure 1:** A hash join without dynamic filtering. The fact scan does not know
which keys survived on the build side, so it reads rows that the join will
discard.

A dynamic filter sends the information learned while building the hash table
back to the probe-side scan. In this example the filter might be represented as
a range plus an exact set:

```text
f.d_key >= 'B' AND f.d_key <= 'D' AND f.d_key IN ('B', 'D')
```

The scan can use this predicate to skip rows and, when statistics permit, whole
Parquet row groups or files.

```text
             ┌─────────────────────┐
             │    HashJoinExec     │
             │ build keys: B, D    │
             └──────────┬──────────┘
                  ▲     │       ▲
                  │     │       │ only B and D
                  │     ▼       │
       ┌──────────┴──────┐  ┌───┴─────────────────────────┐
       │ dimension scan  │  │ fact scan                   │
       │ region = 'EMEA' │  │ DynamicFilter [B, D]        │
       └─────────────────┘  │ skips A, C, E, ...          │
                            └─────────────────────────────┘
```

**Figure 2:** The join passes its runtime knowledge sideways to the fact scan.
The optimization is often called *sideways information passing* because the
information moves against the normal, bottom-to-top flow of record batches.

Avoiding this work is valuable on one machine. In a distributed query it can be
even more valuable: rows removed at the scan also avoid serialization, network
transfer, repartitioning, and downstream CPU on other workers. The challenge is
that the join and scan may no longer share a process—or even run in the same
stage.

## Background: Dynamic Filtering in Single-Node DataFusion

The [DataFusion dynamic filtering blog post] describes the original design and
its use for TopK queries and hash joins. The central abstraction is
`DynamicFilterPhysicalExpr`, an updateable physical expression shared between a
producer and one or more consumers.

A filter starts with a predicate equivalent to `true`. The optimizer pushes the
same expression into a `DataSourceExec`, where it participates in the existing
filter pushdown and pruning machinery. During execution, a producer such as
`HashJoinExec`, `SortExec`, or `AggregateExec` replaces the inner predicate with
a more selective expression. Because both sides hold an `Arc` to the same
`DynamicFilterPhysicalExpr`, the scan sees the new predicate without rebuilding
the plan.

```text
                            shared Arc
                 ┌────────────────────────────┐
                 │ DynamicFilterPhysicalExpr  │
                 │ true  ->  key IN (B, D)    │
                 └─────────────┬──────────────┘
                               │
                  ┌────────────┴────────────┐
                  │                         │
          ┌───────▼────────┐        ┌───────▼─────────┐
          │ HashJoinExec   │        │ DataSourceExec  │
          │ producer       │        │ consumer        │
          └────────────────┘        └─────────────────┘
```

**Figure 3:** On one node, the producer and consumer share live expression
state. Updating the expression is enough to update the scan.

### Global and partition-aware filters

DataFusion uses two related forms of join filter. A **global** filter describes
all build keys visible to the join. Every probe partition can evaluate the same
predicate:

```text
key >= min_build_key
AND key <= max_build_key
AND key IN (build_key_set)
```

For a partitioned hash join, DataFusion can be more selective. Probe rows are
already routed to join partitions by hashing the join key, and each build
partition may contain different keys. A **partition-aware** filter captures
that routing in a `CASE` expression:

```sql
CASE hash(key) % 4
    WHEN 0 THEN key >= min_0 AND key <= max_0 AND key IN (set_0)
    WHEN 1 THEN key >= min_1 AND key <= max_1 AND key IN (set_1)
    WHEN 2 THEN key >= min_2 AND key <= max_2 AND key IN (set_2)
    WHEN 3 THEN key >= min_3 AND key <= max_3 AND key IN (set_3)
END
```

Only one branch is evaluated for a row, and it corresponds to the partition
that will later process that row. This can reject a key even when it falls
inside the global min/max range but does not exist in its destination build
partition.

The existing single-node implementation continues to work unchanged when a
distributed task contains both the producer and consumer. For example, a real
Distributed DataFusion test produces different filters for two task-local
scans:

```text
HashJoinExec: mode=Partitioned, on=[(d_dkey, f_dkey)]
  build: DataSourceExec: table=dim, predicate=service = 'log'
  probe task 0: DataSourceExec: table=fact,
      predicate=DynamicFilter [ f_dkey >= A AND f_dkey <= A AND f_dkey IN (A) ]
  probe task 1: DataSourceExec: table=fact,
      predicate=DynamicFilter [ f_dkey >= B AND f_dkey <= B AND f_dkey IN (B) ]
```

Each task has a normal DataFusion plan with normal shared expression state. No
coordinator involvement is needed for this local case.

## Why the Single-Node Mechanism Breaks When DataFusion Is Distributed

Distributed DataFusion takes a normal DataFusion physical plan and inserts
network boundaries. The resulting stages are serialized and sent to workers,
where multiple task-specific copies execute. Serialization can preserve shared
expression identity *within one decoded plan*, but it cannot create a shared
Rust `Arc` between processes or independently executing stages.

Suppose the join runs in stage 3 and its probe scan runs remotely in stage 2:

```text
Worker A: Stage 3                         Worker B: Stage 2
┌─────────────────────────┐               ┌─────────────────────────┐
│ HashJoinExec            │               │ RepartitionExec         │
│ produces filter id=42   │               │   DataSourceExec        │
│                         │               │     DynamicFilter [true]│
└────────────┬────────────┘               └────────────┬────────────┘
             │                                         │
             └────── NetworkShuffleExec ───────────────┘

       filter id=42 is updated here          a separate Arc lives here
```

**Figure 4:** A stage boundary severs the in-memory relationship. The producer
can update its copy forever without changing the remote scan's copy.

There is also a correctness trap. In a partitioned join, each producer task
sees only its portion of the build input. If task 0 reports `key IN (A)` while
task 1 owns key `B`, immediately applying task 0's predicate to every probe scan
would incorrectly remove rows with key `B`. A distributed implementation must
know which tasks produce a filter, decide when their combined information is
complete, merge it safely, and route it only to the corresponding consumers.

## Designing Distributed Dynamic Filters for DataFusion

The high-level design resembles the global paths in [Trino dynamic filtering]
and [Spark runtime filtering]. Trino collects task `Domain`s at its coordinator
and unions them before sending a filter to remote scans. Spark's Dynamic
Partition Pruning and runtime Bloom filtering similarly aggregate worker
results into a global filter. Distributed DataFusion follows the same safety
rule—combine a correctness-complete view of the build side—while carrying
normal DataFusion physical expressions and preserving partition-aware filters.

```text
 Producer task 0              Producer task 1
 complete predicate F0        complete predicate F1
          │                            │
          └─────────────┬──────────────┘
                        ▼
              ┌───────────────────┐
              │    Coordinator    │
              │ wait for all      │
              │ merge: F0 OR F1   │
              └─────────┬─────────┘
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
      Consumer task 0       Consumer task 1
      update filter id=42   update filter id=42
             │                     │
             ▼                     ▼
       DataSourceExec        DataSourceExec
```

**Figure 5:** The distributed dataflow for a partitioned join. Expression IDs
connect producer and consumer copies after serialization.

### Discovering producers and consumers

The coordinator owns the complete physical plan before distributing it, which
makes planning the best time to discover dynamic filters. Every dynamic
expression has a stable expression ID. A producer and all of its consumers
share the same ID even when their expression trees are later serialized into
different stages.

Two `ExecutionPlan` APIs make discovery extensible:

- `dynamic_expressions_produced()` identifies expressions produced by a plan
  node.
- `apply_expressions()` visits expressions used anywhere within a plan node,
  allowing consumers to be found without downcasting every known
  `ExecutionPlan` implementation.

When splitting a plan, Distributed DataFusion attaches a small metadata-only
**anchor** to each intervening network boundary. Anchors keep the dependency
visible when a consumer is several stages away from its producer. They are not
evaluated against rows. They simply say, "a consumer of expression 42 exists
below this boundary."

For example, the discovery tests reduce a multi-stage plan to the following
annotations:

```text
Stage 4  remote_producers=[42]
  HashJoinExec  producers=[42]
    NetworkShuffleExec
    AggregateExec
      NetworkShuffleExec  anchors=[42]

Stage 3
  AggregateExec
    NetworkShuffleExec  anchors=[42]

Stage 2
  AggregateExec
    DataSourceExec  consumers=[42]
```

The anchor can pass through any number of intermediate stages, so discovery is
based on the expression relationship rather than a particular plan shape.
Custom execution plans participate through the same APIs.

### Reporting complete filters

When a worker receives a task plan, it discovers the dynamic expressions that
must be reported to the coordinator. The worker listens for updates from those
producers and streams snapshots over the existing bidirectional
worker/coordinator channel.

The coordinator maintains a query-scoped registry keyed by expression ID. For
each filter it records the exact producer tasks, consumer tasks, completed
predicates, delivery state, and merge mode. Tracking task keys rather than only
stage IDs matters for plans such as distributed unions, where a filter may not
appear in every task in a stage.

For the initial implementation, a remote filter is forwarded only when it is
complete:

- A **partitioned** hash join uses `AllProducersComplete`. The coordinator waits
  until the stage is sealed—meaning no more producer tasks will be registered—
  and every registered producer has reported a complete predicate.
- A replicated **`CollectLeft`** join uses `FirstProducerComplete`. Every worker
  receives the same build input, so their completed filters are equivalent and
  the first copy is sufficient.

This distinction avoids a global barrier when it is unnecessary without
weakening correctness for partitioned joins.

### Merging filters safely

For a partitioned join, let `F0`, `F1`, ..., `Fn` be the predicates reported by
the producer tasks. The safe global predicate is their union:

```text
Fglobal = F0 OR F1 OR ... OR Fn
```

`AND` would be incorrect because it would keep only keys present in every build
partition. `OR` may admit a row that later routes to a different worker and
fails the join, but it never discards a row that could match. In other words,
the merged expression may lose some selectivity, but the query result is
unchanged.

The same rule works for partition-aware expressions. Each worker reports a
`CASE hash(key) % N` predicate, and the coordinator ORs the complete cases
together. Distributed DataFusion scales a repartition below the join to a
global partition count `M` that is a multiple of the join's local partition
count `N`. The key identity is:

```text
(hash(key) % M) % N = hash(key) % N       when M is a multiple of N
```

For example, if a row hashes to global partition 5 of 12, it reaches local
partition `5 % 4 = 1` on its destination worker. The dynamic filter also selects
case `hash(key) % 4 = 1`. Thus the case used at the scan remains aligned with
the join partition that will process the row.

### Routing merged filters back to scans

Once the coordinator has a merged predicate, it sends an
`ApplyDynamicFilter` message to every registered remote consumer task. The
worker locates the consumer by expression ID, decodes the predicate against the
consumer's input schema, updates its `DynamicFilterPhysicalExpr`, and marks the
filter complete. Multiple consumers with the same ID share state inside the
worker plan, so one update is enough.

Task-local consumers are deliberately skipped: their producer already updates
them through DataFusion's original shared-memory path.

Delivery is asynchronous and **fail-open**. A scan need not wait for the filter
before it starts, and an unknown expression ID, a closed task channel, or a
decode failure leaves the scan unfiltered rather than failing the query. A
filter that arrives earlier can prune more work, but timing changes performance,
not results.

Workers also report their final consumer expressions for plan visualization.
This lets the coordinator display the predicates that actually reached each
task instead of leaving every remote scan as `DynamicFilter [ empty ]` after
execution.

### A worked plan

The distributed dynamic-filtering integration suite includes this query:

```sql
SELECT COUNT(*)
FROM (
    SELECT DISTINCT "RainToday" AS key
    FROM weather
) build
JOIN weather probe
    ON build.key = probe."RainToday";
```

DataFusion turns the join into a right-semi join because only the row count is
needed. A shortened, normalized version of the executed plan looks like this:

```text
┌───── Stage 3: join tasks ──────────────────────────────────────┐
│ AggregateExec: count(*)                                       │
│   HashJoinExec: mode=Partitioned, join_type=RightSemi,         │
│                 on=[(key, RainToday)]                          │
│     AggregateExec: DISTINCT key                               │
│       [Stage 1] NetworkShuffleExec       <- build input        │
│     [Stage 2] NetworkShuffleExec         <- probe input        │
└───────────────────────────────────────────────────────────────┘

┌───── Stage 1: build tasks ─────────────────────────────────────┐
│ RepartitionExec: Hash(key)                                    │
│   AggregateExec: partial DISTINCT key                         │
│     DataSourceExec: weather, projection=[RainToday AS key]     │
└───────────────────────────────────────────────────────────────┘

┌───── Stage 2: remote probe tasks ──────────────────────────────┐
│ RepartitionExec: Hash(RainToday)                              │
│   task 0: DataSourceExec: weather,                            │
│           predicate=DynamicFilter [ expression_id_42 ]         │
│   task 1: DataSourceExec: weather,                            │
│           predicate=DynamicFilter [ expression_id_42 ]         │
└───────────────────────────────────────────────────────────────┘
```

The important detail is that the stage 2 scans are remote from the stage 3
`HashJoinExec`. During execution, the flow for `expression_id_42` is:

```text
1. Stage 3 task 0 completes its build and reports F0.
2. Stage 3 task 1 completes its build and reports F1.
3. The coordinator verifies that the producer stage is sealed and both
   expected reports are present.
4. The coordinator builds F0 OR F1.
5. Both stage 2 tasks receive the merged predicate and update expression 42.
6. Their DataSourceExec operators use the predicate for dynamic pruning.
```

The tests execute the same query with dynamic filter pushdown enabled and
disabled, sort the output batches, and assert that the results are identical.
They also cover local consumers, replicated and partitioned joins, dynamic task
counts, unions, filters spanning multiple shuffles, multiple consumers with
different source-column mappings, and colocated tasks.

## Building the Primitives Upstream in DataFusion

Distributed dynamic filtering exposed several capabilities that were useful in
DataFusion itself. We implemented them upstream rather than maintaining a
parallel set of private hooks.

[`ExecutionPlan::apply_expressions()` (#24018)] restored a general API for
visiting the expression roots owned by any execution plan node. Distributed
DataFusion uses it to find dynamic-filter consumers and anchors without a
registry of built-in node types. It also means custom `ExecutionPlan`
implementations can participate by exposing their expressions through the same
interface. This work reapplied and refined [the original implementation by Lía
Adriana (#20337)].

[Serialize and deduplicate dynamic filters (#21807)] taught DataFusion's
protobuf conversion to preserve shared dynamic-filter identity. If a producer
and consumer reference the same expression before serialization, they must
reference one shared decoded expression afterward; decoding two unrelated
objects with equal contents is not sufficient for live updates.

[Serialize dynamic filters on sort, aggregate, and hash-join plans (#22011)]
completed the producer side of the round trip. Dynamic expressions owned by
these operators now travel with physical plans instead of disappearing when a
plan crosses a process boundary.

Together these changes make dynamic expressions discoverable, serializable,
and identity-preserving. Distributed DataFusion adds network routing on top,
but the underlying plan and expression model remains standard DataFusion.

## Benchmarks

:::{admonition} Results pending
This section will contain the benchmark results, charts, and analysis. The
comparison will show end-to-end query time with distributed dynamic filtering
enabled and disabled, along with work avoided at the scan and shuffle layers.
:::

## Tradeoffs and Future Work

OR-merging is deliberately general: the coordinator does not need to know the
internal shapes of range, set, or partition-aware predicates. The cost is that
the serialized and evaluated expression grows with the number of producer
tasks. First-class union support in DataFusion could compact compatible ranges,
sets, and `CASE` branches without brittle expression surgery in the distributed
engine.

The initial remote path is conservative about timing. Partitioned hash-join
filters are sent only after all producers complete. TopK sorts and MIN/MAX
aggregates can produce successively tighter bounds, and some of those
intermediate generations are safe to apply eagerly. Supporting those remote
updates would let scans begin pruning sooner and would extend the optimization
to producer expressions that update but do not have the same completion
semantics as a hash-join build.

There is also more observability work to do. Useful metrics include filter
arrival time, serialized size, number of producer predicates merged,
selectivity, row groups and files pruned, and shuffle bytes avoided. These will
make it easier to distinguish a filter that was unselective from one that
arrived after most of the scan had already completed.

## Conclusion

Dynamic filtering begins as a simple shared-state technique: a producer updates
an expression and a scan reads it. A distributed plan turns that shared state
into an explicit dataflow. Distributed DataFusion discovers expression
relationships while the whole plan is available, preserves them across stage
boundaries with stable IDs, collects complete task predicates, merges them with
`OR`, and routes the result back to remote scans.

The result preserves DataFusion's existing scan pushdown machinery and its
extensible physical plan model. Local filters still use the fast in-memory
path; remote filters add only the coordination needed to cross processes. Most
importantly, the entire mechanism remains fail-open: it can eliminate I/O,
network traffic, and CPU, but it cannot change a query result.

Special thanks to Andrew Lamb ([@alamb]), Adrian Garcia Badaracco
([@adriangb]), Lía Adriana ([@LiaCastaneda]), Gabriel Musat Mestre
([@GabrielMusat]), and the [Apache DataFusion community] for their design,
implementation, and review work.

## Appendix

### Draft benchmark machine

These are the specifications of the current development machine. They are
placeholders until the final benchmark environment is confirmed.

| Component | Specification |
|---|---|
| Environment | Amazon EC2, `aarch64` |
| CPU | 16 Neoverse-N1 cores, 1 thread per core, 1 socket, 1 NUMA node |
| Cache | 1 MiB L1d, 1 MiB L1i, 16 MiB L2, 32 MiB shared L3 |
| Memory | 61 GiB RAM, 127 GiB swap |
| Storage | 400 GB Amazon EBS; 884.8 GB Amazon EC2 NVMe instance storage |
| Operating system | Ubuntu 22.04.5 LTS, Linux 6.8.0-1057-aws |

The final appendix will also include the benchmark queries, configuration, raw
results, and relevant physical plans.

[DataFusion dynamic filtering blog post]: https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/
[Trino dynamic filtering]: https://trino.io/docs/current/admin/dynamic-filtering.html
[Spark runtime filtering]: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/SupportsRuntimeV2Filtering.html
[`ExecutionPlan::apply_expressions()` (#24018)]: https://github.com/apache/datafusion/pull/24018
[the original implementation by Lía Adriana (#20337)]: https://github.com/apache/datafusion/pull/20337
[Serialize and deduplicate dynamic filters (#21807)]: https://github.com/apache/datafusion/pull/21807
[Serialize dynamic filters on sort, aggregate, and hash-join plans (#22011)]: https://github.com/apache/datafusion/pull/22011
[@alamb]: https://github.com/alamb
[@adriangb]: https://github.com/adriangb
[@LiaCastaneda]: https://github.com/LiaCastaneda
[@GabrielMusat]: https://github.com/GabrielMusat
[Apache DataFusion community]: https://datafusion.apache.org/community/
