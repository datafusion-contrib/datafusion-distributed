---
layout: post
title: "Optimizing Distributed Joins with Dynamic Filtering"
date: 2026-09-20
author: Jayant Shrivastava
categories: [features]
---

# Optimizing Distributed Joins with Dynamic Filtering

*September 20, 2026 · Jayant Shrivastava*

```{contents}
:local:
:depth: 2
```

## Background and Motivation

Single-node DataFusion implements an optimization called [dynamic filtering](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/),
which applies filters discovered during execution, but its shared-memory
mechanism **does not automatically work** when producers and consumers run in
a distributed environment.

Let's take a look at dynamic filtering in a single process. Consider this query, which joins a small `dim` table
with a large `fact` table:

```sql
SELECT f.*
FROM fact f
JOIN (
    SELECT d_key
    FROM dim
    WHERE region = 'EMEA'
) d
ON f.d_key = d.d_key;
```

A hash join first reads the small input, called the **build side**, and creates a
hash table from its join keys. It then reads the large **probe side** and looks
up each probe key in that table.

Without dynamic filtering, the probe side of the join incurs overhead for rows that will be discarded anyways:
- reading and decoding
- materialing columnar buffers
- hashing join columns
- any hypothetical operators that may exist between the data source and the join
  - evaluating expressions / projections
  - shuffles or repartitions
  - aggregations
  - etc.

With dynamic filtering, the join's filter can be pushed down to the data source, reducing
the rows early:

```{figure} ../_static/images/dynamic-filtering/single-node-dynamic-filter.svg
:alt: A hash join learns build keys B and D, sends an in-memory filter update to a fact-table scan, and receives only matching rows B and D from that scan.
:width: 100%

**Figure 1**
```

On a single node, this is an **atomic shared-memory update** from a producer to
a consumer. This works when the producer and consumer run in the same process (even in DataFusion-Distributed
when the producer and consumer are in the same task). However, remote plan nodes do not
share that process's memory, so a consumer in another machine cannot see the update directly.

```{figure} ../_static/images/dynamic-filtering/remote-probe-cannot-share-filter.svg
:alt: The hash join runs on Worker A, the build scan on Worker B, and the probe scan on Worker C. The join's shared-memory update stops at the process boundary, so the remote probe emits every row.
:width: 100%

**Figure 2**
```

Avoiding this work is valuable in single-node datafusion. In a distributed query it can be
even more valuable. Rows removed at the scan-level helps avoid avoid wasted work due to:
- serialization
- network transfer
- shuffle overhead
- CPU/memory on downstream operators

In this post, we will discuss how we implemented Dynamic Filtering in DataFusion-Distribued.

## Design

The design has two requirements.

1. Allow Dynamic Filters to Cross Network Boundaries
2. Route Dynamic Filters from Producers to the Corresponding Consumers

Note that (2) is tricky because dynamic filter producers such as joins may be partitioned across multiple machines
and see different data, producing distinct filters. Furthermore, consumers may be partitioned differently from
the producers.

To meet these requirements, we use a similar approach to [Trino](https://trino.io/docs/current/admin/dynamic-filtering.html)
and Spark:

1. Discover Dynamic Filter Producers and Consumers
2. Collect Dynamic Filters from Producers
3. Merge Dynamic Filters
4. Broadcast the Merged Filters to Consumers

### 1. Discover Dynamic Filter Producers and Consumers

Consider the plan below, divided vertically into 4 stages and horizonally into separate workers/tasks,
with Stage 1 running in 4 workers, Stage 2 running in 4 tasks etc.

We traverse the plan an annotate where the producers and consumers are. In Stage 3, there's 2 `HashJoinExec`
nodes producing dynamic filters in 2 tasks. In Stage 2, there's 4 `HashJoinExec` nodes producing
dynamic filters in 4 tasks. Finally, in Stage 1, there's a consumer `DataSourceExec` utilizing both filters.

In the sections below, we will discuss how filters are collected and safetly merged.

```text

┌───── DistributedExec
│ CoalescePartitionsExec
│   [Stage 3] => NetworkCoalesceExec
└──────────────────────────────────────────────
  ┌───── Stage 3 ── tasks=2
  │ DistributedExec
  │ AggregateExec
  │   HashJoinExec producers=[1]
  │     DataSourceExec
  │     AggregateExec
  │       [Stage 2] NetworkShuffleExec anchors=[1]
  └──────────────────────────────────────────────
    ┌───── Stage 2 ── tasks=4
    │ RepartitionExec
    │   HashJoinExec producers=[2]
    │     DataSourceExec
    │     AggregateExec
    │       NetworkShuffleExec anchors=[2]
    └──────────────────────────────────────────────
      ┌───── Stage 1  ── tasks=8
      │ RepartitionExec
      │   AggregateExec
      │     DataSourceExec consumers=[1, 2]
      └──────────────────────────────────────────────
```


### 2. Collect and Merge Dynamic Filters from Producers

#### Partitioned Hash Join

```{figure} ../_static/images/dynamic-filtering/remote-partitioned-join.svg
:alt: Two Stage 1 build tasks send separate dimension partitions to two hash joins. After the joins build their hash tables and report filters, four Stage 2 probe tasks receive the merged filter and scan the fact table.
:width: 100%

Figure 3
```

The join executes in two tasks, each producing a different filter, `F0 = key in (B, D)` and
`F1 = key in (G, H)`.

At the consumers, a partiticular row does not necessarily know which producer it will route to,
so we have to take the conservative approach of waiting for all producer filters to be reported
and unioning them before passing them on: `Fglobal = F0 OR F1 OR ... OR Fn`. This conjugate filter
is applied to each row at the scan level.

#### A Note on `CASE hash(expr)`

Every task is actually partitioned into multiple partitions denoted by [`target_partitions`](https://datafusion.apache.org/user-guide/configs.html),
often by hash partitioning. For partitioned joins, each join actually produces per-partition filters and
produces a `CASE hash(row) % num_partitions` expression which can be applied to each row.

Assume `target_partitions=4` for this running example.

```text
CASE hash(row) % 4
  WHEN 0 THEN F0_P0(row)
  WHEN 1 THEN F0_P1(row)
  WHEN 2 THEN F0_P2(row)
  WHEN 3 THEN F0_P3(row)
END
```

In Figure 3, we have 2 tasks making a total of 8 global partitions across the tasks but two
filters with 4 partitions each:

```text
CASE hash(row) % 4
  WHEN 0 THEN F0_P0(row)
  WHEN 1 THEN F0_P1(row)
  WHEN 2 THEN F0_P2(row)
  WHEN 3 THEN F0_P3(row)
END

OR

CASE hash(row) % 4
  WHEN 0 THEN F1_P0(row)
  WHEN 1 THEN F1_P1(row)
  WHEN 2 THEN F1_P2(row)
  WHEN 3 THEN F1_P3(row)
END
```

The question is, is this correct? The answer is **yes** due to this property:

```text
(hash(key) % M) % N = hash(key) % N, when M is a multiple of N
```

`M` in this example would be `2 tasks * 4 target_partitions = 8` and `N` would be `target_partitions=4`.
Say for example a row is routed to global partition `5` (ie. partition 1 on worker 1). In other words,
`hash(row) % 8 = 5`. By the property, `hash(row) % 4` must be be 1. Therefore, the
correct filter `F1_P1` is applied to the row.

Note that `F0_P1` would also be applied. This is safe because, if `F0_P1` rejected the row, `F1_P1`
can choose to admit it because of the `OR`. The only downside is some loss of selectivity
(we effectively have 4 filters instead of 8) in favor of simplicity. In the future, we may consider
baking the global partition index into the expressions.

#### `CollectLeft` Hash Join

Every task receives the same complete build side. Their predicates are
equivalent, so the coordinator can just forward the first completed filter
to all the consumers.

```{figure} ../_static/images/dynamic-filtering/remote-collect-left-join.svg
:alt: One Stage 1 build task scans the dimension table and broadcasts its complete build side to two CollectLeft joins. The coordinator accepts the first equivalent filter, then four Stage 2 probe tasks apply it to the fact table.
:width: 100%

Figure 4
```

#### MIN/MAX aggregate

A partial `MIN` turns each observed value into an upper bound; later, lower
values only tighten it. `MAX` works symmetrically. The coordinator therefore
uses `Incremental`, ORs the latest bound from every producer, and publishes each
new generation immediately.

```{figure} ../_static/images/dynamic-filtering/remote-min-aggregate.svg
:alt: Two partial MIN aggregates report successively lower values, which the coordinator merges into safe upper bounds for remote scans.
:width: 100%

Figure 5
```

#### TopK sort

For a descending TopK, each generation raises a lower bound. As with MIN/MAX,
the coordinator uses `Incremental`. OR keeps the least strict current bound,
ensuring that a row still useful to any producer passes the remote scan.

```{figure} ../_static/images/dynamic-filtering/remote-topk-sort.svg
:alt: Two TopK tasks report increasingly strict score bounds, which the coordinator ORs and sends back to remote scans so progressively more low scores are removed.
:width: 100%

Figure 6
```

### 3. Broadcasting Merged Filters to Consumers

Once a merged predicate is ready to be sent, the coordinator sends the filter
to each worker containing a filter that needs to be consumed. This workers
apply the filters during execution to their local plans.

Consumers do not necessarily wait for remote filters. With sorts and aggregates,
the scan often starts before the filter arrives. With joins, the probe side
waits to be polled (ie. waits for the build side).

## Contributing Upstream

Implementing distributed dynamic filtering exposed several opportunities to contribute
useful changes to the [apache/datafusion](https://github.com/apache/datafusion) core itself.

[`ExecutionPlan::apply_expressions()` (#24018)] restored a general API for
visiting physical expressions owned by physical execution plan nodes. Distributed
DataFusion uses it to find dynamic-filter consumers and DataFusion core uses
it to detect when/if filters were pushed down. It also means custom `ExecutionPlan`
implementations can opt in to dynamic filtering by exposing their expressions
through the same interface.

[`ExecutionPlan::dynamic_expressions_produced()` (#24068)] added the
complementary producer-facing API to `apply_expressions`. Visiting a plan's
expressions is enough to find consumers, but distributed routing must also know
which nodes produce and update each dynamic filter. The new trait method exposes that
explicitly, users discover producers without hardcoding the fixed `HashJoinExec`, `SortExec`,
and `AggregateExec` operatoes today.

[Serialize and deduplicate dynamic filters (#21807)] taught DataFusion's
protobuf conversion to preserve shared dynamic-filter identity. If a producer
and consumer reference the same memory before serialization, they must
continue to share the same memory decoded expression afterwards.

[Serialize dynamic filters on sort, aggregate, and hash-join plans (#22011)]
Ensures operators encode their dynamic filters rather than dropping them on serialization.

Together these changes make dynamic expressions in DataFusion discoverable, serializable,
and identity-preserving. Distributed DataFusion adds network routing on top,
but the underlying plans and expressions follow standard DataFusion practices.

## Benchmarks

Benchmarks were run using the [remote benchmarks tool](https://github.com/gabotechs/datafusion-distributed-dev-tools/tree/main/benchmarks-remote)
on a 12-node `c5n.4xlarge` cluster. We use TPC-H query 15 at scale factor 100 as
a case study because it is join-heavy and exercises a remote partitioned-join
filter. Q15 computes quarterly revenue grouped by supplier, references that
revenue relation twice, and joins it to the `supplier` table. The distributed
plan therefore scans `lineitem` twice and routes a supplier-key filter from a
partitioned hash join back to a remote scan.

### Query

The benchmark uses the standard TPC-H Q15 query:

```sql
CREATE VIEW revenue0 (supplier_no, total_revenue) AS
SELECT
    l_suppkey,
    SUM(l_extendedprice * (1 - l_discount))
FROM lineitem
WHERE l_shipdate >= DATE '1996-01-01'
  AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
GROUP BY l_suppkey;

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, revenue0
WHERE s_suppkey = supplier_no
  AND total_revenue = (
      SELECT MAX(total_revenue)
      FROM revenue0
  )
ORDER BY s_suppkey;

DROP VIEW revenue0;
```

### Executed plan

The following is a lightly redacted excerpt from the diagnostic execution with
Parquet pushdown and filter reordering enabled. It retains the actual stage and
operator hierarchy, including the dynamic predicate in the Stage 5 data
source. File names and byte ranges, repeated task-local scans, and unrelated
metrics are omitted.

```text
┌───── DistributedExec ── dynamic_filter_updates_received=21
│ SortPreservingMergeExec: [s_suppkey@0 ASC NULLS LAST]
│   [Stage 6] => NetworkCoalesceExec: output_partitions=84, input_tasks=12
└────────────────────────────────────────────────────────────────────────
  ┌───── Stage 6 ── tasks=12, partitions=7
  │ HashJoinExec: mode=CollectLeft, join_type=Inner,
  │   on=[(max(revenue0.total_revenue)@0, total_revenue@4)]
  │   CoalescePartitionsExec
  │     [Stage 3] => NetworkBroadcastExec
  │   SortExec: expr=[s_suppkey@0 ASC NULLS LAST]
  │     FilterExec: DynamicFilter [ empty ]
  │       HashJoinExec: mode=Partitioned, join_type=Inner,
  │         on=[(s_suppkey@0, supplier_no@0)]
  │         [Stage 4] => NetworkShuffleExec: output_partitions=7
  │         ProjectionExec: [l_suppkey AS supplier_no, sum(...) AS total_revenue]
  │           AggregateExec: mode=FinalPartitioned, gby=[l_suppkey]
  │             [Stage 5] => NetworkShuffleExec: output_partitions=7
  └──────────────────────────────────────────────────────────────────────
    ┌───── Stage 3 ── tasks=1, partitions=12
    │ BroadcastExec: consumer_tasks=12
    │   AggregateExec: mode=Final, aggr=[max(revenue0.total_revenue)]
    │     [Stage 2] => NetworkCoalesceExec: input_tasks=12
    └────────────────────────────────────────────────────────────────────
      ┌───── Stage 2 ── tasks=12, partitions=7
      │ AggregateExec: mode=Partial, aggr=[max(revenue0.total_revenue)]
      │   AggregateExec: mode=FinalPartitioned, gby=[l_suppkey]
      │     [Stage 1] => NetworkShuffleExec: output_partitions=7
      └──────────────────────────────────────────────────────────────────
        ┌───── Stage 1 ── tasks=12, partitions=84
        │ RepartitionExec: partitioning=Hash([l_suppkey@0], 84)
        │   AggregateExec: mode=Partial, gby=[l_suppkey]
        │     DistributedLeafExec:
        │       t0: DataSourceExec: file_groups={...}, file_type=parquet,
        │         predicate=l_shipdate >= 1996-01-01
        │                   AND l_shipdate < 1996-04-01
        │       ... 11 more task-local data sources ...
        └────────────────────────────────────────────────────────────────
    ┌───── Stage 4 ── tasks=1, partitions=84
    │ RepartitionExec: partitioning=Hash([s_suppkey@0], 84)
    │   DistributedLeafExec:
    │     t0: DataSourceExec: file_groups={...}, file_type=parquet,
    │       projection=[s_suppkey, s_name, s_address, s_phone]
    └────────────────────────────────────────────────────────────────────
    ┌───── Stage 5 ── tasks=12, partitions=84
    │ RepartitionExec: partitioning=Hash([l_suppkey@0], 84)
    │   AggregateExec: mode=Partial, gby=[l_suppkey]
    │     DistributedLeafExec:
    │       t0: DataSourceExec: file_groups={...}, file_type=parquet,
    │         predicate=l_shipdate >= 1996-01-01
    │                   AND l_shipdate < 1996-04-01
    │                   AND DynamicFilter [
    │                     CASE hash_repartition % 7
    │                       WHEN 0 THEN l_suppkey >= 53
    │                                   AND l_suppkey <= 999974
    │                       WHEN 1 THEN l_suppkey >= 83
    │                                   AND l_suppkey <= 999992
    │                       ... five more join partitions ...
    │                     END
    │                     OR ... eleven more producer-task predicates ...
    │                   ]
    │         dynamic_rg_pruning=eligible
    │       ... 11 more task-local data sources with the same predicate ...
    └────────────────────────────────────────────────────────────────────
```

Because `revenue0` is referenced twice, DataFusion executes its aggregation
twice rather than materializing the view. The filter examined here is produced
by the Stage 6 partitioned hash join and routed backward across the stage
boundary to the Stage 5 `lineitem` data sources. A separate local filter on
`total_revenue` implements the final maximum-revenue condition.

### Experiment

The experiment used DataFusion 55.0.0 with 12 workers, seven CPUs and 17 GiB of
memory per worker. Each case used one excluded warmup and five measured
executions. The first three cases varied Parquet row-filter pushdown and filter
reordering with dynamic filtering enabled. A fourth case disabled dynamic
filtering entirely while leaving both Parquet options enabled.

| Dynamic filtering | Parquet pushdown | Filter reordering | Median | Min–max |
|---|---|---|---:|---:|
| Enabled | Disabled | Disabled | 5.002 s | 4.714–6.509 s |
| Enabled | Enabled | Disabled | 5.873 s | 5.561–6.291 s |
| Enabled | Enabled | Enabled | 5.811 s | 5.506–6.365 s |
| **Disabled** | Enabled | Enabled | **6.150 s** | **5.597–7.744 s** |

All five disabled plans returned one row using 50 tasks. They contained no
dynamic-filter consumers, received zero dynamic-filter updates, and reported
zero row groups pruned by a dynamic filter. Their elapsed times were 7.744,
6.534, 6.150, 5.774, and 5.597 seconds.

The disabled control's median was 5.8% slower than the matching enabled case,
but the two cases were separate, non-interleaved runs about two hours apart and
their ranges overlap substantially. This is not sufficient evidence of a
dynamic-filtering speedup. The executed-plan metrics are more useful for
understanding what happened.

### Moving the ship-date filter into Parquet

Q15's two `lineitem` scans cover 600.02 million row occurrences each. Without
Parquet pushdown, each scan emits all of them and an ordinary `FilterExec`
immediately above the scan applies the static ship-date range. With pushdown,
that range is evaluated while decoding Parquet:

| Dynamic filtering | Parquet pushdown / reordering | Scan output rows | Rows filtered inside Parquet | Scan output bytes | Bytes scanned | Dynamic row groups pruned |
|---|---|---:|---:|---:|---:|---:|
| Enabled | Disabled / disabled | 1.200 billion | 0 | 49.48 GB | 11.20 GB | 0 |
| Enabled | Enabled / disabled | 45.34 million | 1.155 billion | 1.73 GB | 11.20 GB | 0 |
| Enabled | Enabled / enabled | 45.34 million | 1.155 billion | 1.73 GB | 11.20 GB | 0 |
| **Disabled** | Enabled / enabled | **45.34 million** | **1.155 billion** | **1.73 GB** | **11.20 GB** | **0** |

Pushdown removes **96.2%** of row occurrences and about **96.5%** of scan output
bytes before they leave the data source. It does not reduce reported file bytes
read, and the baseline's local `FilterExec` already removes the same rows before
aggregation. The same scan counts with dynamic filtering disabled confirm that
this reduction comes from the static ship-date predicate. Thus this experiment
moves work into the Parquet decoder rather than showing additional scan
reduction from the remote filter.

### What the remote filter looked like

One `lineitem` scan has only the static ship-date range. The other also receives
the supplier-key predicate from the remote partitioned hash join. Each
diagnostic execution received 21 dynamic-filter updates, and every remote scan
ended with the same correctness-complete union. Normalized and shortened, it
looked like this:

```text
DynamicFilter [
  CASE hash_repartition % 7
    WHEN 0 THEN l_suppkey >= 53  AND l_suppkey <= 999974 AND true
    WHEN 1 THEN l_suppkey >= 83  AND l_suppkey <= 999992 AND true
    ... five more join partitions ...
  END
  OR ... eleven more producer-task predicates ...
]
```

The final expression contains 12 producer predicates joined with `OR`; each is
a partition-aware `CASE` with seven ranges, for 84 range branches in total.
The exact-set component is `true`, and most bounds span nearly the full
one-million-key supplier domain. That is expected because Q15 does not apply a
selective condition to `supplier`.

The result is a useful negative case. The remote filter was discovered,
collected, merged, and delivered correctly, but it provided no additional scan
selectivity: the dynamic row-group pruning counter remained zero, and the scan
with the remote predicate emitted the same number of rows as the static-only
scan. Meanwhile, a separate local dynamic filter later in the plan reduced
approximately one million aggregate/join rows to the single final result. The
distinction matters: seeing a final dynamic predicate proves delivery, while
operator metrics reveal whether it actually avoided work.

## Tradeoffs and Future Work

OR-merging is deliberately general: the coordinator does not need to know the
internal shapes of range, set, or partition-aware predicates. The cost is that
the serialized and evaluated expression grows with the number of producer
tasks. First-class union support in DataFusion could compact compatible ranges,
sets, and `CASE` branches without brittle expression surgery in the distributed
engine.

The remote path is conservative only where correctness requires it:
partitioned hash-join filters wait for all producers, while TopK and MIN/MAX
bounds are sent incrementally. A future improvement could coalesce or
rate-limit very frequent generations, compact compatible bounds before
serialization, and extend incremental delivery to other producers with
monotonic predicates.

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
`OR`, and routes the result back to remote scans. For TopK and MIN/MAX, it also
routes useful intermediate generations so scans can become more selective while
the query is still running.

The result preserves DataFusion's existing scan pushdown machinery and its
extensible physical plan model. Local filters still use the fast in-memory
path; remote filters add only the coordination needed to cross processes. Most
importantly, the entire mechanism remains fail-open: it can eliminate I/O,
network traffic, and CPU, but it cannot change a query result.

Special thanks to Andrew Lamb ([@alamb]), Adrian Garcia Badaracco
([@adriangb]), Lía Adriana ([@LiaCastaneda]), Gabriel Musat Mestre
([@GabrielMusat]), and the [Apache DataFusion community] for their design,
implementation, and review work.

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
