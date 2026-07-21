# Distributed Dynamic Filtering

## Contents

1. Background - Dynamic Filtering in Vanilla DataFusion
2. Background - Dynamic Filtering in Trino and Spark 
2. Dynamic Filtering in Distributed DataFusion - Proposed Highlevel Design
3. Gaps to Address in Vanilla DataFusion - What's blocking us?

## 1. Background - Dynamic Filtering in Vanilla DataFusion

Dynamic filters come in two flavors:
- "global" dynamic filters
- "partition-routed" dynamic filters

"Global" dynamic filters are used by 
- `HashJoinExec`
- `AggregateExec`
- `SortExec`

"Partition-Routed" dynamic filters are used by `HashJoinExec: mode=Partitioned` only. They work by inserting
case expressions that capture the partitioning of the join.
```
CASE hash(expr) % N 
    WHEN 0: ..
    WHEN 1: .. 
    ...
    WHEN N-1: ..
```
Ultimately, they are just an optimization over "global" dynamic filters because they make the filters 
more granular, letting us prune more efficiently.

### "Global" dynamic filter
```
                                                                               DynamicFilterPhysicalExpr:                
                        ┌────────────────────────┐                                                                       
                        │     HashJoinExec:      │                a@0 >= v0 AND a@0 <= v1 AND a@0 IN (SET) ([v2, v3, v4, ...])  
                        │mode=CollectLeft on=a@0 │                                                                       
                        └────────────────────────┘                                        │                              
                               ▲       ▲                                                  │                              
                               │       │                                                  │                              
              ┌────────────────┘       └────────────────┐                                 │                              
              │                                         │                    Pushed down to data source                  
┌──────────────────────────┐              ┌──────────────────────────┐                    │                              
│     DataSourceExec:      │              │     RepartitionExec:     │                    │                              
│Partitioning=Hash(a, 4)   │              │ Partitioning=Hash(a, 4)  │                    │                              
└──────────────────────────┘              └──────────────────────────┘                    │                              
                                                        ▲                                 │                              
                                                        │                                 │                              
                                          ┌──────────────────────────┐                    │                              
                                          │     DataSourceExec:      │                    ▼                              
                                          │ Partitioning=Unknown(10) │   Each partition uses the same expression         
                                          └──────────────────────────┘
```

### "Partition-Aware Dynamic Filter"
```
                                                                           DynamicFilterPhysicalExpr:               
                        ┌────────────────────────┐                                                                  
                        │     HashJoinExec:      │                       CASE Hash(a@0) % 12    
                        │mode=Partitioned on=a@0 │                         WHEN 0: a@0 >= v0 AND a@0 <= v1
                        └────────────────────────┘                         WHEN 1: a@0 IN (SET) ([v2, v3, v4 ...])
                               ▲       ▲                                     ... (1 case per partition)                 
                               │       │                                                                            
              ┌────────────────┘       └────────────────┐                               │                           
              │                                         │                   Pushed down to data source              
┌──────────────────────────┐              ┌──────────────────────────┐                                              
│     DataSourceExec:      │              │     RepartitionExec:     │                  │                           
│Partitioning=Hash(a, 4)   │              │ Partitioning=Hash(a, 4)  │                  │                           
└──────────────────────────┘              └──────────────────────────┘                  │                           
                                                        │                               │                           
                                                        │                               │                           
                                                        │                               │                           
                                                        │                               │                           
                                                ... any number of RepartitionExecs or   │   
                                                    plan nodes. It does not matter      │ 
                                                        │                               │                           
                                                        │                               │                           
                                                        │                               ▼                           
                                          ┌──────────────────────────┐                                              
                                          │     DataSourceExec:      │  Each partition uses the same DynamicFilterPhysicalExpr, except     
                                          │ Partitioning=Unknown(10) │  each row will only hash to one case.                                              
                                          └──────────────────────────┘                                    
```

## 2. Background - Dynamic Filtering in Trino and Spark 

Ultimately, neither Trino and Spark apply send different filters to probe rows based on the
partition that will process them (ie. they are both similar to "global" dynamic filters
in datafusion).

### Trino

Trino uses "Domains" to represent a Set of Rows (ie. rows that would pass a dynamic filter)

1. CollectLeft

The build side of the join is identical across each worker, meaning each has the same Domain. The Domain `D` is sent to the coordinator
which forwards it to all the data sources.

2. Partitioned:

(a) Co-located partitioned hash join
```
Task 0                              Task 1
┌──────────────────────┐           ┌──────────────────────┐
│ Build drivers        │           │ Build drivers        │
│ D0,0 ∪ D0,1 → W0     │           │ D1,0 ∪ D1,1 → W1     │
│          │           │           │          │           │
│          ▼           │           │          ▼           │
│ Local probe scan     │           │ Local probe scan     │
│ uses W0              │           │ uses W1              │
└──────────────────────┘           └──────────────────────┘
```
Each task waits for all its local build drivers and unions their domains into Wi. The local scan can use Wi directly, so W0 and W1 may differ. There is no coordinator round trip for this path.

(b) Non-co-located partitioned hash join
```
Join task 0                         Join task 1
D0,0 ∪ D0,1 → W0                   D1,0 ∪ D1,1 → W1
      │                                   │
      └─────────────────┬─────────────────┘
                        ▼
                  Coordinator
               G = W0 ∪ W1 ∪ ...
                        │
           ┌────────────┴────────────┐
           ▼                         ▼
     Remote scan task 0        Remote scan task 1
     uses global G             uses global G
```
The coordinator waits for all expected join task filters (we must collect all of them for correctness), unions their domains into G, and sends the
same G to the remote scan tasks. It may also use G for connector split pruning.

Sources
- Trino revision [`d4a390f3cbb293d14ee3a2b1152062332f4ec17e`](https://github.com/trinodb/trino/tree/d4a390f3cbb293d14ee3a2b1152062332f4ec17e)
- [dynamic filtering documentation](https://trino.io/docs/current/admin/dynamic-filtering.html)
- [dynamic row filtering](https://github.com/trinodb/trino/pull/22411).


### Spark

Spark uses "Dynamic Partition Pruning" (DPP) and "Runtime Bloom Filtering" to implement dynamic filtering.

DPP
1. Executors compute build keys
2. Distributed distinct/broadcast stage produces a global key set K
3. Driver collects global key set K
4. Driver prunes partition/file listings
5. Only selected scan tasks are launched

Bloom filtering:
1. Executors build partial Bloom filters
2. Spark aggregate merges them
3. One executor produces the final Bloom byte array
4. Driver collects that single scalar result
5. Probe tasks receive/use the same Bloom predicate

Sources
- Spark revision [`035d510b029be1f08219f5e94585952a655073fd`](https://github.com/apache/spark/tree/035d510b029be1f08219f5e94585952a655073fd)
- [Spark 3.0 dynamic partition pruning release notes](https://spark.apache.org/releases/spark-release-3-0-0.html)
- [Spark 3.2 DPP and AQE release notes](https://spark.apache.org/releases/spark-release-3-2-0.html)
- [`SupportsRuntimeV2Filtering` documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/SupportsRuntimeV2Filtering.html)
- [Dynamic partition-pruning physical planning](https://github.com/apache/spark/blob/035d510b029be1f08219f5e94585952a655073fd/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PlanDynamicPruningFilters.scala)
- [Broadcast key collection for DPP](https://github.com/apache/spark/blob/035d510b029be1f08219f5e94585952a655073fd/sql/core/src/main/scala/org/apache/spark/sql/execution/SubqueryBroadcastExec.scala)
- [Runtime Bloom-filter injection](https://github.com/apache/spark/blob/035d510b029be1f08219f5e94585952a655073fd/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/InjectRuntimeFilter.scala)
- [Distributed Bloom-filter aggregation and merging](https://github.com/apache/spark/blob/035d510b029be1f08219f5e94585952a655073fd/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/BloomFilterAggregate.scala)

## 3. Distributed Dyanmic Filters

Distributed Dyanmic Filtering Falls into 2 cases
- "local case" - when the producer `ExecutionPlan` node is on the same machine as the consumer/DataSourceExec
- "remote case" - when the producer `ExecutionPlan` node is on a different machine as the consumer/DataSourceExec

### Local Case

Below, you can see a simplified disrtibuted datafusion plan. Dynamic filtering should "just work" today with no loss of selecivity.

In datafusion-distributed, the coordinator serializes parts of the plan and sends them to the workers. We expect each
task's copy of the plan to behave like a single node plan with dynamic filters working; there are tests
in vanilla datafusion that validate this behavior.

Note that each `HashJoinExec` produces its own dynamic filters, meaning there are 3 in total.

#### Global Dynamic Filters
Each task has its own dynamic filters which do not have `CASE` expressions. They may look like this:

Task 1: `DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1`  
Task 2: `DynamicFilterPhysicalExpr: a@0 >= v2`  
Task 3: `DynamicFilterPhysicalExpr: a@0 IN LIST [v3, v4 ...]`  

#### Partition-Aware Dynamic Filters
Each task has its own dynamic filters which contain `CASE` expressions. They may look like this:

Task 1:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
```
Task 3:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v6
... 4 cases in total
```
Note that in each task / worker, partitions are indexed from 0, meaning the CASE expressions in each task/worker all use
`Hash(a@0) % 4 = 0` to `Hash(a@0) % 4 = 3`.

```
                                                                          ┌───────────────────────────┐ ┌───────────────────────────┐                                                                     
                                                                          │            ...            │ │             ...           │                                                                     
                                                                          │  ┌───────────────────────┐│ │ ┌───────────────────────┐ │                                                                     
                                                                          │  │  NetworkShuffleExec   ││ │ │  NetworkShuffleExec   │ │                                                                     
                                                                          │  │                       ││ │ │                       │ │                                                                     
                                                                          │  └───────────────────────┘│ │ └───────────────────────┘ │                                                                     
                                                                          └───────────────────────────┘ └───────────────────────────┘                                                                     
                                                                                         ▲                            ▲                                                                                   
                                                                                         │                            │                                                                                   
                                  ┌──────────────────────────────────────────────────────┴────────────┬───────────────┴───────────────────────────────────────────────────┐                               
                                  │                                                                   │                                                                   │                               
┌─────────────────────────────────┴──────────────────────────────┐  ┌─────────────────────────────────┴──────────────────────────────┐  ┌─────────────────────────────────┴──────────────────────────────┐
│                     ┌───────────────────────┐                  │  │                    ┌───────────────────────┐                   │  │                    ┌───────────────────────┐                   │
│                     │    RepartitionExec    │                  │  │                    │    RepartitionExec    │                   │  │                    │    RepartitionExec    │                   │
│                     │                       │                  │  │                    │                       │                   │  │                    │                       │                   │
│                     └───────────────────────┘                  │  │                    └───────────────────────┘                   │  │                    └───────────────────────┘                   │
│                    ┌────────────────────────┐                  │  │                    ┌────────────────────────┐                  │  │                    ┌────────────────────────┐                  │
│                    │     HashJoinExec:      │                  │  │                    │     HashJoinExec:      │                  │  │                    │     HashJoinExec:      │                  │
│                    │   mode=DoesNotMatter   │                  │  │                    │   mode=DoesNotMatter   │                  │  │                    │   mode=DoesNotMatter   │                  │
│                    └────────────────────────┘                  │  │                    └────────────────────────┘                  │  │                    └────────────────────────┘                  │
│                           ▲       ▲                            │  │                           ▲       ▲                            │  │                           ▲       ▲                            │
│                           │       │                            │  │                           │       │                            │  │                           │       │                            │
│                ┌──────────┘       └───────────┐                │  │                ┌──────────┘       └───────────┐                │  │                ┌──────────┘       └───────────┐                │
│                │                              │                │  │                │                              │                │  │                │                              │                │
│  ┌──────────────────────────┐                                  │  │  ┌──────────────────────────┐                                  │  │  ┌──────────────────────────┐                                  │
│  │     DataSourceExec:      │         ... random operators     │  │  │     DataSourceExec:      │         ... random operators     │  │  │     DataSourceExec:      │          ... random operators    │
│  │ Partitioning=Hash(a, 12) │                 ▲                │  │  │ Partitioning=Hash(a, 12) │                 ▲                │  │  │ Partitioning=Hash(a, 12) │                 ▲                │
│  └──────────────────────────┘                 │                │  │  └──────────────────────────┘                 │                │  │  └──────────────────────────┘                 │                │
│                                               │                │  │                                               │                │  │                                               │                │
│                                 ┌──────────────────────────┐   │  │                                 ┌──────────────────────────┐   │  │                                 ┌──────────────────────────┐   │
│                                 │     DataSourceExec:      │   │  │                                 │     DataSourceExec:      │   │  │                                 │     DataSourceExec:      │   │
│                                 │ Partitioning=Unknown(12) │   │  │                                 │ Partitioning=Unknown(12) │   │  │                                 │ Partitioning=Unknown(12) │   │
│                                 └──────────────────────────┘   │  │                                 └──────────────────────────┘   │  │                                 └──────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────────────┘
                  Task 1 Runs partitions [0,4)                                         Task 2 Runs partitions [4,8)                                        Task 3 Runs partitions [8,12)                  
```

### Remote Case

The proposed change is to implement dynamic filtering similarly to Trino.

1. During execution, the dynamic filter producers (`HashJoinExec`, `SortExec`, `AggregateExec`) will send dynamic filter updates to the coordinator.
2. The coordinator will union / merge the dynamic filter updates from all workers.
3. The coordinator will send the unioned / merged dynamic filter updates to the consumers (`DataSourceExec`).

For correctness, the coordinator must wait for all dynamic filter updates from all workers before sending them to the consumers, otherwise, the
consumers may prune rows incorrectly.

#### Global Filters

Consider if these "global" dynamic filters generated in stage 2

Task 1: `DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1`
Task 2: `DynamicFilterPhysicalExpr: a@0 >= v2`
Task 3: `DynamicFilterPhysicalExpr: a@0 IN LIST [v3, v4 ...]`

What is the correct filter to push to Stage 1 Task 1 and Stage 1 Task 2? Any row from Stage 1 Task 1 may appear in any task of Stage 2. 

##### Option 1: OR the Filters Together

`DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1 OR a@0 >= v2 OR a@0 IN LIST [v3, v4 ...]`

Pros:
It's simple.

Cons: 
- Loss of selectivity (not worse than single node execution which would have had one filter anyways). We may allow a row to pass a filter due to
  the dynamic filter from a task, but this row may route to a different task where it gets filtered out by the join. 
- It's a bit of a smell to start modifying the `PhysicalExpr` inside the dynamic filter. It's not horrible though. We take the expression
  from each task and wrap them in OR `BinaryExpr`.
- ORing the filters increases the size of the filter, meaning there's more overhead from serde, parsing, filter evaluation, and network transfer.

##### Option 2: Combine the Range and IN LIST expressions

Similar to the above except you try to avoid ORing.

We could try converting these filters from this 
`DynamicFilterPhysicalExpr: a@0 >= 0 AND a@0 <= 5 OR IN LIST [10, 11]`
`DynamicFilterPhysicalExpr: a@0 >= 1 AND a@0 <= 10 OR IN LIST [12, 13]`  

To this
`DynamicFilterPhysicalExpr: a@0 >= 0 AND a@0 <= 10 OR IN LIST [10, 11, 12, 13]`

Pros:
- Less overhead than ORing

Cons:
- Is brittle. What if we have to support non-range and non-IN-LIST expressions? It would be nice if dynamic filters natively implemented a `merge` or `union` operation.

##### Proposal

This RFC proposes that we implement Option 1 first in distributed datafusion. Option 2 can be explored later (either in datafusion-distributed
or in vanilla datafusion as a `merge()` or `union()` API if this sort of change is permitted).

#### Partition-Aware Filters

Consider if these "partition-routed" dynamic filters generated in stage 2

Task 1:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
```
Task 3:
```
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v6
... 4 cases in total
```

##### Idea: OR the Cases Together
```
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
OR 
CASE Hash(a@0) % 4
WHEN 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
OR
CASE Hash(a@0) % 4
WHEN 0: a@0 >= v6
... 4 cases in total
```

Note: This is the equivalent to having 1 case expression and ORing the cases together. ex.
```
CASE Hash(a@0)
WHEN 0: a@0 >= v0 AND a@0 <= v1 OR a@0 IN LIST [v2, v3, v4 ...] OR a@0 >= v6
WHEN 1: ... OR ... OR ...
```
In fact, filter evaluation may even be cheaper if implemented this way.

Note: The cases are all `% 4` and range from `0` to `3`. This happens because distributed datafusion's
execution model sets the `partitioning` of the `HashJoinExec` to `Hash(..., 4)` (`target_partitions=4` in the running example).

###### Correctness

Notice in the diagram that the `RepartitionExec` repartitions by 12 partitions, yet the `CASE hash(expr)` uses `% 4`. Each per-worker
plan assumes `partitioning=Hash(a, 4)` even though the partitioning is technically `Hash(a, 12)`. This begs the question: Is applying dynamic
filtering with respect to `% 4` correct?

The answer is that this is correct. There's two subtle reasons for why:

1. The key invariant in datafusion-distributed is that any `RepartitionExec` will repartition by a multiple of `target_partitions` and always exists
below a partitioned hash join (it always exists because [no data sources can claim `distribution=Hash`](https://github.com/apache/datafusion/blob/5b825e0d7710ed9228b809ba2e487aaea7854708/datafusion/physical-expr/src/partitioning.rs?plain=1#L748), therefore, there must be a `RepartitionExec`).
In the example, we have 3 tasks with 4 partitions each, meaning the `RepartitionExec` below the `HashJoinExec` repartitions by 12. The `HashJoinExec` itself will use `target_partitions` partitions, which is `4` in the example.
It's also acceptable for there to be an arbitrary number of `RepartitionExec`s below the `HashJoinExec`, the final one determines the partitioning of the join.

2. Generally `(x % M) % N = x % N when M is a multiple of N` (with M=12 and N=4 in the running example). Say `Hash(a@0) % 12 = 5`. This means that the
row would end up in partition 1 of task 2 (ie. filtering by `Hash(a@0) % 4 = 1` "aligns" with `Hash(a@0) % 12 = 5`). Because "alignment" is maintained, then
dynamic filtering works. A dynamic filter can be applied with respect to `% 4`, repartitioned by `% 12`, and will still end up in the right `% 4` bucket
on a worker.

Another way to think about this is that partitioning is identical within a worker as across workers. We cannot claim `Hash` partitioning within a worker when
there's `M` global partitions and `N` local partitions unless `M` is a multiple of `N`. If we were range partitioned
globally into 12 ranges on `Column(a)`, then it's correct to claim range partitioning on `Column(a)` within each worker regardless of the number of partitions (assuming each worker
gets contiguous partitions). It's also easy to see why `OR`ing any hypothetical `CASE Range(a@0)` dynamic filters would work; it's because the ranges are disjoint:
```
CASE Range(a@0)
WHEN 0: a@0 >= v0 AND a@0 < v1 // distinct range
...
OR 
CASE Range(a@0)
WHEN 0: v1 <= a@0 < v2 // distinct range
```

Pros:
- Simple.

Cons:
- It's brittle. When will this stop working?
  - If the `RepartitionExec` below a `NetworkShuffleExec` produces a partition count that is not a multiple of `target_partitions`.
  [Here](https://github.com/datafusion-contrib/datafusion-distributed/blob/a6c326807fa3a5ff05b4b7e08a1bd1e3cd7bfe53/src/execution_plans/network_shuffle.rs?plain=1#L160) is where we scale up the `RepartitionExec` today
  - If there are repartitions which do not repartition by `target_partitions`. Unsure if these happen in vanilla datafusion. 
- Loss of selectivity. We may allow a row to pass a filter due to the dynamic filter from a task, but this row may route to a different task.

##### Proposal

This RFC proposes that we `OR` the cases together. If loss of selectivity is a conern, this can be addressed in a future implementation of distributed dynamic filtering. Furthermore,
supporting an official `union()` or `merge()` operation for dynamic filters in vanilla datafusion would be a nice addition so we don't have to perform "expression surgery" in datafusion-distributed.

```
                                                                                      ┌───────────────────────────┐ ┌───────────────────────────┐                                                                  
                                                                                      │            ...            │ │             ...           │                                                                  
                                                                        ┌───────┐     │  ┌───────────────────────┐│ │ ┌───────────────────────┐ │                                                                  
                                                                        │Stage 3│     │  │  NetworkShuffleExec   ││ │ │  NetworkShuffleExec   │ │                                                                  
                                                                        └───────┘     │  │                       ││ │ │                       │ │                                                                  
                                                                                      │  └───────────────────────┘│ │ └───────────────────────┘ │                                                                  
                                                                                      └───────────────────────────┘ └───────────────────────────┘                                                                  
                                                                                                     ▲                            ▲                                                                                
                                                                                                     │                            │                                                                                
                                              ┌──────────────────────────────────────────────────────┴────────────┬───────────────┴───────────────────────────────────────────────────┐                            
                                              │                                                                   │                                                                   │                            
            ┌─────────────────────────────────┴────────────────────────────┐    ┌─────────────────────────────────┴────────────────────────────┐    ┌─────────────────────────────────┴───────────────────────────┐
            │                     ┌───────────────────────┐                │    │                    ┌───────────────────────┐                 │    │                    ┌───────────────────────┐                │
            │                     │    RepartitionExec    │                │    │                    │    RepartitionExec    │                 │    │                    │    RepartitionExec    │                │
            │                     │                       │                │    │                    │                       │                 │    │                    │                       │                │
            │                     └───────────────────────┘                │    │                    └───────────────────────┘                 │    │                    └───────────────────────┘                │
            │                    ┌────────────────────────┐                │    │                    ┌────────────────────────┐                │    │                    ┌────────────────────────┐               │
            │                    │     HashJoinExec:      │                │    │                    │     HashJoinExec:      │                │    │                    │     HashJoinExec:      │               │
            │                    │   mode=DoesNotMatter   │                │    │                    │   mode=DoesNotMatter   │                │    │                    │   mode=DoesNotMatter   │               │
            │                    └────────────────────────┘                │    │                    └────────────────────────┘                │    │                    └────────────────────────┘               │
┌───────┐   │                           ▲      ▲                           │    │                           ▲      ▲                           │    │                           ▲      ▲                          │
│Stage 2│   │                           │      │                           │    │                           │      │                           │    │                           │      │                          │
└───────┘   │                ┌──────────┘      └───────────┐               │    │                ┌──────────┘      └───────────┐               │    │                ┌──────────┘      └───────────┐              │
            │                │                             │               │    │                │                             │               │    │                │                             │              │
            │  ┌──────────────────────────┐  ┌──────────────────────────┐  │    │  ┌──────────────────────────┐  ┌──────────────────────────┐  │    │  ┌──────────────────────────┐  ┌──────────────────────────┐ │
            │  │     DataSourceExec       │  │    NetworkShuffleExec    │  │    │  │     DataSourceExec       │  │    NetworkShuffleExec    │  │    │  │     DataSourceExec       │  │    NetworkShuffleExec    │ │
            │  │                          │  │                          │  │    │  │                          │  │                          │  │    │  │                          │  │                          │ │
            │  └──────────────────────────┘  └──────────────────────────┘  │    │  └──────────────────────────┘  └──────────────────────────┘  │    │  └──────────────────────────┘  └──────────────────────────┘ │
            └──────────────────────────────────────────────────────────────┘    └──────────────────────────────────────────────────────────────┘    └─────────────────────────────────────────────────────────────┘
                                                                                                                                                                                                                   
                               Task 1 Runs partitions [0,4)                                       Task 2 Runs partitions [4,8)                                        Task 3 Runs partitions [8,12)                
                                                                                                                                                                                                                   
                                             ▲                                                                  ▲                                                                   ▲                              
                                             │                                                                  │                                                                   │                              
                                             └──────────────────────────────────────────────┬───────────────────┴────────────────┬──────────────────────────────────────────────────┘                              
                                                                                            │                                    │                                                                                 
                                                                                            │                                    │                                                                                 
                                                                            ┌──────────────────────────────┐     ┌──────────────────────────────┐                                                                  
                                                                            │ ┌──────────────────────────┐ │     │ ┌──────────────────────────┐ │                                                                  
                                                                            │ │     RepartitionExec      │ │     │ │     RepartitionExec      │ │                                                                  
                                                                            │ │ partitioning=Hash(a, 12) │ │     │ │ partitioning=Hash(a, 12) │ │                                                                  
                                                                            │ └──────────────────────────┘ │     │ └──────────────────────────┘ │                                                                  
                                                                            │               ▲              │     │               ▲              │                                                                  
                                                                            │               │              │     │               │              │                                                                  
                                                             ┌───────┐      │     ... random operators     │     │     ... random operators     │                                                                  
                                                             │Stage 1│      │                              │     │                              │                                                                  
                                                             └───────┘      │               ▲              │     │               ▲              │                                                                  
                                                                            │               │              │     │               │              │                                                                  
                                                                            │ ┌─────────────┴────────────┐ │     │ ┌─────────────┴────────────┐ │                                                                  
                                                                            │ │     DataSourceExec:      │ │     │ │     DataSourceExec:      │ │                                                                  
                                                                            │ │ Partitioning=Unknown(8)  │ │     │ │ Partitioning=Unknown(8)  │ │                                                                  
                                                                            │ └──────────────────────────┘ │     │ └──────────────────────────┘ │                                                                  
                                                                            └──────────────────────────────┘     └──────────────────────────────┘                                                                  
                                                                                                                                                                                                                   
                                                                              Task 1 Runs partitions [0, 4)        Task 2 Runs partitions [4, 8)                                                                  
                                                                                                                                                                                                                  
```


## Implementation Details

### Extracting Dynamic Filters

The design fundamentally relies on distributed datafusion intercepting and modifying dynamic filter updates. `DynamicFilterPhysicalExpr` already offers APIs
for this:
```rust
// Write Updates
pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()>

// Read Updates
pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>>
pub(crate) fn subscribe(&self) -> DynamicFilterSubscription
```

The more challenging part is extracting `DynamicFilterPhysicalExpr` from `ExecutionPlan` nodes. We need a way to get
`&DynamicFilterPhysicalExpr` from producers like `HashJoinExec`, `SortExec`, `AggregateExec` and consumers like
`DataSourceExec`. We will talk about this more below in the `Gaps in Vanilla DataFusion` section.

### Routing Dynamic Filters

During planning, which entirely happens on the coordinator (even during adaptive query planning), we need to
be able to traverse the query plan and find the `DynamicFilterPhysicalExpr` producers and consumers. The coordinator
knows workers will contain the producers nodes and consumers, so it is just a matter of plumbing to send dynamic
filter updates from the producer workers, to the coordinator (where they can be ORed or merged), and finally
to the consumers.

One open question we discuss below is - how do you know if a node is a producer or consumer of dynamic filters?

`expression_id` may useful to correlate dynamic filters across machines.
```rust
fn expression_id(&self) -> Option<u64> {
```

### Displaying Dyanamic Filters

Today, distributed query plans will always show `predicate=DynamicFilter [ empty ]`. In addition to making
dynamic filter pruning work during execution, after execution we must propagate final dynamic
filters from data sources to the coordinator so they can be displayed.


## Gaps in Vanilla DataFusion

There are 2 main gaps in vanilla datafusion that we need to address:
1. How do you get `&DynamicFilterPhysicalExpr` from `ExecutionPlan` nodes?
2. How do you merge dynamic filters?

### 1. Getting `&DynamicFilterPhysicalExpr` from `ExecutionPlan`

Requirements
(a) identify which dynamic filters are producers and consumers in an `ExecutionPlan` tree
(b) allow easy extension for custom `ExecutionPlan` implementations

#### Prior Art

##### `ExecutionPlan::apply_expressions`

See https://github.com/apache/datafusion/pull/22437

```rust
fn apply_expressions(
    &self,
    f: &mut dyn FnMut(
        &dyn datafusion::physical_plan::PhysicalExpr,
    ) -> Result<TreeNodeRecursion>,
) -> Result<TreeNodeRecursion>
```

Pros:
- Extracts `&DynamicFilterPhysicalExpr` from `ExecutionPlan`

Cons:
- You still need to downcast to `&DynamicFilterPhysicalExpr` and ignore non-dynamic-filter expressions
- It doesn't tell you if a node is a producer or consumer. You would have to downcast the `ExecutionPlan`
  and use the concrete type like `HashJoinExec` to know if it is a producer or consumer
  - This is potentially brittle because producers / consumers might change. What if a `HashJoinExec`
    becomes a consumer one day?
  - What about custom `ExecutionPlan` implementations? We would need some type of registry of known
    producers and consumers so users can register their own implementations
- `apply_expressions` was ultimately reverted because
  - It was complicated to implement and a required method on all `ExecutionPlan` nodes
  - There was no usage inside of vanilla datafusion, meaning it may grow stale

##### `OptimizerContext` + Session-Scoped Dynamic Filter Registry

[`PhysicalOptimizerRule::optimize_plan`](https://github.com/apache/datafusion/pull/18739) added an
`OptimizerContext` so optimizer rules could access `SessionConfig` extensions. It began replacing
the existing optimizer API:

```rust
// Before
fn optimize(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>>;

// After
fn optimize_plan(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    context: &OptimizerContext,
) -> Result<Arc<dyn ExecutionPlan>>;
```

A [prototype](https://github.com/gabotechs/datafusion/pull/7) built on this API by storing a
`DynamicFilterRegistry` in `SessionConfig`. However, this was subsequently [reverted](https://github.com/apache/datafusion/pull/19186).
The new public API and deprecation path were considered too broad without a concrete vanilla
DataFusion use case.

##### `ExecutionPlan::handle_child_pushdown_result` / `ExecutionPlan::gather_filters_for_pushdown`
```rust
fn handle_child_pushdown_result(
    &self,
    _phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult,
    _config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>

fn gather_filters_for_pushdown(
    &self,
    _phase: FilterPushdownPhase,
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    _config: &ConfigOptions,
) -> Result<FilterDescription>
```

There's a few reasons not to use these to collect dynamic filters:
- They describe how to perform filter pushdown, they do not really answer "what dynamic filters and producer-consumer relations exist in the optimized plan?". We would have
  to modify these methods heavily.
- The required inputs, such as `ChildPushdownResult`, `parent_filters`, and `FilterPushdownPhase`, only make sense inside the original pushdown traversal. Not during
  distributed planning.
- Assorted edge cases:
  - For a `HashJoinExec` that has already pushed down its filter `gather_filters_for_pushdown` will not return the existing dynamic filter again.
  - For a `HashJoinExec` that was not able to push down its filter, calling `gather_filters_for_pushdown` can create a fresh filter where no `DataSourceExec` uses it.
  - `handle_child_pushdown_result` doesn't even extract filters. Instead, it returns an `ExecutionPlan` that has filters.

#### Other Ideas

##### Collect Runtime Filter Bindings During Filter Pushdown

Similar to the "Session-Scoped Dynamic Filter Registry" option above, but it avoids breaking changes. The
goal is to add a `FilterPushdownReport` in the `SessionState` containing dynamic filters.

```rust
struct FilterPushdownReport {
    // One logical runtime filter can have multiple producer/consumer plan nodes.
    bindings: HashMap<u64, Vec<RuntimeFilterBinding>>, // expression_id -> bindings
}

enum RuntimeFilterBinding {
    Producer(Arc<dyn PhysicalExpr>),
    Consumer {
        expression: Arc<dyn PhysicalExpr>,
        usage: FilterUsage, // Pruning or FullyEvaluated
    },
}
```

The optimizer `PushdownFilters` rule would change in the following way:
```text
1. create report
2. During the gather self-filters stage, record producer by expression_id
3. During the upwards pass record retention at consumer
4. Return report to query planner
```
On the downward pass, hook immediately after `gather_filters_for_pushdown`:

```rust
let description = node.gather_filters_for_pushdown(phase, parent_filters, config)?;
for filter in description.self_filters().iter().flatten() {
    if let Some(id) = filter.expression_id() {
        report.record_producer(id, Arc::clone(filter));
    }
}
```

On the upward pass, record filters retained by the current node:

```rust
let mut result = node.handle_child_pushdown_result(phase, child_result, config)?;
for retained in result.retained_here.drain(..) {
    if let Some(id) = retained.expression_id() {
        report.record_consumer(id, retained);
    }
}
```

This requires adding `retained_here` to the pushdown result. The existing `PushedDown::Yes/No`
cannot provide this information: Parquet may retain a filter for statistics pruning while returning
`No` because it cannot fully evaluate it.

**Session state plumbing.** `DefaultPhysicalPlanner::optimize_physical_plan` has a
`&SessionState`, but `PhysicalOptimizerRule::optimize` receives only `&ConfigOptions`:

```rust
new_plan = rule.optimize(new_plan, session_state.config_options())?;
```

Therefore, the filter-pushdown rule cannot access or modify `SessionState` today. The clean API is
for filter pushdown to return the report to the physical-planning layer:

```rust
fn optimize_with_report(...)
    -> Result<(Arc<dyn ExecutionPlan>, FilterPushdownReport)>;
```

Datafusion-distributed can then carry the report in its query-planning context alongside the
optimized plan; it does not need to put it in `SessionState`.

If the report must be exposed through `SessionState`, create a query-owned
`Arc<Mutex<FilterPushdownReport>>` before optimization and add an optimizer context that passes the
handle to rules:

```rust
let mut query_state = session_context.state(); // fresh state for this query
let report = Arc::new(Mutex::new(FilterPushdownReport::default()));
query_state.config_mut().set_extension(Arc::clone(&report));
```

The new optimizer context would retrieve this `SessionConfig` extension and give the handle to the
rule. Installing it on the long-lived `SessionContext` would incorrectly share one report across
queries. This approach avoids plan-node downcasts and replaying optimizer callbacks, but requires a
small upstream optimizer API change and does not report filters created outside the pushdown
protocol.

##### Registry + Downcasting Pattern

The idea is to have a registry of functions which can be used to extract `&DynamicFilterPhysicalExpr` from `ExecutionPlan` nodes. datafusion-distributed
would have a registry with implementations for `HashJoinExec`, `SortExec`, `AggregateExec`, and `DataSourceExec`. Users would be able to add their own.

Example: implementation for HashJoinExec
```rust
fn try_collect(
  &self,
  node: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Vec<Arc<dyn PhysicalExpr>>>> {
  let Some(join) = node.downcast_ref::<HashJoinExec>() else {
      return Ok(None);
  };

  Ok(join
      .dynamic_filter_expr() // Note we need the dynamic filters to be public.
      .map(|expr| vec![Arc::clone(expr) as Arc<dyn PhysicalExpr>]))
}
```
Note: Public methods like `dynamic_filter_expr()` were only added to support serialization and will
likely be removed by https://github.com/apache/datafusion/issues/23494.
```rust
HashJoinExec::dynamic_filter_expr()
AggregateExec::dynamic_filter_expr()
SortExec::dynamic_filter_expr()
```

Example: implementation for ParquetSource
```rust
fn try_collect(
  &self,
  source: &Arc<dyn FileSource>,
) -> Result<Option<Vec<Arc<dyn PhysicalExpr>>>> {
    let Some(parquet_source) = source.downcast_ref::<ParquetSource>() else {
        return Ok(None);
    };

    let Some(predicate) = parquet_source.filter() else {
        return Ok(None);
    };
    if snapshot_generation(&predicate) == 0 {
        return Ok(None);
    }
    ... get the dynamic filter from the predicate 
}
```

Pros:
- Requires no changes to vanilla datafusion. We can ship this faster.

Cons:
- Dynamic filtering will not work for users with custom plan nodes unless users register them
- Distributed datafusion has to maintain a registry of all `ExecutionPlan` implementations in vanilla datafusion. This will need to be updated
  during upgrades.
- Internal dynamic filter fields to be `pub`
- Looping over every `try_collect` is costly



#### Proposal

### 2. Merging Dynamic Filters

The highlevel idea is to merge dynamic filters using OR expressions

It would be nice to see first class support or this behavior. It would also be nice if dynamic filters stored their `Partitioning`.

Today dynamic filters store one expression which is updated atomically:
```rust
struct Inner {
    expression_id: u64,.
    generation: u64,
    // The actual dynamic filter expression
    expr: Arc<dyn PhysicalExpr>,
    is_complete: bool,
}

impl DynamicFilterPhysicalExpr {
    // Atomically update the expression
    pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()>;
    // Atomically get the current expression
    pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>>;
}
```

It would be interesting to make them more partition-aware by:
- store one expression per partition when the dynamic filter is "partition aware", otherwise just store one
- change `update()` to update the expression for a specific partition
- update `current()` to return a generated `CASE` expression
- implement a `union()` operation
```rust
struct Inner {
    expression_id: u64,
    generation: u64,
    // Instead of one expr, we may have one expr per partition
    expr: LiveFilterExpr,
    lowered_expr: Arc<dyn PhysicalExpr>,
    is_complete: bool,
}

// The actual dynamic filter expression
enum LiveFilterExpr {
    Global(Arc<dyn PhysicalExpr>),
    Partitioned(PartitionedFilterExpr),
}

impl LiveFilterExpr {
    // Generates a new expression by ORing.
    // For LiveFilterExpr::Global, we can OR the cases together.
    // For LiveFilterExpr::Partitioned, we can modify the cases as needed (ex. we do a CASE-wise OR for hash partitioned filters).
    // We may also alter the behavior depending on the partitioning.
    pub fn union(&self, other: LiveFilterExpr) -> Result<()>;
}

struct PartitionedFilterExpr {
    partitioning: Partitioning, // ex. Partitioning=Hash(column_a, 12)
    partition_expr: Arc<dyn PhysicalExpr>, // ex. hash(column_a)
    cases: BTreeMap<u64, Arc<dyn PhysicalExpr>>, // map of partition id to filter expr 
}

impl DynamicFilterPhysicalExpr {
    // Update takes an optional partition id.
    // Error if called with a partition id when the dynamic filter is not partition-aware and vice versa.
    pub fn update(&self, expr: Arc<dyn PhysicalExpr>, partition_id: Option<u64>) -> Result<()>;

    // Method is the same but now generates CASE expressions
    pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>>;
}
```

## Summary

Distributed dynamic filtering needs the following features from vanilla datafusion. Each of these has a "hacky way" and a "nice way"
of being implemented. Ideally, other distributed projects like comet and ballista can benefit from these features.

1. ability to get `&DynamicFilterPhysicalExpr` from `ExecutionPlan` 

Hacky: add a registry of known `&DynamicFilterPhysicalExpr` producers and consumers. Use downcasting to get `&DynamicFilterPhysicalExpr` from each node 
  (note that even in the registry pattern, the `&DynamicFilterPhysicalExpr` must be publically accessible)
Nice: add an explicit method `ExecutionPlan::dynamic_filter_exprs` method

2. ability to merge dynamic filters

Hacky: Use ORs to merge filters or combine CASE expressions by modifying `PhysicalExpr`s
Nice: add `Partitioning` into `&DynamicFilterPhysicalExpr` with an explicit `union()` API
