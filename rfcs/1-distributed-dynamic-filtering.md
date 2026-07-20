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
│Partitioning=Hash(a, 12)  │              │ Partitioning=Hash(a, 12) │                    │                              
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
                        │mode=Partitioned on=a@0 │                       WHEN 0: a@0 >= v0 AND a@0 <= v1
                        └────────────────────────┘                       WHEN 1: a@0 IN (SET) ([v2, v3, v4 ...])
                               ▲       ▲                                     ... (12 cases in total)                 
                               │       │                                                                            
              ┌────────────────┘       └────────────────┐                               │                           
              │                                         │                   Pushed down to data source              
┌──────────────────────────┐              ┌──────────────────────────┐                                              
│     DataSourceExec:      │              │     RepartitionExec:     │                  │                           
│Partitioning=Hash(a, 12)  │              │ Partitioning=Hash(a, 12) │                  │                           
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
                                          │ Partitioning=Unknown(10) │  each row will only hash to one case                                              
                                          └──────────────────────────┘                                             
```

## 2. Background - Dynamic Filtering in Trino and Spark 

This summary focuses on replicated / collect left / broadcast hash joins and partitioned/shuffled
hash joins.

Both Trino and Spark ultimately apply a **global filter for each logical join**. Neither sends
a different filter to probe rows based on the remote shuffle partition that will process them.

### Trino

Trino uses "Domains" to represent a Set of Rows (ie. rows that would pass a dynamic filter)

1. CollectLeft

The build side of the join is identical across each worker, meaning each has the same Domain. The Domain D is sent to the coordinator
which forwards it to all the data sources.

2. Partitioned:

Each partition in each worker has a unique Domain. Domains are unioned across partitions and across workers. Example:
```
  Worker 0:
    D0 = {1, 4}
    D1 = {7}
    W0   = {1, 4, 7}

  Worker 1:
    D0 = {12}
    D1 = {20, 25}
    W1   = {12, 20, 25}

  Global:
    G = W0 ∪ W1
      = {1, 4, 7, 12, 20, 25}
```
The coordinator sends the global filter to all data sources.

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
Below, you can see a simplified disrtibuted datafusion plan. Dynamic filtering should "just work" today in the local case already due
to prior serialization-related work in vanilla datafusion.

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
CASE Hash(a@0) % 12
WHEN 0: a@0 >= v6
... 4 cases in total
```
Note that in each task / worker, partitions are indexed from 0, meaning the CASE expressions in each task/worker all use
`Hash(a@0) % 12 = 0` to `Hash(a@0) % 12 = 3`.

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
│                                 │ Partitioning=Unknown(10) │   │  │                                 │ Partitioning=Unknown(10) │   │  │                                 │ Partitioning=Unknown(10) │   │
│                                 └──────────────────────────┘   │  │                                 └──────────────────────────┘   │  │                                 └──────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────────────┘  └────────────────────────────────────────────────────────────────┘
                  Task 1 Runs partitions [0,4)                                         Task 2 Runs partitions [4,8)                                        Task 3 Runs partitions [8,12)                  
```

### Remote Case

Now things get pretty complicated. 

#### Global Filters

Consider if these "global" dynamic filters generated in stage 2

Task 1: `DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1`
Task 2: `DynamicFilterPhysicalExpr: a@0 >= v2`
Task 3: `DynamicFilterPhysicalExpr: a@0 IN LIST [v3, v4 ...]`

What is the correct filter to push to Stage 1 Task 1 and Stage 1 Task 2? Any row from Stage 1 Task 1 may appear in any task of Stage 2. 

##### Idea: OR the Filters Together

`DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1 OR a@0 >= v2 OR a@0 IN LIST [v3, v4 ...]`

Pros:
It's simple.

Cons: 
- Less efficient pruning (note: not worse than single node execution, which would have had 1 dynamic filter anyways rather than 3)
- It's a bit of a smell to start modifying the `PhysicalExpr` inside the dynamic filter. It's not horrible though. We take the expression
  from each task and wrap them in OR `BinaryExpr`.
- ORing the filters increases the size of the filter, meaning there's more overhead from serde, parsing, filter evaluation, and network transfer.

##### Idea: Can we preserve selectivity?

For example, this would give us very selective pruning:
```
CASE Hash(a@12) % 12:
  WHEN 0: a@0 >= v0 AND a@0 <= v1
  WHEN 1: a@0 >= v0 AND a@0 <= v1
  WHEN 2: a@0 >= v0 AND a@0 <= v1
  WHEN 3: a@0 >= v0 AND a@0 <= v1
  
  WHEN 0: a@0 >= v2
  WHEN 1: a@0 >= v2
  WHEN 2: a@0 >= v2
  WHEN 3: a@0 >= v2
  
  WHEN 0: a@0 IN LIST [v3, v4 ...]
  WHEN 1: a@0 IN LIST [v3, v4 ...]
  WHEN 2: a@0 IN LIST [v3, v4 ...]
  WHEN 3: a@0 IN LIST [v3, v4 ...]
END
```

The coordinator knows enough information to generate this case statement. It is aware of the `RepartitionExec Hash(x, 12)` and we can use the invariant
that the `RepartitionExec` always has `num_partitions_per_worker` * `num_workers` partitions. It would be very nice if the partitioning of the `HashJoinExec`
itself was `Hash(a, 12)` and not `Hash(a, 4)`:
1. `Hash(a, 4)` is technically wrong. We repartitioned into 12, not 4 partitions. 
2. This allows the coordinator to avoid searching for the `RepartitionExec` below to determine the partitioning of the `HashJoinExec`. It can just inspect the
   dynamic filter producer, the `HashJoinExec`, directly.

Pros:
- More selective than the above

##### Alternative Idea: Combine the Range and IN LIST expressions

Similar to the above except you try to avoid ORing.

We could try converting these filters from this 
`DynamicFilterPhysicalExpr: a@0 >= 0 AND a@0 <= 5 OR IN LIST [10, 11]`
`DynamicFilterPhysicalExpr: a@0 >= 1 AND a@0 <= 10 OR IN LIST [12, 13]`
To this
`DynamicFilterPhysicalExpr: a@0 >= 0 AND a@0 <= 10 OR IN LIST [10, 11, 12, 13]`

Pros:
- Less overhead than ORing

Cons:
- Is brittle. What if we have to support non-range and non-IN-LIST expressions?

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

(Note: This is the same as having 1 case expression and ORing the cases together. ex. `WHEN 0: ... OR ... OR ..., WHEN 1: ... OR ... OR ...`)

Note that the cases are all `% 4` and range from `0` to `4` (as opposed to `% 12` ranging from `0` to `12`). Firstly, distributed datafusion happens to
set the `partitioning` of the `HashJoinExec` to `Hash(..., 4)`, not `Hash(..., 12)`. That's why each hash join produces dynamic filters with
`%4` and cases that range from `0` to `4`.

This happens to work due to the property that `(x % M) % N = x % N` when `M` is a multiple of `N`. For example, ORing all the cases
where `Hash(a) % 4 = 1` correctly covers the cases where `Hash(a) % 12 in (1, 5, 9)`. Even if we have several layers of `NetworkShuffleExec`,
the property holds `((x % M1) % M2) % N = x % N` so long as `Mi` `N`.


Pros:
- Simple

Cons:
- It's a bit brittle. When will this stop working?
  - If the `RepartitionExec` below a `NetworkShuffleExec` produces a partition count that is not a multiple of `target_partitions`.
  [Here](https://github.com/datafusion-contrib/datafusion-distributed/blob/a6c326807fa3a5ff05b4b7e08a1bd1e3cd7bfe53/src/execution_plans/network_shuffle.rs?plain=1#L160) is where we scale up the `RepartitionExec` today
  - If there are repartitions which do not repartition by `target_partitions`. Unsure if these happen in vanilla datafusion.
- Are there other options?
- If anything, we should `HashJoinExec` reflects the correct partitioning, `Hash(..., 12)` rather than `Hash(..., 4)`. To be honest,
  claiming that the `HashJoinExec` partitioning is `Hash(4)` is incorrect. Finding some way around this will likely (a) be difficult because you will have
  to track all the repartitions between the `DataSourceExec` and the `HashJoinExec`; or (b) a smell. 


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

We need to:
(a) extract `&DynamicFilterPhysicalExpr` from producers like `HashJoinExec`, `SortExec`, `AggregateExec` and consumers like `DataSourceExec`
(b) identify which dynamic filters are producers vs consumers
(c) allow easy extension for custom `ExecutionPlan` implementations

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
  and use the concrete type to know if it is a producer or consumer
  - This is potentially brittle because producers / consumers might change. What if a `HashJoinExec`
    becomes a consumer one day?
  - What about custom `ExecutionPlan` implementations? We would need some type of registry of known
    producers and consumers so users can register their own implementations
- Was reverted because
  - It was complicated to implement and a required method on all `ExecutionPlan` nodes
  - There was no usage inside of vanilla datafusion, meaning it may grow stale

##### `ExecutionPlan::handle_child_pushdown_result` / `ExecutionPlan::gather_filters_for_pushdown`
```rust
fn handle_child_pushdown_result(
    &self,
    _phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult,
    _config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
}

fn gather_filters_for_pushdown(
    &self,
    _phase: FilterPushdownPhase,
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    _config: &ConfigOptions,
) -> Result<FilterDescription> {
    Ok(FilterDescription::all_unsupported(
        &parent_filters,
        &self.children(),
    ))
}
```

There's a few reasons not to use these to collect dynamic filters:
- They describe how to perform filter pushdown, they do not really answer "what dynamic filters and producer-consumer relations exist in the optimized plan?". We would have
  to modify these methods heavily.
- The required inputs, such as `ChildPushdownResult`, `parent_filters`, and `FilterPushdownPhase`, only make sense inside the original pushdown traversal. Not during
  distributed planning.
- Assorted edge cases:
  - For a `HashJoinExec` that has already pushed down its filter `gather_filters_for_pushdown` will not return the existing dynamic filter again.
  - For a `HashJoinExec` that was not able to push down its filter, calling `gather_filters_for_pushdown` can create a fresh filter where no `DataSourceExec` uses it.
  - `handle_child_pushdown_result` may return an updated_node, and DataSourceExec may attach more predicates to its source.

#### Other Ideas

##### Registry + Downcasting Pattern

The idea is to have a registry of functions which can be used to extract `&DynamicFilterPhysicalExpr` from `ExecutionPlan` nodes.

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
      .dynamic_filter_expr() // note that this API may be removed
      .map(|expr| vec![Arc::clone(expr) as Arc<dyn PhysicalExpr>]))
}
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

    Ok(Some(vec![snapshot_consumer_expression(predicate)?]))
}
```

In datafusion distributed, it's important to be extensible. Rather than hardcoding every implementation, we should allow users to
specify how to get dynamic filters from their own plan nodes.

Pros:
- Requires no changes to vanilla datafusion. We can ship this faster.

Cons:
- Dynamic filtering will not work for users with custom plan nodes with extra steps. Distributed datafusion will not work 
out of the box. Users will have to register their custom `ExecutionPlan` implementations
- Distributed datafusion has to maintain a registry of all `ExecutionPlan` implementations in vanilla datafusion
- We rely a lot on downcasting `ExecutionPlan` nodes

##### New Non-Required method `ExecutionPlan::dynamic_filter_exprs` 

```rust
trait ExecutionPlan {
    fn dynamic_filter_expr(&self) -> Vec<DynamicFilterNodeBehavior(Arc<dyn PhysicalExpr>)> {
        vec![] 
    }
}

enum DynamicFilterNodeBehavior {
    Producer(Arc<dyn PhysicalExpr>),
    Consumer(Arc<dyn PhysicalExpr>),
}
```

Note that as of writing, we already have these methods, although they were only added to support serialization and will
likely be removed by https://github.com/apache/datafusion/issues/23494.
```rust
HashJoinExec::dynamic_filter_expr()
AggregateExec::dynamic_filter_expr()
SortExec::dynamic_filter_expr()
```

Pros:
- Does exactly what we want

Cons:
- Very specific. We usually treat dynamic filters as any other filter expression. However, this method cuts through the abstraction
  and brings them to the surface.

### 2. Merging Dynamic Filters

The highlevel idea is to 
- merge "global" dynamic filters using OR expressions
- merge "partition-routed" dynamic filters by merging their `CASE` expressions
Both of these require mutating the dynamic filter `PhysicalExpr`

It would be nice to see first class support or this behavior.
- Should we make dynamic filter to have 2 variants: "global" and "partition-aware"?
- Should we implement a `merge` method?

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
- implement a `merge()` operation which only merges compatible dynamic filters
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
    // Generates a new expression by either ORing or merging CASES. Errors if the
    // partitioning is not compatible or if the CASEs overlap.
    pub fn merge(&self, other: LiveFilterExpr) -> Result<()>;
}

struct PartitionedFilterExpr {
    partitioning: Partitioning, // ex. Partitioning=Hash(column_a, 12)
    partition_expr: Arc<dyn PhysicalExpr>, // ex. hash(column_a)
    cases: BTreeMap<u64, Arc<dyn PhysicalExpr>>, // map of partition id to filter expr 
}

impl PartitionedFilterExpr {
    // In distributed datfusion, we may have to merge a dynamic filter with partitions 0-4 with another
    // dynamic filter with partitions 0-4, however, we want them to be labelled 0-8. This method
    // can be used to align the partition ids.
    pub fn align_partition_cases(&self, offset: u64) -> Result<()>;
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
of being implemented.

1. ability to get `&DynamicFilterPhysicalExpr` from `ExecutionPlan` 

Hacky: add a registry of known `&DynamicFilterPhysicalExpr` producers and consumers. Use downcasting to get `&DynamicFilterPhysicalExpr` from each node 
  (note that even in the registry pattern, the `&DynamicFilterPhysicalExpr` must be publically accessible)
Nice: add an explicit method `ExecutionPlan::dynamic_filter_exprs` method

2. ability to merge dynamic filters

Hacky: Use ORs to merge filters or combine CASE expressions by mofidying `PhysicalExpr`s
Nice: add `Partitioning` into `&DynamicFilterPhysicalExpr` and make them more partition-aware 



TODO
### How Significantly Do `CASE` how ORing cases compares to CASE

https://github.com/apache/datafusion/pull/18451 - original PR. No benches.
https://github.com/apache/datafusion/issues/21207#issuecomment-5005667541

- does case iterate over all the partitions
yeah it does


