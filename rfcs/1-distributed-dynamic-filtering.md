# Distributed Dynamic Filtering

## Contents

1. Background
2. Dynamic Filtering in Distributed DataFusion - Proposed Highlevel Design
3. Gaps to Address in Vanilla DataFusion - What's blocking us?

## Background

Dynamic filters are resilient to partitioning. They come in two flavors
- "global" dynamic filters
- "partition-aware" dynamic filters

"partition-aware" dynamic filters are an optimization which makes dynamic filters
more granular. The only real difference is that the "partition-aware" dynamic filters
contain `CASE` expressions that capture the partitioning.

### "Global" dynamic filter
Used by 
- `HashJoinExec`
- `AggregateExec`
- `SortExec`

```
                                                                               DynamicFilterPhysicalExpr:                
                        ┌────────────────────────┐                                                                       
                        │     HashJoinExec:      │                a@0 >= v0 AND a@0 <= v1 AND a@0 IN (SET) ([v2, v3, v4  
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
Used by `HashJoinExec: mode=Partitioned` only

```
                                                                           DynamicFilterPhysicalExpr:               
                        ┌────────────────────────┐                                                                  
                        │     HashJoinExec:      │              CASE Hash(a@0) % 12 = 0: a@0 >= v0 AND a@0 <= v1    
                        │mode=Partitioned on=a@0 │          CASE Hash(a@0) % 12 = 0: a@0 IN (SET) ([v2, v3, v4 ...])
                        └────────────────────────┘                                    ....                          
                               ▲       ▲                                     (1 case per partition)                 
                               │       │                                                                            
              ┌────────────────┘       └────────────────┐                               │                           
              │                                         │                   Pushed down to data source              
┌──────────────────────────┐              ┌──────────────────────────┐                                              
│     DataSourceExec:      │              │     RepartitionExec:     │                  │                           
│Partitioning=Hash(a, 12)  │              │ Partitioning=Hash(a, 12) │                  │                           
└──────────────────────────┘              └──────────────────────────┘                  │                           
                                                        ▲                               │                           
                                                        │                               ▼                           
                                          ┌──────────────────────────┐                                              
                                          │     DataSourceExec:      │  Each partition uses the same expression, except     
                                          │ Partitioning=Unknown(10) │  each partition has its own case                                              
                                          └──────────────────────────┘                                             
```

## Distributed Dyanmic Filters

Distributed Dyanmic Filtering Falls into 2 cases
- "local case" - when the producer `ExecutionPlan` node is on the same machine as the consumer/DataSourceExec
- "remote case" - when the producer `ExecutionPlan` node is on a different machine as the consumer/DataSourceExec

### Local Case
Below, you can see a simplified disrtibuted datafusion plan. Dynamic filtering should "just work" today already due to prior work such as https://github.com/apache/datafusion/issues/20418.

In datafusion-distributed, the coordinator serializes parts of the plan and sends them to the workers. We expect each task's copy of the plan to behave like a single node plan with
dynamic filters working; there are tests in vanilla datafusion that validate this behavior.

Note that each `HashJoinExec` produces its own dynamic filters, meaning there are 3 in total.

#### Global Dynamic Filters
Each task has its own dynamic filters which do not have `CASE` expressions. They may look like this:

Task 1: `DynamicFilterPhysicalExpr: a@0 >= v0 AND a@0 <= v1`
Task 2: `DynamicFilterPhysicalExpr: a@0 >= v2`
Task 3: `DynamicFilterPhysicalExpr: a@0 IN LIST [v3, v4 ...]`

#### Partition-Aware Dynamic Filters
Each task has its own dynamic filters which contain `CASE` expressions. they may look like this:

Task 1:
```
CASE Hash(a@0) % 12 = 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 12 = 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
```
Task 3:
```
CASE Hash(a@0) % 12 = 0: a@0 >= v6
... 4 cases in total
```
Note that in each task / worker, partitions are indexed from 0, meaning the cases are all from `Hash(a@0) % 12 = 0` to `Hash(a@0) % 12 = 3`.

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
Pros:
It's simple.

Cons: 
- At first you may think the resulting filter will have higher selectivity. It does relative to the local case plan above. However, compared to single-node execution,
the selectivity is about the same. In single node execution, the dynamic filter would include rows from all 12 partitions of the build side. ORing the filters
combines filters for 4 partitions * 3 tasks into one filter for all 12 partitions.
- It's a bit of a smell to start messing with `PhysicalExprs`. It's not horrible though. We take the expression from each task, wrapping them in OR `BinaryExpr`, and
we're done.

#### Partition-Aware Filters

Consider if these "partition-aware" dynamic filters generated in stage 2

Task 1:
```
CASE Hash(a@0) % 12 = 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 12 = 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
```
Task 3:
```
CASE Hash(a@0) % 12 = 0: a@0 >= v6
... 4 cases in total
```

##### Idea: Combine the Cases Together
```
CASE Hash(a@0) % 12 = 0: a@0 >= v0 AND a@0 <= v1
...
CASE Hash(a@0) % 12 = 4: a@0 IN LIST [v2, v3, v4 ...] 
...
CASE Hash(a@0) % 12 = 8: a@0 >= v6
...
```

Now we end up with 12 cases and one `PhysicalExpr` which can be pushed down to each task for stage 1. Note that we have to
offset the cases using the task number. For example, `Hash(a@0) % 12 = 0` in Task 3 becomes `Hash(a@0) % 12 = 8`.

Cons:
- Bit of a smell.

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
            │  │     DataSourceExec:      │  │    NetworkShuffleExec    │  │    │  │     DataSourceExec:      │  │    NetworkShuffleExec    │  │    │  │     DataSourceExec:      │  │    NetworkShuffleExec    │ │
            │  │ Partitioning=Hash(a, 12) │  │                          │  │    │  │ Partitioning=Hash(a, 12) │  │                          │  │    │  │ Partitioning=Hash(a, 12) │  │                          │ │
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
                                                                            │ │ Partitioning=Unknown(10) │ │     │ │ Partitioning=Unknown(10) │ │                                                                  
                                                                            │ └──────────────────────────┘ │     │ └──────────────────────────┘ │                                                                  
                                                                            └──────────────────────────────┘     └──────────────────────────────┘                                                                  
                                                                                                                                                                                                                   
                                                                              Task 1 Runs partitions [0, 6)        Task 2 Runs partitions [6, 12)                                                                  
                                                                                                                                                                                                                  
```


## Bookkeeping - Implementation Details

### Extracting Dynamic Filters

The design fundamentally relies on distributed datafusion intercepting and modifying dynamic filter updates. `DynamicFilterPhysicalExpr` already offers APIs
for this:
```
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

`expression_id` may useful to correlate dynamic filters across machines.
```
fn expression_id(&self) -> Option<u64> {
```

### Displaying Dyanamic Filters

Today, distributed query plans will always show `predicate=DynamicFilter [ empty ]`. In addition to making
dynamic filter pruning work during execution, we need to make sure to propagate final dynamic filters from data sources
to the coordinator so they can be displayed.



## [WIP DRAFT] Gaps in Vanilla DataFusion

### Getting `&DynamicFilterPhysicalExpr` from `ExecutionPlan`

#### `apply_expressions`

See https://github.com/apache/datafusion/pull/22445

```rust
fn apply_expressions(
    &self,
    f: &mut dyn FnMut(
        &dyn datafusion::physical_plan::PhysicalExpr,
    ) -> Result<TreeNodeRecursion>,
) -> Result<TreeNodeRecursion>
```
This API was added to allow for iteration over all expressions in `ExecutionPlan` nodes but was reverted because
- it was complicated to implement and a required method on all `ExecutionPlan` nodes
- there was no usage inside of vanilla datafusion, meaning it may grow stale

#### `handle_child_pushdown_result`
```
fn handle_child_pushdown_result(
    &self,
    _phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult,
    _config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
}
```

#### `gather_filters_for_pushdown`

```
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


#### Registry + Downcasting Pattern

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
out of the box.
- Distributed datafusion has to maintain a registry of all `ExecutionPlan` implementations in vanilla datafusion
- We rely a lot on downcasting `ExecutionPlan` nodes

### Some Dynamic Filters Do Not Exist During Planning 


### Identifying Producers vs Consumers

When collecting dynamic filters from a plan, we need to know if a node is producing or consuming dynamic filters to know
in which direction to route updates.

### Merging Dynamic Filters

There's no first class support for ORing dynamic filters or combining `CASE` expressions. We would have to assume
that a toplevel `CASE` expression is used for partition ids. This is a bit of a smell. If dynamic filters were to
use `CASE` expressions as a part of the filter expression, then this could be a correctness issue.

- Should we make dynamic filters partitioning aware?
- Should we implement a `merge` method?

