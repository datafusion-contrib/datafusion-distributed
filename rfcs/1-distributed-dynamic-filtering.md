# Distributed Dynamic Filtering

## Contents

1. Background
2. Dynamic Filtering in Distributed DataFusion - Proposed Highlevel Design
3. Gaps to Address in Vanilla DataFusion - What's blocking us?

## Background

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
more granular and more selective. 

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
                                                        ▲                               │                           
                                                        │                               ▼                           
                                          ┌──────────────────────────┐                                              
                                          │     DataSourceExec:      │  Each partition uses the same DynamicFilterPhysicalExpr, except     
                                          │ Partitioning=Unknown(10) │  each row will only hash to use one case                                              
                                          └──────────────────────────┘                                             
```



## Distributed Dyanmic Filters

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
CASE Hash(a@0) % 12 
WHEN 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 12
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
- At first you may think the resulting filter will have higher selectivity. It does relative to the local case plan above. However, compared to single-node execution,
the selectivity should be about the same. In single node execution, the dynamic filter accounts for all rows matching the build side. ORing the filters
combines filters for 4 partitions * 3 tasks should provide similar selectivity.
  - Note the ORed filter would not be identical to the filter generated during single node execution.
- It's a bit of a smell to start modifying the `PhysicalExpr` inside the dynamic filter. It's not horrible though. We take the expression from each task and
wrap them in OR `BinaryExpr`.
- ORing the filters increases the size of the filter, meaning there's more overhead from serde, parsing, filter evaluation, and network transfer.
  - Unsure how significant this would be in practice.

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
CASE Hash(a@0) % 12
WHEN 0: a@0 >= v0 AND a@0 <= v1
... 4 cases in total
```
Task 2:
```
CASE Hash(a@0) % 12
WHEN 0: a@0 IN LIST [v2, v3, v4 ...] 
... 4 cases in total
```
Task 3:
```
CASE Hash(a@0) % 12
WHEN 0: a@0 >= v6
... 4 cases in total
```

##### Idea: Combine the Cases Together
```
CASE Hash(a@0) % 12
WHEN 0: a@0 >= v0 AND a@0 <= v1
...
WHEN 4: a@0 IN LIST [v2, v3, v4 ...] 
...
WHEN 8: a@0 >= v6
...
... 3 * 4 = 12 cases in total 
```

Now we end up with 12 cases and one `PhysicalExpr` which can be pushed down to each task for stage 1. Note that we have to
offset the cases using the task number. For example, `Hash(a@0) % 12 = 0` in Task 3 becomes `Hash(a@0) % 12 = 8`.

Cons:
- Bit of a smell to go into the internals of the `PhysicalExpr` and merge cases.
- It's actually difficult to differentiate between `CASE` expressions used to represent partitions and `CASE` expressions which
  are actually a part of the filter itself. In other words, if I see `CASE`, is this a partition-routed filter or a global filter
  that uses a `CASE` expression?
  - It happens that hash joins only use `CASE` expressions for partitions and never for probe side pruning. However, this may
    happen in the future so we may want better interfaces and gurantees here. For example, should `DynamicFilterPhysicalExpr`
    actually store something about the `Partitioning` rather than baking the partitioning into a `CASE` expression?
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


## Implementation Details

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

One open question we discuss below is - how do you know if a node is a producer or consumer of dynamic filters?

`expression_id` may useful to correlate dynamic filters across machines.
```
fn expression_id(&self) -> Option<u64> {
```


### Displaying Dyanamic Filters

Today, distributed query plans will always show `predicate=DynamicFilter [ empty ]`. In addition to making
dynamic filter pruning work during execution, after execution we must propagate final dynamic
filters from data sources to the coordinator so they can be displayed.


## [WIP DRAFT] Gaps in Vanilla DataFusion

### Getting `&DynamicFilterPhysicalExpr` from `ExecutionPlan`

We need to:
(a) extract `&DynamicFilterPhysicalExpr` from producers like `HashJoinExec`, `SortExec`, `AggregateExec` and consumers like `DataSourceExec`
(b) identify which dynamic filters are producers vs consumers
(c) allow easy extension for custom `ExecutionPlan` implementations

#### Prior Art

1. `ExecutionPlan::apply_expressions`

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

2. `ExecutionPlan::handle_child_pushdown_result` / `ExecutionPlan::gather_filters_for_pushdown`
```
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

1. Registry + Downcasting Pattern

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

2. New Non-Required method `ExecutionPlan::dynamic_filter_exprs` 

```
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
```
HashJoinExec::dynamic_filter_expr()
AggregateExec::dynamic_filter_expr()
SortExec::dynamic_filter_expr()
```

Pros:
- Does exactly what we want

Cons:
- Very specific. We usually treat dynamic filters as any other filter expression. However, this method cuts through the abstraction
  and brings them to the surface.

### Merging Dynamic Filters

The highlevel idea is to 
- merge "global" dynamic filters using OR expressions
- merge "partition-routed" dynamic filters by merging their `CASE` expressions
Both of these require mutating the dynamic filter `PhysicalExpr`

It would be interesting to see first class support fo this behavior.
- Should we make dynamic filters partitioning aware?
- Should we implement a `merge` method?

Today dynamic filters store one expression which is updated atomically
```rust
struct Inner {
    expression_id: u64,
    generation: u64,
    // The actual dynamic filter expression
    expr: Arc<dyn PhysicalExpr>,
    is_complete: bool,
}

impl DynamicFilterPhysicalExpr {
    pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()>
}
```

It would be interesting to make them more partition-aware by
- store one expression per partition when the dynamic filter is "partition aware"
- change `update()` to update the expression for a specific partition
- update `current()` to return a generated `CASE` expression
- implement a `merge()` operation which only merges compatible dynamic filters
  (global with global or partition-aware with partition-aware)
```rust
struct Inner {
    expression_id: u64,
    generation: u64,
    state: LiveFilterExpr,
    lowered_expr: Arc<dyn PhysicalExpr>,
    is_complete: bool,
}

enum LiveFilterExpr {
    Global(Arc<dyn PhysicalExpr>),
    Partitioned(PartitionedFilterExpr),
}

struct PartitionedFilterExpr {
    partitioning: Partitioning, // ex. Partitioning=Hash(column_a, 12)
    partition_expr: Arc<dyn PhysicalExpr>, // ex. hash(column_a)
    cases: BTreeMap<u64, Arc<dyn PhysicalExpr>>, // map of partition id to filter expr 
}


impl DynamicFilterPhysicalExpr {
    pub fn update_global(&self, expr: Arc<dyn PhysicalExpr>) -> Result<()>;

    pub fn update_partition(
        &self,
        partition_id: u64,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<()>;

    pub fn merge(&self, other: LiveFilterExpr) -> Result<()>;

    pub fn align_partition_cases(&self, offset: u64) -> Result<()>;
}
```

## Summary

Distributed dynamic filtering needs the following features from vanilla datafusion. Each of these has a "hacky way" and a "nice way"
of being implemented.
1. ability to get `&DynamicFilterPhysicalExpr` from `ExecutionPlan` 

Hacky: add a registry of known `&DynamicFilterPhysicalExpr` producers and consumers. Use downcasting to get `&DynamicFilterPhysicalExpr` from each node 
  (note that even in the registry pattern, the &DynamicFilterPhysicalExpr must be publically accessible)
Nice: add an explicit method `ExecutionPlan::dynamic_filter_exprs` method

2. ability to merge dynamic filters

Hacky: OR global filters and combine CASE expressions
Nice: add `Partitioning` into `&DynamicFilterPhysicalExpr` and implement `&DynamicFilterPhysicalExpr::merge` to generate CASE or OR expressions 



