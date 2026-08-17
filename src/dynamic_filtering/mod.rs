mod discovery;
mod display;

use crate::codec::roundtrip_pb;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub(crate) use discovery::*;
pub use display::rewrite_distributed_plan_with_dynamic_filters;
pub(crate) use display::sever_dynamic_filter_relationships_in_plan_for_display;

pub(crate) fn is_dynamic_filtering_enabled(session_config: &SessionConfig) -> bool {
    session_config
        .options()
        .optimizer
        .enable_dynamic_filter_pushdown
}

/// Deepcopies the plan if it contains any dynamic filter producers or consumers. This isolates
/// any dynamic filters in this plan from dynamic filters *outside* the plan. Plan nodes
/// *within* this plan will share in-memory dynamic filter state with eachother, even after copying.
///
/// # Why Task-Local Dynamic Filters are Safe
///
/// ## TopK Dynamic Filters
///
///     Stage 2 Tasks: M
///     └── SortPreservingMergeExec: fetch=10 (global TopK)
///         └── NetworkCoalesceExec
///             
///     Stage 1 Tasks: N
///     └── SortExec: fetch=10 (local TopK and dynamic-filter producer)
///         └── DataSourceExec: dynamic-filter consumer
///
/// Each `SortExec` may push its task-local TopK bound into its own input. It still emits the local
/// TopK candidates, which the `SortPreservingMergeExec` reduces to the global TopK in the parent
/// stage.
///
/// ## Min/Max Dynamic Filters in Partial Aggregates with No Group
///
///     Stage 2 Tasks: M
///     └── AggregateExec: mode=FinalPartitioned, gby=[], aggr=[max(foo)] (global max)
///         └── NetworkShuffleExec
///             
///     Stage 1 Tasks: N
///     └── RepartitionExec
///         └── AggregateExec: mode=Partial, gby=[], aggr=[max(foo)] (local max and dynamic filter producer)
///             └── DataSourceExec: dynamic-filter consumer
///
/// The partial aggregate may push its task-local min/max bound into its own input. It still emits
/// the local min/max, which the final aggregate reduces to the global min/max in the parent stage.
///
/// ## CollectLeft Joins
///
///     Stage Y Tasks: M
///
///     HashJoinExec: mode=CollectLeft
///     ├── build: CoalescePartitionsExec
///     │   └── all build partitions (the complete build side)
///     └── probe:
///         └── DataSourceExec: dynamic-filter consumer
///
/// A `CollectLeft` hash join collects the complete build side in every task (by broadcasting or
/// otherwise) and pushes it down to every probe partition. Since every probe partition sees
/// the entire build-side filter, it will only filter rows which the join would filter out.
///
/// ## Partitioned Joins
///
/// A partitioned hash join builds per-partition predicates from the task-local hash table.
///
///     Stage Y Task i
///
///     HashJoinExec: mode=Partitioned
///     ├── build partition i  → producer predicate P(i)
///     └── probe partition i  → consumer of P(i)
///
/// The build and probe execute corresponding partitions in the same task, so the probe
/// gets the correct/complete filter from it's corresponding build side.
///
/// If the probe's partitioning is different than the join, then there must be a
/// `RepartitionExec` on the probe side. This will either become a network shuffle, falling
/// outside the local case, or it's a local repartition, meaning the join is not distributed
/// and contains all of the partitions and the entire build side.
///
/// In all cases, a probe-side partition sees the correct filter.
///
/// # Cases this Function Avoids
///
/// Outside of the cases above, we assume that dynamic filter propagation between two tasks requires
/// remote communication and coordination through the coordinator stage. However, there's
/// some edge cases that break this assumption.
///
/// It's possible for any two tasks to be collocated and share memory because
/// - a user to implement a custom transport layer and skip all proto serialization
/// - the coordinator may send plans to it's local worker via an in-memory channel without serializing
///
/// The examples below show how this may cause incorrect results.
///
/// ## Example 1: Producer-Consumer
///
/// Consider this partitioned hash join topology where the consumer task is
/// collocated with one producer on worker A:
/// ```text
/// Worker A
///
/// Stage 2 Task 0
/// HashJoinExec <- Dynamic Filter Produced: (foo > 100)
///
/// Stage 1 Task 0
/// DataSourceExec <- consumer
///
/// Worker B
/// Stage 2 Task 1
/// HashJoinExec <- Dynamic Filter Produced: (foo != 150)
/// ```
///
/// The in-process transport allows the Worker A join to propagate its filter to
/// the consumer and mark it as completed, so the consumer incorrectly applies
/// (foo > 100) instead of (foo > 100 OR foo != 150).
///
/// ## Example 2: Producer-Producer
///
/// ```text
/// Worker A
///
/// Stage 2 Task 0
/// HashJoinExec <- Dynamic Filter Produced: (foo > 100)
///
/// Stage 2 Task 1
/// HashJoinExec <- Dynamic Filter Produced: (foo != 150)
///
/// Stage 1 Task 0
/// DataSourceExec <- consumer
/// ```
///
/// Both producers are collocated on worker A. In this situation, they both race to
/// update the dynamic filter, meaning the final expression will either be foo > 100
/// or foo != 150. The correct expression is (foo > 100 OR foo != 150).
///
/// Example 3: Local Producer-Consumer
///
/// ```text
/// Worker A
///
/// Stage 2 Task 0
/// HashJoinExec <- Dynamic Filter Produced: (foo > 100)
///   DataSourceExec <- consumer
///
/// Stage 2 Task 1
/// HashJoinExec <- Dynamic Filter Produced: (foo != 150)
///   DataSourceExec <- consumer
/// ```
///
/// Since both producers and both consumers are located on the same worker, they all share
/// one in-memory dynamic filter. This ends up being a race between two writers and two readers.
pub(crate) fn maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let has_producers = !discover_dynamic_filter_producers(&plan)?.is_empty();
    let has_consumers = !discover_dynamic_filter_consumers(&plan)?
        .consumers
        .is_empty();

    if has_producers || has_consumers {
        roundtrip_pb(plan, task_ctx)
    } else {
        Ok(plan)
    }
}
