mod discovery;
mod display;

use crate::codec::roundtrip_pb;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub use discovery::*;
pub use display::rewrite_distributed_plan_with_dynamic_filters;
pub(crate) use display::sever_dynamic_filter_relationships_in_plan_for_display;

/// Isolates all shared, in-memory dynamic filter state from this plan if it contains
/// any dynamic filter producers or consumers.
///
/// Plan nodes *within* this plan will share in-memory dynamic filter state. However, they
/// will not share state with plan nodes outside of this plan, such as in parent stages.
///
/// # Correctness: Task-Local Dynamic Filters are Safe to Apply
///
/// Claim: It is correct for a producer to *always* update consumers within the same task.
///
/// Proof:
///
/// Invariant: If the task-local consumer filters out a row, removing that row must not change the output
/// of the task-local producer.
///
/// DataFusion-Distributed dynamic-filter producers satisfy this invariant:
///
/// 1. TopK dynamic filters are distributed into N TopK operations for N tasks followed by a global
///    TopK sort preserving TopK across those tasks. A task-local TopK dynamic filter does not
///    change any outputs in this scenario.
/// 2. A partial aggregate without grouping may push `MIN` and `MAX` bounds to consumers. In a task, a row
///    rejected by the aggregate's current bound will not change that aggregate's output. So, a local
///    min/max dynamic filter is safe to apply.
/// 3. A `CollectLeft` hash join has the complete build side in every task, so any task can apply
///    a predicate representing the build side.
/// 4. A partitioned hash join builds its predicate from the task-local hash table. There are three
///    cases for its build and probe side:
///    - Their partitioning matches. Thus, the build and probe execute the same partitions in the
///      same task, so the local dynamic filter filters rows corresponding to the local hash table.
///    - Their partitioning differs and the required repartition becomes a network shuffle. There is
///      local dynamic filter to push down anymore.
///    - Their partitioning differs but the repartition remains local. This is a single-node plan,
///      where the task executes all partitions and the producer's predicate covers the complete
///      build input.
///
/// This reasoning applies only within one task. A consumer in another task may need the union of
/// several task-local predicates and must receive that predicate through the coordinator instead
/// of sharing a producer's in-memory state.
///
/// # Cases this Function Avoids
///
/// It's possible for any two tasks to be collocated and share memory because
/// - a user to implement a custom transport layer and skip all proto serialization
/// - the coordinator may send plans to it's local worker via an in-memory channel without serializing
///
/// This causes weird scenarios that fall outside the well-defined cases above. This function is
/// responsible for isolating task plans to prevent these scenarios from occurring.
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
/// For a partitioned hash join, a producer may update its task-local consumer in memory, but
/// updates must not cross task boundaries.
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
