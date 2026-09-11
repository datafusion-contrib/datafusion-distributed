mod discovery;
mod display;

use crate::codec::roundtrip_pb;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub(crate) use discovery::*;
pub use display::rewrite_distributed_plan_with_dynamic_filters;
pub(crate) use display::sever_dynamic_filter_relationships_in_plan_for_display;

/// Removes all shared, in-memory dynamic filter state from this plan when it contains
/// any dynamic filter producers or consumers. Plan nodes *within* this plan will share
/// in-memory dynamic filter state. However, they will not share state with plan nodes
/// outside of this plan, such as in parent stages.
///
/// Since it's possible for a user to implement a custom transport layer generally
/// skipping proto serialization, we assume that any two tasks can be collocated
/// and share memory. Aside from this, the only reason any two tasks can be collocated
/// and share memory is if they are both on the coordinator and the coordinator sends
/// the plan via an in-memory channel.
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
