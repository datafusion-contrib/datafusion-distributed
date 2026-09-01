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

// We must take care to avoid partial dynamic filter updates when sending an
// in-memory plan.
//
// Consider this partitioned hash join topology where the consumer task is
// collocated with one producer on worker A:
// ```text
// Worker A
//
// Stage 2 Task 0
// HashJoinExec <- Dynamic Filter Produced: (foo > 100)
//
// Stage 1 Task 0
// DataSourceExec <- consumer
//
// Worker B
// Stage 2 Task 1
// HashJoinExec <- Dynamic Filter Produced: (foo != 150)
// ```
//
// The in-process transport allows the Worker A join to propagate its filter to
// the consumer and mark it as completed, so the consumer incorrectly applies
// (foo > 100) instead of (foo > 100 OR foo != 150).
//
// In this situation, we roundtrip Stage 1 Task 0 to sever the in-memory
// relationship. The dynamic filter update from the producer must reach the
// coordinator for merging prior to being forwarded to the consumer.
pub(crate) fn maybe_roundtrip_plan_to_sever_in_memory_dynamic_filter_relationships(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if has_nonlocal_dynamic_filter_relationships(&plan)? {
        roundtrip_pb(plan, task_ctx)
    } else {
        Ok(plan)
    }
}
