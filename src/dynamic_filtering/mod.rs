mod discovery;
mod display;

use crate::DistributedConfig;
use crate::codec::{decode_physical_expr, encode_physical_expr, roundtrip_pb};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use std::sync::Arc;

pub(crate) use discovery::*;
pub use display::rewrite_distributed_plan_with_dynamic_filters;
pub(crate) use display::sever_dynamic_filter_relationships_in_plan_for_display;

pub(crate) fn is_local_dynamic_filtering_enabled(session_config: &SessionConfig) -> bool {
    session_config
        .options()
        .optimizer
        .enable_dynamic_filter_pushdown
}

pub(crate) fn is_remote_dynamic_filtering_enabled(session_config: &SessionConfig) -> Result<bool> {
    let remote_enabled =
        DistributedConfig::from_session_config(session_config)?.remote_dynamic_filters;
    let local_enabled = is_local_dynamic_filtering_enabled(session_config);
    Ok(remote_enabled && local_enabled)
}

/// Deepcopies the plan if it contains any dynamic filter producers or consumers. This isolates
/// any dynamic filters in this plan from dynamic filters *outside* the plan. Plan nodes
/// *within* this plan will share in-memory dynamic filter state with eachother, even after copying.
///
/// # Why Task-Local Dynamic Filters are Safe
///
/// ## TopK Dynamic Filters
///
/// ```text
/// Stage 2 Tasks: M
/// └── SortPreservingMergeExec: fetch=10 (global TopK)
///     └── NetworkCoalesceExec
///
/// Stage 1 Tasks: N
/// └── SortExec: fetch=10 (local TopK and dynamic-filter producer)
///     └── DataSourceExec: dynamic-filter consumer
/// ```
///
/// Each `SortExec` may push its task-local TopK bound into its own input. It still emits the local
/// TopK candidates, which the `SortPreservingMergeExec` reduces to the global TopK in the parent
/// stage.
///
/// ## Min/Max Dynamic Filters in Partial Aggregates with No Group
///
/// ```text
/// Stage 2 Tasks: M
/// └── AggregateExec: mode=FinalPartitioned, gby=[], aggr=[max(foo)] (global max)
///     └── NetworkShuffleExec
///
/// Stage 1 Tasks: N
/// └── RepartitionExec
///     └── AggregateExec: mode=Partial, gby=[], aggr=[max(foo)] (local max and dynamic filter producer)
///         └── DataSourceExec: dynamic-filter consumer
/// ```
///
/// The partial aggregate may push its task-local min/max bound into its own input. It still emits
/// the local min/max, which the final aggregate reduces to the global min/max in the parent stage.
///
/// ## CollectLeft Joins
///
/// ```text
/// Stage Y Tasks: M
///
/// HashJoinExec: mode=CollectLeft
/// ├── build: CoalescePartitionsExec
/// │   └── all build partitions (the complete build side)
/// └── probe:
///     └── DataSourceExec: dynamic-filter consumer
/// ```
///
/// A `CollectLeft` hash join collects the complete build side in every task (by broadcasting or
/// otherwise) and pushes it down to every probe partition. Since every probe partition sees
/// the entire build-side filter, it will only filter rows which the join would filter out.
///
/// ## Partitioned Joins
///
/// A partitioned hash join builds per-partition predicates from the task-local hash table.
///
/// ```text
/// Stage Y Task i
///
/// HashJoinExec: mode=Partitioned
/// ├── build partition i  → producer predicate P(i)
/// └── probe partition i  → consumer of P(i)
/// ```
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

/// Applies a predicate to a consumer dynamic filter expression.
///
/// To update a consumer, this function creates a synthetic "producer"
/// [`DynamicFilterPhysicalExpr`] which can be used to update consumer
/// dynamic filters via a shared state. The reason for this is to match
/// the same behavior as DataFusion.
///
/// ```text
/// HashJoinExec: on=[build.key = probe.key]
/// │   └── producer: original=[key], children=[key]   ──────────────┐
/// ├── build                                                        │
/// └── UnionExec: probe                                             │
///     ├── DataSourceExec: phone_number AS key                      │ shared inner
///     │   └── consumer 1: original=[key], children=[phone_number] ─┤
///     └── DataSourceExec: telephone AS key                         │
///         └── consumer 2: original=[key], children=[telephone]  ───┘
/// ```
///
/// Behavior:
/// 1. [`DynamicFilterPhysicalExpr::update()`] remaps any occurrences of `original` to `children`
///    and stores the result in the shared state.
///
/// 2. [`DynamicFilterPhysicalExpr::current()`] reads the remapped expression and remaps it again,
///    mapping `original` to `children` and returns it without storing.
///
/// In the above plan, the producer calls update() so an expression like `key > 123` is mapped to
/// `key > 123` and this is stored. Then, the consumers call current(), reading from the same state,
/// to get `phone_number > 123` and `telephone > 123` respectively.
///
/// You cannot update() consumers directly. In the above example, updating consumer 1 would remap
/// `key > 123` to `phone_number > 123` and store this in the shared state. Consumer 2 would be
/// unable to apply this filter now.
pub(crate) fn apply_dynamic_filter_update(
    consumer: &Arc<DynamicFilterPhysicalExpr>,
    predicate: &protobuf::PhysicalExprNode,
    producer_schema: &Schema,
    task_ctx: &TaskContext,
) -> Result<()> {
    let predicate = decode_physical_expr(predicate, producer_schema, task_ctx)?;
    // Since consumer.children() returns the remapped children, we use the proto as a workaround to get the
    // original children from the producer.
    let consumer: Arc<dyn PhysicalExpr> = consumer.clone();
    let proto = encode_physical_expr(&consumer, task_ctx)?;
    let Some(ExprType::DynamicFilter(dynamic_filter)) = proto.expr_type else {
        return internal_err!("expected a dynamic filter expression");
    };
    let original_children = dynamic_filter
        .children
        .iter()
        .map(|child| decode_physical_expr(child, producer_schema, task_ctx))
        .collect::<Result<Vec<_>>>()?;
    let update_target = Arc::clone(&consumer).with_new_children(original_children)?;
    let Ok(update_target) = Arc::downcast::<DynamicFilterPhysicalExpr>(update_target) else {
        return internal_err!("expected a dynamic filter update target");
    };
    update_target.update(predicate)
}
