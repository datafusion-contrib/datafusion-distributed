use crate::MaybeEncoded;
use crate::codec::decode_physical_expr;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use std::sync::Arc;

/// Applies a `source` dynamic filter expression to a `target` dynamic filter expression.
///
/// This function always uses the original children of the dynamic filter expression
/// when applying an update. See below.
///
/// ## Normal Behavior
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
/// 1. [`DynamicFilterPhysicalExpr::update()`] remaps any occurrences of `original` to `children`
///    and stores the result in the shared state.
///
/// 2. [`DynamicFilterPhysicalExpr::current()`] reads the remapped expression and remaps it again,
///    mapping `original` to `children` and returns it without storing.
///
/// Example: the producer calls update(`key > 123`) which trivially maps to `key > 123`
/// and is stored. Then, the consumers call current(), reading from the shared inner state,
/// to get the remapped `phone_number > 123` and `telephone > 123` expressions respectively.
///
/// ## Failiure Mode
///
/// The source expression may be a consumer and cause remapping to fail.
///
/// Say that the source and target are both `consumer 1: original=[key], children=[phone_number]`.
/// The source is a populated filter from a worker `phone_number > 123` and the target is empty,
/// used for display.
///
/// In this situation, we should use the inner expression `key > 123` from the source to call
/// update(`key > 123`). We should not call `update(source.current())` or
/// `update(phone_number > 123)`, or else the update fails.
pub(crate) fn apply_dynamic_filter_update(
    target: &Arc<DynamicFilterPhysicalExpr>,
    source: &MaybeEncoded<Arc<dyn PhysicalExpr>>,
    source_schema: &Schema,
    task_ctx: &TaskContext,
) -> Result<()> {
    // We need two private fields from the source, so we convert to the proto
    // representation as a workaround.
    // 1. `is_complete` (which is public in df56)
    // 2. The inner expression which has unremapped children (this is only
    //    required when the source is another consumer)
    //
    // TODO: Avoid needing to encode to proto.
    let source_proto = source.to_proto(task_ctx)?;
    if source_proto.expr_id != target.expression_id() {
        return internal_err!("dynamic filter update has a mismatched expression ID");
    }
    let Some(ExprType::DynamicFilter(source_dynamic_filter)) = source_proto.expr_type else {
        return internal_err!("expected a dynamic filter update");
    };
    let Some(source_predicate) = source_dynamic_filter.inner_expr.as_deref() else {
        return internal_err!("dynamic filter update has no predicate");
    };
    let predicate = decode_physical_expr(source_predicate, source_schema, task_ctx)?;

    let source_original_children = source_dynamic_filter
        .children
        .iter()
        .map(|child| decode_physical_expr(child, source_schema, task_ctx))
        .collect::<Result<Vec<_>>>()?;

    let update_target = Arc::clone(target).with_new_children(source_original_children)?;
    let Ok(update_target) = Arc::downcast::<DynamicFilterPhysicalExpr>(update_target) else {
        return internal_err!("expected a dynamic filter update target");
    };
    update_target.update(predicate)?;
    if source_dynamic_filter.is_complete {
        update_target.mark_complete();
    }
    Ok(())
}
