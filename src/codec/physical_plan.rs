use super::DistributedCodec;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter, physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

/// Creates the converter used by all top-level physical protobuf operations.
///
/// Extension codecs must use the converter supplied to their callbacks so the
/// same deduplication state is preserved for the entire operation.
pub(super) fn new_proto_converter() -> DeduplicatingProtoConverter {
    DeduplicatingProtoConverter::default()
}

pub(crate) fn encode_execution_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &TaskContext,
) -> Result<Vec<u8>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let converter = new_proto_converter();
    physical_plan_to_bytes_with_proto_converter(plan, &codec, &converter)
        .map(|bytes| bytes.to_vec())
}

pub(crate) fn decode_execution_plan(
    encoded: &[u8],
    task_ctx: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let converter = new_proto_converter();
    physical_plan_from_bytes_with_proto_converter(encoded, task_ctx, &codec, &converter)
}

/// Round-trips a plan through protobuf to produce an independent [`ExecutionPlan`].
pub(crate) fn roundtrip_pb(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let encoded = encode_execution_plan(plan, task_ctx)?;
    decode_execution_plan(&encoded, task_ctx)
}

pub(crate) fn encode_physical_expr(
    expression: &Arc<dyn PhysicalExpr>,
    task_ctx: &TaskContext,
) -> Result<protobuf::PhysicalExprNode> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let converter = new_proto_converter();
    converter.physical_expr_to_proto(expression, &codec)
}

pub(crate) fn decode_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    input_schema: &Schema,
    task_ctx: &TaskContext,
) -> Result<Arc<dyn PhysicalExpr>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
    let converter = new_proto_converter();
    converter.proto_to_physical_expr(proto, input_schema, &decode_ctx)
}

/// Creates a sythetic "producer" [`DynamicFilterPhysicalExpr`] which can be used to
/// update consumer dynamic filters via a shared state.
///
/// DataFusion does a particular song and dance to update dynamic filters. This function
/// helps us do the same.
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
/// 1. [`DynamicFilterPhysicalExpr::update()`] remaps any occurences of `original` to `children`
/// and stores the result in the shared state.
///
/// 2. [`DynamicFilterPhysicalExpr::current()`] reads the remapped expression and remaps it again,
/// mapping `original` to `children` and returns it without storing.
///
/// In the above plan, the producer calls update() so an expression like `key > 123` is mapped to
/// `key > 123` and this is stored. Then, the consumers call current(), reading from the same state,
/// to get `phone_number > 123` and `telephone > 123` respectively.
///
/// You cannot update() consumers directly. In the above example, updating consumer 1 would remap
/// `key > 123` to `phone_number > 123` and store this in the shared state. Consumer 2 would be
/// unable to apply this filter now.
pub(crate) fn dynamic_filter_update_target(
    consumer: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    task_ctx: &TaskContext,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    // Since consumer.children() returns the remapped children, we use the proto as a workaround to get the 
    // original children from the producer.
    let proto = encode_physical_expr(consumer, task_ctx)?;
    let Some(ExprType::DynamicFilter(dynamic_filter)) = proto.expr_type else {
        return internal_err!("expected a dynamic filter expression");
    };
    let original_children = dynamic_filter
        .children
        .iter()
        .map(|child| decode_physical_expr(child, input_schema, task_ctx))
        .collect::<Result<Vec<_>>>()?;
    let update_target = Arc::clone(consumer).with_new_children(original_children)?;
    let Ok(update_target) = Arc::downcast::<DynamicFilterPhysicalExpr>(update_target) else {
        return internal_err!("expected a dynamic filter update target");
    };
    Ok(update_target)
}

pub(crate) fn encode_partitioning(
    partitioning: &Partitioning,
    task_ctx: &TaskContext,
) -> Result<Vec<u8>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let converter = new_proto_converter();
    Ok(serialize_partitioning(partitioning, &codec, &converter)?.encode_to_vec())
}

pub(crate) fn decode_partitioning(
    encoded: &[u8],
    schema: SchemaRef,
    task_ctx: &TaskContext,
) -> Result<Partitioning> {
    let proto_partitioning =
        protobuf::Partitioning::decode(encoded).map_err(|err| proto_error(err.to_string()))?;
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
    let converter = new_proto_converter();
    parse_protobuf_partitioning(Some(&proto_partitioning), &decode_ctx, &schema, &converter)?
        .ok_or_else(|| proto_error("Could not parse partitioning"))
}
