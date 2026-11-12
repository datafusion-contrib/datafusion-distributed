use super::DistributedCodec;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{HashMap, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties, ReplaceChildrenOptions,
};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter, physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr_with_converter, parse_protobuf_partitioning,
};
use datafusion_proto::physical_plan::to_proto::{
    serialize_partitioning, serialize_physical_expr_with_converter,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalPlanNodeExt, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::cell::RefCell;
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

/// Round-trips a single node through protobuf to produce an independent [`ExecutionPlan`].
///
/// Only `node` is serialized: its children are swapped for [`EmptyExec`] placeholders during
/// the round-trip and put back afterwards.
///
/// ```text
/// node               node              node
///   ...children  ->    EmptyExec  ->     ...children
/// ```
///
/// Nodes round-tripped with the same `converter` keep sharing the [`PhysicalExpr`]s (e.g.
/// dynamic filters) they shared before the round-trip.
pub(crate) fn roundtrip_pb(
    node: Arc<dyn ExecutionPlan>,
    task_ctx: &TaskContext,
    converter: &RoundtripProtoConverter,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = node.children().into_iter().cloned().collect::<Vec<_>>();
    let placeholders = children
        .iter()
        .map(|child| {
            let partitions = child.output_partitioning().partition_count();
            Arc::new(EmptyExec::new(child.schema()).with_partitions(partitions)) as _
        })
        .collect();
    let recompute = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
    let node = node.replace_children(placeholders, recompute)?;

    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let proto = converter.execution_plan_to_proto(&node, &codec)?;
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
    let node = converter.proto_to_execution_plan(&proto, &decode_ctx)?;

    node.replace_children(children, recompute)
}

/// Round-trips a single expression through protobuf. See [`roundtrip_pb`].
pub(crate) fn roundtrip_expr_pb(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    task_ctx: &TaskContext,
    converter: &RoundtripProtoConverter,
) -> Result<Arc<dyn PhysicalExpr>> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let proto = converter.physical_expr_to_proto(expr, &codec)?;
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
    converter.proto_to_physical_expr(&proto, input_schema, &decode_ctx)
}

/// Same as [`DeduplicatingProtoConverter`], but expressions are deduplicated across every
/// round-trip performed with the same instance instead of within a single one.
#[derive(Default)]
pub(crate) struct RoundtripProtoConverter {
    /// Decoded expressions keyed by `expr_id`.
    decoded: RefCell<HashMap<u64, Arc<dyn PhysicalExpr>>>,
}

impl PhysicalProtoConverterExtension for RoundtripProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &protobuf::PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        proto.try_into_physical_plan_with_context(ctx, self)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalPlanNode> {
        protobuf::PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(plan),
            codec,
            self,
        )
    }

    fn proto_to_physical_expr(
        &self,
        proto: &protobuf::PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let parsed = parse_physical_expr_with_converter(proto, input_schema, ctx, self)?;
        let Some(id) = proto.expr_id else {
            return Ok(parsed);
        };
        if let Some(cached) = self.decoded.borrow().get(&id) {
            // Same as DeduplicatingProtoConverter: `with_new_children` keeps the state shared
            // with the cached expression.
            let children = parsed.children().into_iter().cloned().collect();
            return Arc::clone(cached).with_new_children(children);
        }
        self.decoded.borrow_mut().insert(id, Arc::clone(&parsed));
        Ok(parsed)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<protobuf::PhysicalExprNode> {
        serialize_physical_expr_with_converter(expr, codec, self)
    }
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
