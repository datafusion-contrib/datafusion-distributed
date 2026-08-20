use super::DistributedCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter, physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DeduplicatingProtoConverter, PhysicalPlanDecodeContext};
use datafusion_proto::protobuf;
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
