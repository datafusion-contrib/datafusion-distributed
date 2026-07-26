use crate::DistributedCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalProtoConverter, PhysicalPlanDecodeContext,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

/// A value that a transport may either leave encoded or materialize in memory.
/// Users are free to pass [MaybeEncoded::Encoded] or [MaybeEncoded::Decoded] at any
/// moment and Distributed DataFusion's code will internally know how to handle it.
#[derive(Clone)]
pub enum MaybeEncoded<T> {
    Encoded(Vec<u8>),
    Decoded(T),
}

impl<T> MaybeEncoded<T> {
    /// Returns the decoded variant:
    /// - If in `Decoded` state, it just passes through the content.
    /// - If in `Encoded` state, it decodes using the provided callback.
    pub(crate) fn decode_with(self, decode: impl FnOnce(Vec<u8>) -> Result<T>) -> Result<T> {
        match self {
            Self::Encoded(encoded) => decode(encoded),
            Self::Decoded(decoded) => Ok(decoded),
        }
    }

    /// Returns the decoded variant:
    /// - If in `Decoded` state, it just passes through the content.
    /// - If in `Encoded` state, it throws an error.
    pub(crate) fn try_decoded(self) -> Result<T> {
        match self {
            Self::Encoded(_) => {
                internal_err!(
                    "Expected MaybeDecoded::Decoded({}), but got MaybeEncoded::Decoded",
                    std::any::type_name::<T>()
                )
            }
            Self::Decoded(decoded) => Ok(decoded),
        }
    }
}

impl MaybeEncoded<Arc<dyn ExecutionPlan>> {
    /// Returns the encoded [ExecutionPlan] as protobuf bytes:
    /// - If in `Decoded` state, it encodes it using the codecs registered in the [TaskContext].
    /// - If in `Encoded` state, it just passes through the content.
    pub fn encode(self, ctx: &Arc<TaskContext>) -> Result<Vec<u8>> {
        match self {
            Self::Encoded(encoded) => Ok(encoded),
            Self::Decoded(plan) => {
                let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
                protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &codec)
                    .map(|v| v.encode_to_vec())
            }
        }
    }

    /// Returns the decoded [ExecutionPlan].
    /// - If in `Decoded` state, it just passes through the content.
    /// - If in `Encoded` state, it decodes it using the codecs registered in the [TaskContext].
    pub(crate) fn decode(self, task_ctx: &TaskContext) -> Result<Arc<dyn ExecutionPlan>> {
        self.decode_with(|encoded| {
            let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
            let proto_node = protobuf::PhysicalPlanNode::try_decode(encoded.as_ref())?;
            proto_node.try_into_physical_plan(task_ctx, &codec)
        })
    }
}

impl MaybeEncoded<Partitioning> {
    /// Returns the encoded [Partitioning] as protobuf bytes:
    /// - If in `Decoded` state, it encodes it using the codecs registered in the [TaskContext].
    /// - If in `Encoded` state, it just passes through the content.
    pub fn encode(self, ctx: &Arc<TaskContext>) -> Result<Vec<u8>> {
        match self {
            Self::Encoded(encoded) => Ok(encoded),
            Self::Decoded(partitioning) => {
                let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
                Ok(serialize_partitioning(
                    &partitioning,
                    &codec,
                    // I think nobody cares about this being the default PhysicalProtoConverter.
                    // If someone does, please open an issue.
                    &DefaultPhysicalProtoConverter {},
                )?
                .encode_to_vec())
            }
        }
    }

    /// Returns the decoded [Partitioning].
    /// - If in `Decoded` state, it just passes through the content.
    /// - If in `Encoded` state, it decodes it using the codecs registered in the [TaskContext].
    pub fn decode(self, schema: SchemaRef, task_ctx: &TaskContext) -> Result<Partitioning> {
        self.decode_with(|encoded| {
            let proto_partitioning = protobuf::Partitioning::decode(encoded.as_slice())
                .map_err(|err| proto_error(err.to_string()))?;
            let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
            let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
            parse_protobuf_partitioning(
                Some(&proto_partitioning),
                &decode_ctx,
                &schema,
                // I think nobody cares about this being the default PhysicalProtoConverter.
                // If someone does, please open an issue.
                &DefaultPhysicalProtoConverter {},
            )?
            .ok_or_else(|| proto_error("Could not parse partitioning"))
        })
    }
}
