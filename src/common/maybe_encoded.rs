use crate::DistributedCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DeduplicatingProtoConverter, DefaultPhysicalProtoConverter,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
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
                DeduplicatingProtoConverter::default()
                    .execution_plan_to_proto(&plan, &codec)
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
            let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx, &codec);
            DeduplicatingProtoConverter::default().proto_to_execution_plan(&proto_node, &decode_ctx)
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::SessionContext;

    #[test]
    fn execution_plan_roundtrip_preserves_expression_identity() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let input = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let inner =
            Arc::new(FilterExec::try_new(dynamic_filter.clone(), input)?) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(FilterExec::try_new(dynamic_filter, inner)?) as Arc<dyn ExecutionPlan>;

        let task_ctx = SessionContext::new().task_ctx();
        let encoded = MaybeEncoded::Decoded(plan).encode(&task_ctx)?;
        let decoded = MaybeEncoded::<Arc<dyn ExecutionPlan>>::Encoded(encoded).decode(&task_ctx)?;

        let outer = decoded.downcast_ref::<FilterExec>().unwrap();
        let inner = outer.input().downcast_ref::<FilterExec>().unwrap();
        let outer_filter = outer
            .predicate()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap();
        let inner_filter = inner
            .predicate()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap();

        outer_filter.update(lit(false))?;
        assert_eq!(inner_filter.current()?.to_string(), "false");
        Ok(())
    }
}
