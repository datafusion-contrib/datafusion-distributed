use crate::DistributedCodec;
use crate::distributed_planner::ProducerHead;
use crate::worker::generated::worker::{
    BroadcastExecHead, NoneHead, RepartitionExecHead, execute_task_request as pb,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalPlanDecodeContext};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

impl ProducerHead {
    pub(crate) fn from_proto(
        proto: pb::ProducerHead,
        input_schema: &SchemaRef,
        ctx: &Arc<TaskContext>,
    ) -> Result<Self> {
        Ok(match proto {
            pb::ProducerHead::None(_) => ProducerHead::None,
            pb::ProducerHead::Broadcast(v) => ProducerHead::BroadcastExec {
                output_partitions: v.output_partitions as usize,
            },
            pb::ProducerHead::Repartition(v) => {
                let proto_partitioning = protobuf::Partitioning::decode(v.partitioning.as_slice())
                    .map_err(|e| proto_error(e.to_string()))?;
                let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
                let decode_ctx = PhysicalPlanDecodeContext::new(ctx, &codec);
                let partitioning = parse_protobuf_partitioning(
                    Some(&proto_partitioning),
                    &decode_ctx,
                    input_schema,
                    &DefaultPhysicalProtoConverter {},
                )?
                .ok_or_else(|| proto_error("Could not parse partitioning"))?;

                ProducerHead::RepartitionExec { partitioning }
            }
        })
    }

    pub(crate) fn to_proto(&self, ctx: &Arc<TaskContext>) -> Result<pb::ProducerHead> {
        Ok(match self {
            ProducerHead::None => pb::ProducerHead::None(NoneHead {}),
            ProducerHead::BroadcastExec { output_partitions } => {
                pb::ProducerHead::Broadcast(BroadcastExecHead {
                    output_partitions: *output_partitions as u64,
                })
            }
            ProducerHead::RepartitionExec { partitioning } => {
                let codec = DistributedCodec::new_combined_with_user(ctx.session_config());
                let partitioning =
                    serialize_partitioning(partitioning, &codec, &DefaultPhysicalProtoConverter {})
                        .map(|v| v.encode_to_vec())?;
                pb::ProducerHead::Repartition(RepartitionExecHead { partitioning })
            }
        })
    }
}
