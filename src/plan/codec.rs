use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

use super::PartitionIsolatorExec;

/// DataFusion [PhysicalExtensionCodec] implementation that allows serializing and
/// deserializing the custom ExecutionPlans in this project
#[derive(Debug)]
pub struct DistributedCodec;

impl PhysicalExtensionCodec for DistributedCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let DistributedExecProto {
            node: Some(distributed_exec_node),
        } = DistributedExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?
        else {
            return Err(proto_error(
                "Expected DistributedExecNode in DistributedExecProto",
            ));
        };

        match distributed_exec_node {
            DistributedExecNode::ArrowFlightReadExec(ArrowFlightReadExecProto {
                schema,
                partitioning,
                stage_num,
            }) => {
                let schema: Schema = schema
                    .as_ref()
                    .map(|s| s.try_into())
                    .ok_or(proto_error("ArrowFlightReadExec is missing schema"))??;

                let partioning = parse_protobuf_partitioning(
                    partitioning.as_ref(),
                    registry,
                    &schema,
                    &DistributedCodec {},
                )?
                .ok_or(proto_error("ArrowFlightReadExec is missing partitioning"))?;

                Ok(Arc::new(ArrowFlightReadExec::new(
                    partioning,
                    Arc::new(schema),
                    stage_num as usize,
                )))
            }
            DistributedExecNode::PartitionIsolatorExec(PartitionIsolatorExecProto {
                partition_count,
            }) => {
                if inputs.len() != 1 {
                    return Err(proto_error(format!(
                        "PartitionIsolatorExec expects exactly one child, got {}",
                        inputs.len()
                    )));
                }

                let child = inputs.first().unwrap();

                Ok(Arc::new(PartitionIsolatorExec::new(
                    child.clone(),
                    partition_count as usize,
                )))
            }
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        if let Some(node) = node.as_any().downcast_ref::<ArrowFlightReadExec>() {
            let inner = ArrowFlightReadExecProto {
                schema: Some(node.schema().try_into()?),
                partitioning: Some(serialize_partitioning(
                    node.properties().output_partitioning(),
                    &DistributedCodec {},
                )?),
                stage_num: node.stage_num as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::ArrowFlightReadExec(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else if let Some(node) = node.as_any().downcast_ref::<PartitionIsolatorExec>() {
            let inner = PartitionIsolatorExecProto {
                partition_count: node.partition_count as u64,
            };

            let wrapper = DistributedExecProto {
                node: Some(DistributedExecNode::PartitionIsolatorExec(inner)),
            };

            wrapper.encode(buf).map_err(|e| proto_error(format!("{e}")))
        } else {
            Err(proto_error(format!("Unexpected plan {}", node.name())))
        }
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistributedExecProto {
    #[prost(oneof = "DistributedExecNode", tags = "1, 2")]
    pub node: Option<DistributedExecNode>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DistributedExecNode {
    #[prost(message, tag = "1")]
    ArrowFlightReadExec(ArrowFlightReadExecProto),
    #[prost(message, tag = "2")]
    PartitionIsolatorExec(PartitionIsolatorExecProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionIsolatorExecProto {
    #[prost(uint64, tag = "1")]
    pub partition_count: u64,
}

/// Protobuf representation of the [ArrowFlightReadExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [ArrowFlightReadExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowFlightReadExecProto {
    #[prost(message, optional, tag = "1")]
    schema: Option<protobuf::Schema>,
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(uint64, tag = "3")]
    stage_num: u64,
}
