use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;

/// DataFusion [PhysicalExtensionCodec] implementation that allows sending and receiving
/// [ArrowFlightReadExecProto] over the wire.
#[derive(Debug)]
pub struct ArrowFlightReadExecProtoCodec;

impl PhysicalExtensionCodec for ArrowFlightReadExecProtoCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let ArrowFlightReadExecProto { partitioning } =
            ArrowFlightReadExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

        if inputs.len() != 1 {
            return Err(proto_error(format!(
                "Expected exactly 1 input, but got {}",
                inputs.len()
            )));
        }

        let Some(partitioning) = parse_protobuf_partitioning(
            partitioning.as_ref(),
            registry,
            &inputs[0].schema(),
            &DefaultPhysicalExtensionCodec {},
        )?
        else {
            return Err(proto_error("Partitioning not specified"));
        };
        Ok(Arc::new(ArrowFlightReadExec::new(
            inputs[0].clone(),
            partitioning,
        )))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let Some(node) = node.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Err(proto_error(format!(
                "Expected ArrowFlightReadExec, but got {}",
                node.name()
            )));
        };

        ArrowFlightReadExecProto {
            partitioning: Some(serialize_partitioning(
                &node.properties().partitioning,
                &DefaultPhysicalExtensionCodec {},
            )?),
        }
        .encode(buf)
        .map_err(|err| proto_error(format!("{err}")))
    }
}

/// Protobuf representation of the [ArrowFlightReadExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [ArrowFlightReadExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowFlightReadExecProto {
    #[prost(message, optional, tag = "2")]
    partitioning: Option<protobuf::Partitioning>,
}
