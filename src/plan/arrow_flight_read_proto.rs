use crate::context::StageContext;
use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

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
        let ArrowFlightReadExecProto {
            partitioning,
            stage_context,
        } = ArrowFlightReadExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

        if inputs.len() != 1 {
            return Err(proto_error(format!(
                "Expected exactly 1 input, but got {}",
                inputs.len()
            )));
        }

        let Some(stage_context) = stage_context else {
            return Err(proto_error("Missing stage context"));
        };

        let Some(partitioning) = parse_protobuf_partitioning(
            partitioning.as_ref(),
            registry,
            &inputs[0].schema(),
            &DefaultPhysicalExtensionCodec {},
        )?
        else {
            return Err(proto_error("Partitioning not specified"));
        };
        let mut node = ArrowFlightReadExec::new(inputs[0].clone(), partitioning);

        fn parse_uuid(uuid: &str) -> Result<Uuid, DataFusionError> {
            uuid.parse::<Uuid>()
                .map_err(|err| proto_error(format!("{err}")))
        }

        node.stage_context = Some(StageContext {
            id: parse_uuid(&stage_context.id)?,
            n_tasks: stage_context.n_tasks as usize,
            input_id: parse_uuid(&stage_context.input_id)?,
            input_urls: stage_context
                .input_urls
                .iter()
                .map(|url| Url::parse(url))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| proto_error(format!("{err}")))?,
        });

        Ok(Arc::new(node))
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
        let Some(stage_context) = &node.stage_context else {
            return Err(proto_error(
                "Upon serializing the ArrowFlightReadExec, the stage context must be set.",
            ));
        };

        ArrowFlightReadExecProto {
            partitioning: Some(serialize_partitioning(
                &node.properties().partitioning,
                &DefaultPhysicalExtensionCodec {},
            )?),
            stage_context: Some(StageContextProto {
                id: stage_context.id.to_string(),
                n_tasks: stage_context.n_tasks as u64,
                input_id: stage_context.input_id.to_string(),
                input_urls: stage_context
                    .input_urls
                    .iter()
                    .map(|url| url.to_string())
                    .collect(),
            }),
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
    #[prost(message, optional, tag = "1")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, tag = "2")]
    stage_context: Option<StageContextProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContextProto {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(uint64, tag = "2")]
    pub n_tasks: u64,
    #[prost(string, tag = "3")]
    pub input_id: String,
    #[prost(string, repeated, tag = "4")]
    pub input_urls: Vec<String>,
}
