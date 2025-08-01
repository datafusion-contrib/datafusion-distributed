use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::internal_datafusion_err;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::{proto_error, PhysicalPlanNode};
use prost::bytes::BufMut;
use prost::Message;
use std::sync::Arc;

/// DataFusion [PhysicalExtensionCodec] implementation that allows sending and receiving
/// [ArrowFlightReadExecProto] over the wire.
#[derive(Debug)]
pub struct ArrowFlightReadExecProtoCodec {
    runtime: Arc<RuntimeEnv>,
}

impl ArrowFlightReadExecProtoCodec {
    pub fn new(runtime: &Arc<RuntimeEnv>) -> Self {
        Self {
            runtime: Arc::clone(runtime),
        }
    }
}

impl PhysicalExtensionCodec for ArrowFlightReadExecProtoCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>], // TODO: why would I want this here
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        ArrowFlightReadExecProto::try_decode(buf)?.try_into_physical_plan(
            registry,
            &self.runtime,
            self,
        )
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        ArrowFlightReadExecProto::try_from_physical_plan(node, self)?.try_encode(buf)
    }
}

/// Protobuf representation of the [ArrowFlightReadExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [ArrowFlightReadExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowFlightReadExecProto {
    #[prost(message, optional, tag = "1")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, optional, tag = "2")]
    schema: Option<protobuf::Schema>,
}

impl AsExecutionPlan for ArrowFlightReadExecProto {
    fn try_decode(buf: &[u8]) -> datafusion::common::Result<Self>
    where
        Self: Sized,
    {
        ArrowFlightReadExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))
    }

    fn try_encode<B>(&self, buf: &mut B) -> datafusion::common::Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf)
            .map_err(|err| proto_error(format!("{err}")))
    }

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema: Schema = self
            .schema
            .as_ref()
            .ok_or(internal_datafusion_err!("missing schema in proto"))?
            .try_into()?;

        let partitioning = parse_protobuf_partitioning(
            self.partitioning.as_ref(),
            registry,
            &schema,
            extension_codec,
        )?
        .ok_or(internal_datafusion_err!(
            "could not decode partitioning from proto"
        ))?;

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(schema)),
            partitioning.clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(ArrowFlightReadExec::new(&properties, "")))
    }

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> datafusion::common::Result<Self>
    where
        Self: Sized,
    {
        let Some(node) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Err(proto_error("Only ArrowFlightReadExec is supported"));
        };

        Ok(Self {
            partitioning: Some(serialize_partitioning(
                &node.properties().partitioning,
                extension_codec,
            )?),
            schema: Some(plan.schema().try_into()?),
        })
    }
}
