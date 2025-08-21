use super::combined::CombinedRecordBatchStream;
use crate::channel_manager::ChannelManager;
use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::errors::tonic_status_to_datafusion_error;
use crate::flight_service::{DoGet, StageKey};
use crate::plan::DistributedCodec;
use crate::stage::{proto_from_stage, ExecutionStage};
use crate::user_provided_codec::get_user_codec;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_err, internal_datafusion_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{future, TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::Request;
use url::Url;

/// This node has two variants.
/// 1. Pending: it acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///     using Arrow Flight.
#[derive(Debug, Clone)]
pub enum ArrowFlightReadExec {
    Pending(ArrowFlightReadPendingExec),
    Ready(ArrowFlightReadReadyExec),
}

/// Placeholder version of the [ArrowFlightReadExec] node. It acts as a marker for the
/// distributed optimization step, which will replace it with the appropriate
/// [ArrowFlightReadReadyExec] node.
#[derive(Debug, Clone)]
pub struct ArrowFlightReadPendingExec {
    properties: PlanProperties,
    child: Arc<dyn ExecutionPlan>,
}

/// Ready version of the [ArrowFlightReadExec] node. This node can be created in
/// just two ways:
/// - by the distributed optimization step based on an original [ArrowFlightReadPendingExec]
/// - deserialized from a protobuf plan sent over the network.
#[derive(Debug, Clone)]
pub struct ArrowFlightReadReadyExec {
    /// the properties we advertise for this execution plan
    properties: PlanProperties,
    pub(crate) stage_num: usize,
}

impl ArrowFlightReadExec {
    pub fn new_pending(child: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        Self::Pending(ArrowFlightReadPendingExec {
            properties: PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            child,
        })
    }

    pub(crate) fn new_ready(
        partitioning: Partitioning,
        schema: SchemaRef,
        stage_num: usize,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self::Ready(ArrowFlightReadReadyExec {
            properties,
            stage_num,
        })
    }

    pub(crate) fn to_distributed(&self, stage_num: usize) -> Result<Self, DataFusionError> {
        match self {
            ArrowFlightReadExec::Pending(p) => Ok(Self::new_ready(
                p.properties.partitioning.clone(),
                p.child.schema(),
                stage_num,
            )),
            _ => internal_err!("ArrowFlightReadExec is already distributed"),
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ArrowFlightReadExec::Pending(_) => write!(f, "ArrowFlightReadExec"),
            ArrowFlightReadExec::Ready(v) => {
                write!(f, "ArrowFlightReadExec: Stage {:<3}", v.stage_num)
            }
        }
    }
}

impl ExecutionPlan for ArrowFlightReadExec {
    fn name(&self) -> &str {
        "ArrowFlightReadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            ArrowFlightReadExec::Pending(v) => &v.properties,
            ArrowFlightReadExec::Ready(v) => &v.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            ArrowFlightReadExec::Pending(v) => vec![&v.child],
            ArrowFlightReadExec::Ready(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return plan_err!(
                "ArrowFlightReadExec: wrong number of children, expected 0, got {}",
                children.len()
            );
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let ArrowFlightReadExec::Ready(this) = self else {
            return exec_err!("ArrowFlightReadExec is not ready, was the distributed optimization step performed?");
        };

        // get the channel manager and current stage from our context
        let channel_manager: ChannelManager = context.as_ref().try_into()?;

        let stage = context
            .session_config()
            .get_extension::<ExecutionStage>()
            .ok_or(internal_datafusion_err!(
                "ArrowFlightReadExec requires an ExecutionStage in the session config"
            ))?;

        // of our child stages find the one that matches the one we are supposed to be
        // reading from
        let child_stage = stage
            .child_stages_iter()
            .find(|s| s.num == this.stage_num)
            .ok_or(internal_datafusion_err!(
                "ArrowFlightReadExec: no child stage with num {}",
                this.stage_num
            ))?;

        let flight_metadata = context
            .session_config()
            .get_extension::<ContextGrpcMetadata>();

        let mut combined_codec = ComposedPhysicalExtensionCodec::default();
        combined_codec.push(DistributedCodec {});
        if let Some(ref user_codec) = get_user_codec(context.session_config()) {
            combined_codec.push_arc(Arc::clone(user_codec));
        }

        let child_stage_proto = proto_from_stage(child_stage, &combined_codec).map_err(|e| {
            internal_datafusion_err!("ArrowFlightReadExec: failed to convert stage to proto: {e}")
        })?;

        let schema = child_stage.plan.schema();

        let child_stage_tasks = child_stage.tasks.clone();
        let child_stage_num = child_stage.num as u64;
        let query_id = stage.query_id.to_string();

        let stream = async move {
            let futs = child_stage_tasks.iter().enumerate().map(|(i, task)| {
                let child_stage_proto_capture = child_stage_proto.clone();
                let channel_manager_capture = channel_manager.clone();
                let schema = schema.clone();
                let query_id = query_id.clone();
                let flight_metadata = flight_metadata
                    .as_ref()
                    .map(|v| v.as_ref().clone())
                    .unwrap_or_default();
                let key = StageKey {
                    query_id,
                    stage_id: child_stage_num,
                    task_number: i as u64,
                };
                async move {
                    let url = task.url()?.ok_or(internal_datafusion_err!(
                        "ArrowFlightReadExec: task is unassigned, cannot proceed"
                    ))?;

                    let ticket_bytes = DoGet {
                        stage_proto: Some(child_stage_proto_capture),
                        partition: partition as u64,
                        stage_key: Some(key),
                        task_number: i as u64,
                    }
                    .encode_to_vec()
                    .into();

                    let ticket = Ticket {
                        ticket: ticket_bytes,
                    };

                    stream_from_stage_task(
                        ticket,
                        flight_metadata,
                        &url,
                        schema.clone(),
                        &channel_manager_capture,
                    )
                    .await
                }
            });

            let streams = future::try_join_all(futs).await?;

            let combined_stream = CombinedRecordBatchStream::try_new(schema, streams)?;

            Ok(combined_stream)
        }
        .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

async fn stream_from_stage_task(
    ticket: Ticket,
    metadata: ContextGrpcMetadata,
    url: &Url,
    schema: SchemaRef,
    channel_manager: &ChannelManager,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let channel = channel_manager.get_channel_for_url(url).await?;

    let ticket = Request::from_parts(
        MetadataMap::from_headers(metadata.0),
        Extensions::default(),
        ticket,
    );

    let mut client = FlightServiceClient::new(channel);
    let stream = client
        .do_get(ticket)
        .await
        .map_err(|err| {
            tonic_status_to_datafusion_error(&err)
                .unwrap_or_else(|| DataFusionError::External(Box::new(err)))
        })?
        .into_inner()
        .map_err(|err| FlightError::Tonic(Box::new(err)));

    let stream = FlightRecordBatchStream::new_from_flight_data(stream).map_err(|err| match err {
        FlightError::Tonic(status) => tonic_status_to_datafusion_error(&status)
            .unwrap_or_else(|| DataFusionError::External(Box::new(status))),
        err => DataFusionError::External(Box::new(err)),
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream,
    )))
}
