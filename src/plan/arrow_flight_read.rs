use crate::channel_manager_ext::get_distributed_channel_resolver;
use crate::config_extension_ext::ContextGrpcMetadata;
use crate::errors::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use crate::flight_service::{DoGet, StageKey};
use crate::plan::DistributedCodec;
use crate::stage::{proto_from_stage, ExecutionStage};
use crate::ChannelResolver;
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
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use http::Extensions;
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::Request;

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
        let Some(channel_resolver) = get_distributed_channel_resolver(context.session_config())
        else {
            return exec_err!(
                "ArrowFlightReadExec requires a ChannelResolver in the session config"
            );
        };

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

        let codec = DistributedCodec::new_combined_with_user(context.session_config());

        let child_stage_proto = proto_from_stage(child_stage, &codec).map_err(|e| {
            internal_datafusion_err!("ArrowFlightReadExec: failed to convert stage to proto: {e}")
        })?;

        let child_stage_tasks = child_stage.tasks.clone();
        let child_stage_num = child_stage.num as u64;
        let query_id = stage.query_id.to_string();

        let context_headers = flight_metadata
            .as_ref()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();

        let stream = child_stage_tasks.into_iter().enumerate().map(|(i, task)| {
            let channel_resolver = Arc::clone(&channel_resolver);

            let ticket = Request::from_parts(
                MetadataMap::from_headers(context_headers.0.clone()),
                Extensions::default(),
                Ticket {
                    ticket: DoGet {
                        stage_proto: Some(child_stage_proto.clone()),
                        partition: partition as u64,
                        stage_key: Some(StageKey {
                            query_id: query_id.clone(),
                            stage_id: child_stage_num,
                            task_number: i as u64,
                        }),
                        task_number: i as u64,
                    }
                    .encode_to_vec()
                    .into(),
                },
            );

            async move {
                let url = task.url()?.ok_or(internal_datafusion_err!(
                    "ArrowFlightReadExec: task is unassigned, cannot proceed"
                ))?;

                let channel = channel_resolver.get_channel_for_url(&url).await?;
                let stream = FlightServiceClient::new(channel)
                    .do_get(ticket)
                    .await
                    .map_err(map_status_to_datafusion_error)?
                    .into_inner()
                    .map_err(|err| FlightError::Tonic(Box::new(err)));

                Ok(FlightRecordBatchStream::new_from_flight_data(stream)
                    .map_err(map_flight_to_datafusion_error))
            }
            .try_flatten_stream()
            .boxed()
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::select_all(stream),
        )))
    }
}
