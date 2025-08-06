use super::combined::CombinedRecordBatchStream;
use crate::channel_manager::ChannelManager;
use crate::errors::tonic_status_to_datafusion_error;
use crate::flight_service::DoGet;
use crate::stage::{ExecutionStage, ExecutionStageProto};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{internal_datafusion_err, plan_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{future, TryFutureExt, TryStreamExt};
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone)]
pub struct ArrowFlightReadExec {
    /// the number of the stage we are reading from
    pub stage_num: usize,
    /// the properties we advertise for this execution plan
    properties: PlanProperties,
}

impl ArrowFlightReadExec {
    pub fn new(partitioning: Partitioning, schema: SchemaRef, stage_num: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            properties,
            stage_num,
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ArrowFlightReadExec: Stage {:<3}", self.stage_num)
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
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
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        /// get the channel manager and current stage from our context
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
            .find(|s| s.num == self.stage_num)
            .ok_or(internal_datafusion_err!(
                "ArrowFlightReadExec: no child stage with num {}",
                self.stage_num
            ))?;

        let child_stage_tasks = child_stage.tasks.clone();
        let child_stage_proto = ExecutionStageProto::try_from(child_stage).map_err(|e| {
            internal_datafusion_err!(
                "ArrowFlightReadExec: failed to convert stage to proto: {}",
                e
            )
        })?;

        let ticket_bytes = DoGet {
            stage_proto: Some(child_stage_proto),
            partition: partition as u64,
        }
        .encode_to_vec()
        .into();

        let ticket = Ticket {
            ticket: ticket_bytes,
        };

        let schema = child_stage.plan.schema();

        let stream = async move {
            let futs = child_stage_tasks.iter().map(|task| async {
                let url = task.url()?.ok_or(internal_datafusion_err!(
                    "ArrowFlightReadExec: task is unassigned, cannot proceed"
                ))?;
                stream_from_stage_task(ticket.clone(), &url, schema.clone(), &channel_manager).await
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
    url: &Url,
    schema: SchemaRef,
    channel_manager: &ChannelManager,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let channel = channel_manager.get_channel_for_url(&url).await?;

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
