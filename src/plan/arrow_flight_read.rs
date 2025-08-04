use crate::channel_manager::ChannelManager;
use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::context::{StageContext, StageTaskContext};
use crate::errors::tonic_status_to_datafusion_error;
use crate::flight_service::DoGet;
use crate::plan::arrow_flight_read_proto::ArrowFlightReadExecProtoCodec;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::common::{internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::IntoRequest;

#[derive(Debug, Clone)]
pub struct ArrowFlightReadExec {
    properties: PlanProperties,
    child: Arc<dyn ExecutionPlan>,
    pub(crate) stage_context: Option<StageContext>,
}

impl ArrowFlightReadExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            child,
            stage_context: None,
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let (hash_expr, size) = match &self.properties.partitioning {
            Partitioning::RoundRobinBatch(size) => (vec![], size),
            Partitioning::Hash(hash_expr, size) => (hash_expr.clone(), size),
            Partitioning::UnknownPartitioning(size) => (vec![], size),
        };

        let hash_expr = hash_expr
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ");

        let stage_trail = match &self.stage_context {
            None => " (Unassigned stage)".to_string(),
            Some(stage) => format!(
                " stage_id={} input_stage_id={} input_hosts=[{}]",
                stage.id,
                stage.input_id,
                stage
                    .input_urls
                    .iter()
                    .map(|url| url.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };

        write!(
            f,
            "ArrowFlightReadExec: input_tasks={size} hash_expr=[{hash_expr}]{stage_trail}",
        )
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
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "ArrowFlightReadExec: wrong number of children, expected 1, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            child: Arc::clone(&children[0]),
            stage_context: self.stage_context.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let plan = Arc::clone(&self.child);
        let channel_manager: ChannelManager = context.as_ref().try_into()?;

        let Some(stage) = self.stage_context.clone() else {
            return plan_err!("No stage assigned to this ArrowFlightReadExec");
        };
        let task_context = context.session_config().get_extension::<StageTaskContext>();

        let hash_expr = match &self.properties.partitioning {
            Partitioning::Hash(hash_expr, _) => hash_expr.clone(),
            _ => vec![],
        };

        let stream = async move {
            if partition >= stage.input_urls.len() {
                return internal_err!(
                    "Invalid partition {partition} for a stage with only {} inputs",
                    stage.input_urls.len()
                );
            }

            let channel = channel_manager
                .get_channel_for_url(&stage.input_urls[partition])
                .await?;

            let mut codec = ComposedPhysicalExtensionCodec::default();
            codec.push(ArrowFlightReadExecProtoCodec);
            codec.push_from_config(context.session_config());

            let ticket = DoGet::new_remote_plan_exec_ticket(
                plan,
                stage.input_id,
                partition,
                task_context.as_ref().map(|v| v.task_idx).unwrap_or(0),
                stage.n_tasks,
                &hash_expr,
                &codec,
            )?;

            let mut client = FlightServiceClient::new(channel);
            let stream = client
                .do_get(ticket.into_request())
                .await
                .map_err(|err| {
                    tonic_status_to_datafusion_error(&err)
                        .unwrap_or_else(|| DataFusionError::External(Box::new(err)))
                })?
                .into_inner()
                .map_err(|err| FlightError::Tonic(Box::new(err)));

            Ok(
                FlightRecordBatchStream::new_from_flight_data(stream).map_err(|err| match err {
                    FlightError::Tonic(status) => tonic_status_to_datafusion_error(&status)
                        .unwrap_or_else(|| DataFusionError::External(Box::new(status))),
                    err => DataFusionError::External(Box::new(err)),
                }),
            )
        }
        .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
