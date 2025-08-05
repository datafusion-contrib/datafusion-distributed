use crate::errors::datafusion_error_to_tonic_status;
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::plan::DistributedCodec;
use crate::stage::{stage_from_proto, ExecutionStageProto};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The ExecutionStage that we are going to execute
    #[prost(message, optional, tag = "1")]
    pub stage_proto: Option<ExecutionStageProto>,
    /// the partition of the stage to execute
    #[prost(uint64, tag = "2")]
    pub partition: u64,
}

impl ArrowFlightEndpoint {
    pub(super) async fn get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoGetStream>, Status> {
        let Ticket { ticket } = request.into_inner();
        let doget = DoGet::decode(ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let stage_msg = doget
            .stage_proto
            .ok_or(Status::invalid_argument("DoGet is missing the stage proto"))?;

        let state_builder = SessionStateBuilder::new()
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_default_features();

        let mut state = self.session_builder.on_new_session(state_builder).build();

        let function_registry = state.function_registry().ok_or(Status::invalid_argument(
            "FunctionRegistry not present in newly built SessionState",
        ))?;

        let codec = DistributedCodec {};
        let codec = Arc::new(codec) as Arc<dyn PhysicalExtensionCodec>;

        let stage = stage_from_proto(stage_msg, function_registry, &self.runtime.as_ref(), codec)
            .map(Arc::new)
            .map_err(|err| Status::invalid_argument(format!("Cannot decode stage proto: {err}")))?;

        // Add the extensions that might be required for ExecutionPlan nodes in the plan
        let config = state.config_mut();
        config.set_extension(Arc::clone(&self.channel_manager));
        config.set_extension(stage.clone());

        let stream = stage
            .plan
            .execute(doget.partition as usize, state.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(stage.plan.schema().clone())
            .build(stream.map_err(|err| {
                FlightError::Tonic(Box::new(datafusion_error_to_tonic_status(&err)))
            }));

        Ok(Response::new(Box::pin(flight_data_stream.map_err(
            |err| match err {
                FlightError::Tonic(status) => *status,
                _ => Status::internal(format!("Error during flight stream: {err}")),
            },
        ))))
    }
}

fn invalid_argument<T>(msg: impl Into<String>) -> Result<T, Status> {
    Err(Status::invalid_argument(msg))
}
