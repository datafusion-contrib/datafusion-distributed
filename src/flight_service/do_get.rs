use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::errors::datafusion_error_to_tonic_status;
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::plan::DistributedCodec;
use crate::stage::{stage_from_proto, ExecutionStageProto};
use crate::user_provided_codec::get_user_codec;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::SessionContext;
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
        let state_builder = self
            .session_builder
            .session_state_builder(state_builder)
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let state = state_builder.build();
        let mut state = self
            .session_builder
            .session_state(state)
            .await
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let function_registry = state.function_registry().ok_or(Status::invalid_argument(
            "FunctionRegistry not present in newly built SessionState",
        ))?;

        let mut combined_codec = ComposedPhysicalExtensionCodec::default();
        combined_codec.push(DistributedCodec);
        if let Some(ref user_codec) = get_user_codec(state.config()) {
            combined_codec.push_arc(Arc::clone(&user_codec));
        }

        let stage = stage_from_proto(
            stage_msg,
            function_registry,
            &self.runtime.as_ref(),
            &combined_codec,
        )
        .map_err(|err| Status::invalid_argument(format!("Cannot decode stage proto: {err}")))?;
        let inner_plan = Arc::clone(&stage.plan);

        // Add the extensions that might be required for ExecutionPlan nodes in the plan
        let config = state.config_mut();
        config.set_extension(Arc::clone(&self.channel_manager));
        config.set_extension(Arc::new(stage));

        let ctx = SessionContext::new_with_state(state);

        let ctx = self
            .session_builder
            .session_context(ctx)
            .await
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let stream = inner_plan
            .execute(doget.partition as usize, ctx.task_ctx())
            .map_err(|err| Status::internal(format!("Error executing stage plan: {err:#?}")))?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(inner_plan.schema().clone())
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
