use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::errors::datafusion_error_to_tonic_status;
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::plan::{DistributedCodec, PartitionGroup};
use crate::stage::{stage_from_proto, ExecutionStage, ExecutionStageProto};
use crate::user_provided_codec::get_user_codec;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::optimizer::OptimizerConfig;
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::service::StageKey;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    /// The ExecutionStage that we are going to execute
    #[prost(message, optional, tag = "1")]
    pub stage_proto: Option<ExecutionStageProto>,
    /// The index to the task within the stage that we want to execute
    #[prost(uint64, tag = "2")]
    pub task_number: u64,
    /// the partition number we want to execute
    #[prost(uint64, tag = "3")]
    pub partition: u64,
    /// The stage key that identifies the stage.  This is useful to keep
    /// outside of the stage proto as it is used to store the stage
    /// and we may not need to deserialize the entire stage proto
    /// if we already have stored it
    #[prost(message, optional, tag = "4")]
    pub stage_key: Option<StageKey>,
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

        let partition = doget.partition as usize;
        let task_number = doget.task_number as usize;
        let (mut state, stage) = self.get_state_and_stage(doget).await?;

        // find out which partition group we are executing
        let task = stage
            .tasks
            .get(task_number)
            .ok_or(Status::invalid_argument(format!(
                "Task number {} not found in stage {}",
                task_number,
                stage.name()
            )))?;

        let partition_group =
            PartitionGroup(task.partition_group.iter().map(|p| *p as usize).collect());
        state.config_mut().set_extension(Arc::new(partition_group));

        let inner_plan = stage.plan.clone();

        let stream = inner_plan
            .execute(partition, state.task_ctx())
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

    async fn get_state_and_stage(
        &self,
        doget: DoGet,
    ) -> Result<(SessionState, Arc<ExecutionStage>), Status> {
        let key = doget
            .stage_key
            .ok_or(Status::invalid_argument("DoGet is missing the stage key"))?;
        let once_stage = self.stages.entry(key).or_default();

        let (state, stage) = once_stage
            .get_or_try_init(|| async {
                let stage_proto = doget
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

                let function_registry =
                    state.function_registry().ok_or(Status::invalid_argument(
                        "FunctionRegistry not present in newly built SessionState",
                    ))?;

                let mut combined_codec = ComposedPhysicalExtensionCodec::default();
                combined_codec.push(DistributedCodec);
                if let Some(ref user_codec) = get_user_codec(state.config()) {
                    combined_codec.push_arc(Arc::clone(user_codec));
                }

                let stage = stage_from_proto(
                    stage_proto,
                    function_registry,
                    self.runtime.as_ref(),
                    &combined_codec,
                )
                .map(Arc::new)
                .map_err(|err| {
                    Status::invalid_argument(format!("Cannot decode stage proto: {err}"))
                })?;

                // Add the extensions that might be required for ExecutionPlan nodes in the plan
                let config = state.config_mut();
                config.set_extension(Arc::clone(&self.channel_manager));
                config.set_extension(stage.clone());

                Ok::<_, Status>((state, stage))
            })
            .await?;

        Ok((state.clone(), stage.clone()))
    }
}
