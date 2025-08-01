use crate::channel_manager::{ArrowFlightChannel, ChannelManager};
//use crate::flight_service::{DoGet, DoPut};
use crate::plan::arrow_flight_read_proto::ArrowFlightReadExecProtoCodec;
//use crate::stage_delegation::{ActorContext, StageContext, StageDelegation};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{exec_datafusion_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::IntoRequest;
use url::{ParseError, Url};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ArrowFlightReadExec {
    /// The properties we want to expose to the plan.  These are almost
    /// certainly the properties of the stage we are going to consume
    properties: PlanProperties,
    /// the name of the stage we are reading from
    stage_name: String,
}

impl ArrowFlightReadExec {
    pub fn new(properties: &PlanProperties, stage_name: &str) -> Self {
        Self {
            properties: properties.clone(),
            stage_name: stage_name.to_string(),
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ArrowFlightReadExec: {}", self.stage_name)
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
        if children.len() != 1 {
            return plan_err!(
                "ArrowFlightReadExec: wrong number of children, expected 1, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            stage_name: self.stage_name.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        todo!()
        /*let runtime = context.runtime_env();
            let partitioning = self.properties.partitioning.clone();

            let channel_manager = ChannelManager::try_from_session(context.session_config())?;

            let current_actor_opt = context.session_config().get_extension::<ActorContext>();
            let current_stage_opt = context.session_config().get_extension::<StageContext>();
            let stage_delegation_opt = context.session_config().get_extension::<StageDelegation>();
            if current_stage_opt.is_some() && stage_delegation_opt.is_none() {
                return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.");
            }
            if current_stage_opt.is_some() && current_actor_opt.is_none() {
                return internal_err!("No ActorContext extension found in the SessionConfig even though a StageContext was present.");
            }
            let current_actor = current_actor_opt.unwrap_or_default();

            let plan = Arc::clone(&self.child);
            let next_stage_context = Arc::clone(&self.next_stage_context_cell);

            let stream = async move {
                let (next_stage_context, channels) = next_stage_context.get_or_try_init(|| async {
                    if let Some(ref current_stage) = current_stage_opt {
                        if current_actor.actor_idx == current_stage.delegate {
                            // We are inside a stage, and we are the delegate, so need to
                            // build the channels and communicate them.
                            build_next_stage(&channel_manager, Some(current_stage), partitioning).await
                        } else {
                            // We are inside a stage, but we are not the delegate, so we need to
                            // wait for the delegate to tell us what the new channels are.
                            let Some(stage_delegation) = stage_delegation_opt else {
                                return internal_err!("No StageDelegation extension found in the SessionConfig even though a StageContext was present.");
                            };
                            listen_to_next_stage(
                                &channel_manager,
                                &stage_delegation,
                                current_stage.id.clone(),
                                current_actor.actor_idx as usize
                            ).await
                        }
                    } else {
                        // We are not in a stage, the whole thing starts here.
                        build_next_stage(&channel_manager, None, partitioning).await
                    }
                }).await?;

                if let Some(current_stage) = current_stage_opt {
                    if current_actor.actor_idx == current_stage.delegate {
                        // We are the delegate, and it's our duty to communicate the next stage context
                        // to the other actors that are not us. They will be waiting for us to send
                        // them this info.
                        communicate_next_stage(
                            Arc::clone(&channel_manager),
                            current_stage.as_ref().clone(),
                            next_stage_context.clone()
                        ).await?;
                    }
                }

                if partition >= channels.len() {
                    return internal_err!("Invalid channel index {partition} with a total number of {} channels", channels.len());
                }

                let ticket = DoGet::new_remote_plan_exec_ticket(
                    plan,
                    next_stage_context.clone(),
                    ActorContext {
                        caller_actor_idx: current_actor.actor_idx,
                        actor_idx: partition as u64,
                    },
                    // TODO: The user should be able to pass its own extension decoder.
                    &ArrowFlightReadExecProtoCodec::new(&runtime),
                )?;

                let mut client = FlightServiceClient::new(channels[partition].channel.clone());
                let stream = client
                    .do_get(ticket.into_request())
                    .await
                    .map_err(|err| DataFusionError::External(Box::new(err)))?
                    .into_inner()
                    .map_err(|err| FlightError::Tonic(Box::new(err)));

                Ok(FlightRecordBatchStream::new_from_flight_data(stream)
                    // TODO: propagate the error from the service to here, probably serializing it
                    //  somehow.
                    .map_err(|err| DataFusionError::External(Box::new(err))))
            }.try_flatten_stream();

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        */
    }
}
