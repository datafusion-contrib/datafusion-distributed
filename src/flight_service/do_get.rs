use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::context::StageTaskContext;
use crate::errors::datafusion_error_to_tonic_status;
use crate::flight_service::service::ArrowFlightEndpoint;
use crate::plan::ArrowFlightReadExecProtoCodec;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_physical_exprs;
use datafusion_proto::physical_plan::to_proto::serialize_physical_exprs;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use futures::TryStreamExt;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoGet {
    #[prost(oneof = "DoGetInner", tags = "1")]
    pub inner: Option<DoGetInner>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DoGetInner {
    #[prost(message, tag = "1")]
    RemotePlanExec(RemotePlanExec),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemotePlanExec {
    #[prost(message, optional, boxed, tag = "1")]
    pub plan: Option<Box<PhysicalPlanNode>>,
    #[prost(string, tag = "2")]
    pub stage_id: String,
    #[prost(uint64, tag = "3")]
    pub task_idx: u64,
    #[prost(uint64, tag = "4")]
    pub output_task_idx: u64,
    #[prost(uint64, tag = "5")]
    pub output_tasks: u64,
    #[prost(message, repeated, tag = "6")]
    pub hash_expr: Vec<PhysicalExprNode>,
}

impl DoGet {
    pub fn new_remote_plan_exec_ticket(
        plan: Arc<dyn ExecutionPlan>,
        stage_id: Uuid,
        task_idx: usize,
        output_task_idx: usize,
        output_tasks: usize,
        hash_expr: &[Arc<dyn PhysicalExpr>],
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Ticket, DataFusionError> {
        let node = PhysicalPlanNode::try_from_physical_plan(plan, extension_codec)?;
        let do_get = Self {
            inner: Some(DoGetInner::RemotePlanExec(RemotePlanExec {
                plan: Some(Box::new(node)),
                stage_id: stage_id.to_string(),
                task_idx: task_idx as u64,
                output_task_idx: output_task_idx as u64,
                output_tasks: output_tasks as u64,
                hash_expr: serialize_physical_exprs(hash_expr, extension_codec)?,
            })),
        };
        Ok(Ticket::new(do_get.encode_to_vec()))
    }
}

impl ArrowFlightEndpoint {
    pub(super) async fn get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoGetStream>, Status> {
        let Ticket { ticket } = request.into_inner();
        let action = DoGet::decode(ticket).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode DoGet message: {err}"))
        })?;

        let Some(action) = action.inner else {
            return invalid_argument("DoGet message is empty");
        };

        let DoGetInner::RemotePlanExec(action) = action;

        let state_builder = SessionStateBuilder::new()
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_default_features();

        let mut state = self.session_builder.on_new_session(state_builder).build();

        let Some(function_registry) = state.function_registry() else {
            return invalid_argument("FunctionRegistry not present in newly built SessionState");
        };

        let Some(plan_proto) = action.plan else {
            return invalid_argument("RemotePlanExec is missing the plan");
        };

        let mut codec = ComposedPhysicalExtensionCodec::default();
        codec.push(ArrowFlightReadExecProtoCodec);
        codec.push_from_config(state.config());

        let plan = plan_proto
            .try_into_physical_plan(function_registry, &self.runtime, &codec)
            .map_err(|err| Status::internal(format!("Cannot deserialize plan: {err}")))?;

        let stage_id = Uuid::parse_str(&action.stage_id).map_err(|err| {
            Status::invalid_argument(format!(
                "Cannot parse stage id '{}': {err}",
                action.stage_id
            ))
        })?;

        let task_idx = action.task_idx as usize;
        let caller_actor_idx = action.output_task_idx as usize;
        let prev_n = action.output_tasks as usize;
        let partitioning = match parse_physical_exprs(
            &action.hash_expr,
            function_registry,
            &plan.schema(),
            &codec,
        ) {
            Ok(expr) if expr.is_empty() => Partitioning::Hash(expr, prev_n),
            Ok(_) => Partitioning::RoundRobinBatch(prev_n),
            Err(err) => return invalid_argument(format!("Cannot parse hash expressions {err}")),
        };

        let config = state.config_mut();
        config.set_extension(Arc::clone(&self.channel_manager));
        config.set_extension(Arc::new(StageTaskContext { task_idx }));

        let stream_partitioner = self
            .partitioner_registry
            .get_or_create_stream_partitioner(stage_id, task_idx, plan, partitioning)
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let stream = stream_partitioner
            .execute(caller_actor_idx, state.task_ctx())
            .map_err(|err| datafusion_error_to_tonic_status(&err))?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(stream_partitioner.schema())
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
