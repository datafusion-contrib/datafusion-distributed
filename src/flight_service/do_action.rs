use crate::config_extension_ext::set_distributed_option_extension_from_headers;
use crate::protobuf::{DistributedCodec, datafusion_error_to_tonic_status};
use crate::{DistributedConfig, StageKey, Worker, WorkerQueryContext};
use arrow_flight::Action;
use arrow_flight::flight_service_server::FlightService;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tonic::{Request, Response, Status};

pub(crate) const INIT_ACTION_TYPE: &str = "init";

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitAction {
    /// The ExecutionPlan we are going to execute encoded as protobuf bytes.
    #[prost(bytes, tag = "1")]
    pub plan_proto: Bytes,
    /// The stage key that identifies the stage.  This is useful to keep
    /// outside of the stage proto as it is used to store the stage
    /// and we may not need to deserialize the entire stage proto
    /// if we already have stored it
    #[prost(message, optional, tag = "2")]
    pub stage_key: Option<StageKey>,
}

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(super) task_ctx: Arc<TaskContext>,
    /// Plan to be executed.
    pub(super) plan: Arc<dyn ExecutionPlan>,
    /// `num_partitions_remaining` is initialized to the total number of partitions in the task (not
    /// only tasks in the partition group). This is decremented for each request to the endpoint
    /// for this task. Once this count is zero, the task is likely complete. The task may not be
    /// complete because it's possible that the same partition was retried and this count was
    /// decremented more than once for the same partition.
    pub(super) num_partitions_remaining: Arc<AtomicUsize>,
}

impl TaskData {
    /// Returns the number of partitions remaining to be processed.
    pub(crate) fn num_partitions_remaining(&self) -> usize {
        self.num_partitions_remaining
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the total number of partitions in this task.
    pub(crate) fn total_partitions(&self) -> usize {
        self.plan.properties().partitioning.partition_count()
    }
}

impl Worker {
    pub(super) async fn init(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Worker as FlightService>::DoActionStream>, Status> {
        let (metadata, _ext, body) = request.into_parts();
        let init = InitAction::decode(body.body).map_err(|err| {
            Status::invalid_argument(format!("Cannot decode InitAction message: {err}"))
        })?;
        let key = init.stage_key.ok_or_else(missing("stage_key"))?;

        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        let task_data = || async {
            let headers = metadata.into_headers();

            let mut cfg = SessionConfig::default();
            set_distributed_option_extension_from_headers::<DistributedConfig>(&mut cfg, &headers)?;
            let session_state = self
                .session_builder
                .build_session_state(WorkerQueryContext {
                    builder: SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(cfg)
                        .with_runtime_env(Arc::clone(&self.runtime)),
                    headers,
                })
                .await?;

            let codec = DistributedCodec::new_combined_with_user(session_state.config());
            let task_ctx = session_state.task_ctx();
            let proto_node = PhysicalPlanNode::try_decode(init.plan_proto.as_ref())?;
            let mut plan = proto_node.try_into_physical_plan(&task_ctx, &codec)?;

            for hook in self.hooks.on_plan.iter() {
                plan = hook(plan)
            }

            // Initialize partition count to the number of partitions in the stage
            let total_partitions = plan.properties().partitioning.partition_count();
            Ok::<_, DataFusionError>(TaskData {
                plan,
                task_ctx,
                num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
            })
        };

        entry.write(task_data().await.map_err(datafusion_error_to_tonic_status)).map_err(|_| {
            Status::internal(format!(
                "Logic error while setting plan for Stage key {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            ))
        })?;
        Ok(Response::new(futures::stream::empty().boxed()))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}
