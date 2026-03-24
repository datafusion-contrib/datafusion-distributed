use crate::config_extension_ext::set_distributed_option_extension_from_headers;
use crate::protobuf::DistributedCodec;
use crate::worker::generated::worker::SetPlanRequest;
use crate::{DistributedConfig, DistributedTaskContext, Worker, WorkerQueryContext};
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tonic::Status;
use tonic::metadata::MetadataMap;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(super) task_ctx: Arc<TaskContext>,
    /// Plan to be executed.
    pub(crate) plan: Arc<dyn ExecutionPlan>,
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
    pub(crate) async fn impl_set_plan(
        &self,
        request: SetPlanRequest,
        metadata: MetadataMap,
    ) -> Result<(), Status> {
        let key = request.task_key.ok_or_else(missing("task_key"))?;

        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        let task_data = || async {
            let headers = metadata.into_headers();

            let mut cfg =
                SessionConfig::default().with_extension(Arc::new(DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                }));
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
            let proto_node = PhysicalPlanNode::try_decode(request.plan_proto.as_ref())?;
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

        entry.write(task_data().await.map_err(Arc::new)).map_err(|_| {
            Status::internal(format!(
                "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            ))
        })?;
        Ok(())
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}
