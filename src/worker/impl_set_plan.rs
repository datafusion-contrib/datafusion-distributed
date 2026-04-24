use crate::common::deserialize_uuid;
use crate::config_extension_ext::set_distributed_option_extension_from_headers;
use crate::protobuf::DistributedCodec;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, RemoteWorkUnitFeedTxs};
use crate::worker::generated::worker::{MetricsCollection, SetPlanRequest};
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::{DistributedConfig, DistributedTaskContext, Worker, WorkerQueryContext};
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::oneshot;
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
    /// Sender half of the metrics channel. `impl_execute_task` takes this (via `Option::take`)
    /// once all partitions have finished or been dropped, sending the collected metrics back to
    /// the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<MetricsCollection>>>>,
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
    /// Sets the plan for a task and returns a receiver that will yield the collected metrics
    /// once all partitions of that task have finished executing (or been dropped early).
    pub(crate) async fn impl_set_plan(
        &self,
        request: SetPlanRequest,
        grpc_headers: MetadataMap,
    ) -> Result<(RemoteWorkUnitFeedTxs, oneshot::Receiver<MetricsCollection>), Status> {
        let key = request.task_key.ok_or_else(missing("task_key"))?;

        let entry = self
            .task_data_entries
            .get_with(key.clone(), async { Default::default() })
            .await;

        let mut remote_work_unit_feed_registry = RemoteWorkUnitFeedRegistry::default();
        for WorkUnitFeedDeclaration { id, partitions } in &request.work_unit_feed_declarations {
            if let Ok(id) = deserialize_uuid(id) {
                remote_work_unit_feed_registry.add(id, *partitions as usize);
            }
        }

        let (metrics_tx, metrics_rx) = oneshot::channel();

        let task_data = || async {
            let headers = grpc_headers.into_headers();

            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                }));
            set_distributed_option_extension_from_headers::<DistributedConfig>(&mut cfg, &headers)?;

            let shuffle_batch_size =
                DistributedConfig::from_config_options(cfg.options())?.shuffle_batch_size;
            if shuffle_batch_size != 0 {
                cfg = cfg.with_batch_size(shuffle_batch_size);
            }

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
                metrics_tx: Arc::new(std::sync::Mutex::new(Some(metrics_tx))),
            })
        };

        entry.write(task_data().await.map_err(Arc::new)).map_err(|_| {
            Status::internal(format!(
                "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            ))
        })?;
        Ok((remote_work_unit_feed_registry.senders, metrics_rx))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}
