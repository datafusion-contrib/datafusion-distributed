use crate::common::deserialize_uuid;
use crate::work_unit_feed::RemoteWorkUnitFeedRegistry;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, WorkerToCoordinatorMsg, worker_to_coordinator_msg,
};
use crate::{
    DistributedCodec, DistributedConfig, DistributedExt, DistributedTaskContext, TaskData, Worker,
    WorkerQueryContext,
};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, Streaming};

impl Worker {
    pub(super) async fn impl_coordinator_channel(
        &self,
        request: Request<Streaming<CoordinatorToWorkerMsg>>,
    ) -> Result<Response<<Worker as WorkerService>::CoordinatorChannelStream>, Status> {
        let (grpc_headers, _ext, mut body) = request.into_parts();

        // The first message must be a SetPlanRequest.
        let Some(msg) = body.next().await else {
            return Err(Status::internal("Empty Coordinator stream"));
        };
        let Some(Inner::SetPlanRequest(request)) = msg?.inner else {
            return Err(Status::internal(
                "First Coordinator message must be SetPlanRequest",
            ));
        };
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
                }))
                .with_distributed_option_extension_from_headers::<DistributedConfig>(&headers)?;

            let d_cfg = DistributedConfig::from_config_options(cfg.options())?;
            let shuffle_batch_size = d_cfg.shuffle_batch_size;
            let collect_metrics = d_cfg.collect_metrics;
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
                metrics_tx: match collect_metrics {
                    true => Arc::new(std::sync::Mutex::new(Some(metrics_tx))),
                    false => Arc::new(std::sync::Mutex::new(None)),
                },
            })
        };

        entry.write(task_data().await.map_err(Arc::new)).map_err(|_| {
            Status::internal(format!(
                "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            ))
        })?;

        // Continue reading remaining messages (work unit feed data) in the background.
        let work_unit_senders = remote_work_unit_feed_registry.senders;
        tokio::spawn(async move {
            while let Some(Ok(msg)) = body.next().await {
                let Some(Inner::WorkUnit(msg)) = msg.inner else {
                    continue;
                };
                let Ok(id) = deserialize_uuid(&msg.id) else {
                    continue;
                };
                let Some(tx) = work_unit_senders.get(&(id, msg.partition as usize)) else {
                    continue;
                };
                if tx.send(Ok(msg.body)).is_err() {
                    break; // channel closed
                }
            }
        });

        // Stream back the metrics once the task finishes executing.
        // The oneshot receiver resolves when impl_execute_task sends the collected
        // metrics after all partitions have finished or been dropped.
        let stream = futures::stream::once(async move {
            match metrics_rx.await {
                Ok(task_metrics) => Ok(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics)),
                }),
                Err(_) => Err(Status::internal(
                    "Metrics channel closed before metrics were sent",
                )),
            }
        });
        Ok(Response::new(stream.boxed()))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}
