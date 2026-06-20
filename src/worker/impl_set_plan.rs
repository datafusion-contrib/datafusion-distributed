use crate::common::deserialize_uuid;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, RemoteWorkUnitFeedTxs};
use crate::worker::generated::worker as pb;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::task_data::TaskDataMetrics;
use crate::worker::worker_service::Worker;
use crate::{
    DistributedCodec, DistributedConfig, DistributedExt, DistributedTaskContext, TaskData,
    WorkerQueryContext,
};
use datafusion::common::{DataFusionError, Result, exec_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use http::HeaderMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot;

/// What a transport gets back from [Worker::set_task_plan]: the channels to push work units into,
/// and the one-shot receiver that resolves with the task's metrics once every partition finished
/// or was dropped.
pub(crate) struct SetPlanOutcome {
    pub(crate) work_unit_senders: RemoteWorkUnitFeedTxs,
    pub(crate) metrics_rx: oneshot::Receiver<pb::TaskMetrics>,
}

impl Worker {
    /// Stores one task's plan so [`super::impl_execute_task::execute_local_task`] can pick it up:
    /// builds the worker-side session through this worker's [`crate::WorkerSessionBuilder`],
    /// decodes the plan against it, and publishes the resulting [TaskData] under its task key.
    ///
    /// This is the transport-neutral core of plan delivery. The Flight service wraps it in its
    /// coordinator gRPC stream; an in-process transport calls it directly. `customize_cfg` lets the
    /// caller attach transport-specific session extensions (Flight adds its local-bypass context)
    /// without this method knowing about them.
    pub(crate) async fn set_task_plan(
        &self,
        request: pb::SetPlanRequest,
        headers: HeaderMap,
        customize_cfg: impl FnOnce(SessionConfig) -> Result<SessionConfig>,
    ) -> Result<SetPlanOutcome> {
        let key = request
            .task_key
            .clone()
            .ok_or_else(|| exec_datafusion_err!("Missing field 'task_key'"))?;

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

        let task_data = async {
            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                }));
            cfg = customize_cfg(cfg)?;
            cfg =
                cfg.with_distributed_option_extension_from_headers::<DistributedConfig>(&headers)?;

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
                plan = hook(plan, session_state.config())?;
            }

            // Initialize partition count to the number of partitions in the stage.
            let total_partitions = plan.properties().partitioning.partition_count();
            Ok::<_, DataFusionError>(TaskData {
                base_plan: plan,
                final_plan: Arc::new(OnceLock::new()),
                task_ctx,
                num_partitions_remaining: Arc::new(AtomicUsize::new(total_partitions)),
                metrics_tx: match collect_metrics {
                    true => Arc::new(std::sync::Mutex::new(Some(metrics_tx))),
                    false => Arc::new(std::sync::Mutex::new(None)),
                },
                task_data_metrics: Arc::new(TaskDataMetrics::new(request.query_start_time_ns)),
            })
        };

        entry.write(task_data.await.map_err(Arc::new)).map_err(|_| {
            exec_datafusion_err!(
                "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            )
        })?;

        Ok(SetPlanOutcome {
            work_unit_senders: remote_work_unit_feed_registry.senders,
            metrics_rx,
        })
    }
}
