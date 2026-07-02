use crate::common::{TreeNodeExt, deserialize_uuid};
use crate::execution_plans::SamplerExec;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::datafusion_error_to_tonic_status;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, set_work_unit_received_time};
use crate::worker::LocalWorkerContext;
use crate::worker::generated::worker::coordinator_to_worker_msg::Inner;
use crate::worker::generated::worker::set_plan_request::WorkUnitFeedDeclaration;
use crate::worker::generated::worker::worker_service_server::WorkerService;
use crate::worker::generated::worker::{
    CoordinatorToWorkerMsg, TaskMetrics, WorkerToCoordinatorMsg, worker_to_coordinator_msg,
};
use crate::worker::task_data::TaskDataMetrics;
use crate::{
    DistributedCodec, DistributedConfig, DistributedExt, DistributedTaskContext, TaskData, Worker,
    WorkerQueryContext,
};
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryStreamExt};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tonic::{Request, Response, Status, Streaming};
use url::Url;

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
        let mut load_info_rxs = vec![];

        let task_data = || async {
            let headers = grpc_headers.into_headers();

            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                }))
                .with_extension(Arc::new(LocalWorkerContext {
                    task_data_entries: Arc::clone(&self.task_data_entries),
                    self_url: Url::parse(&request.target_worker_url)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
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
                plan = hook(plan, session_state.config())?;
            }
            load_info_rxs =
                SamplerExec::kick_off_first_sampler(Arc::clone(&plan), Arc::clone(&task_ctx))?;

            // Initialize partition count to the number of partitions in the stage
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

        let task_data_result = task_data().await.map_err(Arc::new);

        entry
            .write(task_data_result.clone())
            .map_err(|_| Status::internal(format!(
                "Logic error while setting plan for TaskKey {key:?}: the plan was set twice. This is a bug in datafusion-distributed, please report it."
            )))?;

        let task_data = task_data_result
            .map_err(DataFusionError::Shared)
            .map_err(datafusion_error_to_tonic_status)?;

        // Continue reading remaining messages (work unit feed data) in the background.
        let mut work_unit_senders = Some(remote_work_unit_feed_registry.senders);
        let task_data_entries = Arc::clone(&self.task_data_entries);

        // This tokio task takes ownership of the `oneshot::Sender<pb::TaskMetrics>` that keeps
        // alive the worker->coordinator stream. as soon as this task ends, the runtime metrics
        // are send back and the worker->coordinator stream ends. The flow is the following:
        // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
        // 2. In DistributedExec::execute(), the end query guard is dropped.
        // 3. In StageCoordinator::send_plan_task(), `end_stream_notifier` fires and the
        //    coordinator->worker channel is gracefully ended.
        // 4. The coordinator->worker channel EOS is received by this same function, ending the
        //    while loop inside this `tokio::spawn` below.
        // 5. The metrics are send back in the worker->coordinator channel, and then that channel
        //    is closed.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut body = body.map_ok(set_work_unit_received_time);
            while let Some(Ok(msg)) = body.next().await {
                let Some(msg) = msg.inner else {
                    continue;
                };
                match msg {
                    Inner::SetPlanRequest(_) => {
                        // SetPlanRequest should be the first already polled message in the stream,
                        // if some reached here it means that something is wrong.
                        continue;
                    }
                    Inner::WorkUnitBatch(msg) => {
                        let Some(work_unit_senders) = work_unit_senders.as_mut() else {
                            continue;
                        };
                        for wu in msg.batch {
                            let Ok(id) = deserialize_uuid(&wu.id) else {
                                continue;
                            };
                            let partition = wu.partition as usize;
                            let Some(tx) = work_unit_senders.get(&(id, partition)) else {
                                continue;
                            };
                            if tx.send(Ok(wu)).is_err() {
                                // Channel closed, this sender needs to be dropped, as none will ever
                                // be listening on the other side.
                                work_unit_senders.remove(&(id, partition));
                                continue;
                            }
                        }
                    }
                    Inner::WorkUnitEos(_) => {
                        // No further work unit message will be received here, so drop all the
                        // sender sides so that receiver sides see an EOS upon draining the
                        // remaining messages.
                        //
                        // The [WorkUnitEos] message just applies work units, and it's not a global
                        // EOS signal for the coordinator->worker stream, as there might be more
                        // messages of different nature in that stream.
                        let _ = work_unit_senders.take();
                    }
                }
            }

            let metrics_tx = task_data.metrics_tx.lock().unwrap().take();
            if let Some(Ok(plan)) = task_data.final_plan.get() {
                let d_ctx = DistributedTaskContext {
                    task_index: key.task_number as usize,
                    task_count: request.task_count as usize,
                };
                let task_data_metrics = &task_data.task_data_metrics;
                task_data_metrics.mark_execution_finished();
                if let Some(metrics_tx) = metrics_tx {
                    send_metrics_via_channel(metrics_tx, plan, d_ctx, task_data_metrics);
                }
            }
            task_data_entries.invalidate(&key).await
        });

        let load_info_stream = FuturesUnordered::from_iter(load_info_rxs)
            .filter_map(async |load_info_or_channel_dropped| {
                // This error can only happen if the pb::LoadInfo sender was dropped, which is fine.
                let load_info = load_info_or_channel_dropped.ok()?;
                Some(Ok(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::LoadInfo(load_info)),
                }))
            })
            .chain(futures::stream::once(async move {
                Ok(WorkerToCoordinatorMsg {
                    inner: Some(worker_to_coordinator_msg::Inner::LoadInfoEos(true)),
                })
            }));

        // Stream back metrics when the coordinator channel reaches EOS. At that point the
        // coordinator has closed the query-scoped request stream, so any remaining task state can
        // be finalized even if some partition streams were not dropped through the normal path.
        let metrics_stream = metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(async |task_metrics_or_channel_dropped| {
            let task_metrics = task_metrics_or_channel_dropped.ok()?;
            Some(Ok(WorkerToCoordinatorMsg {
                inner: Some(worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics)),
            }))
        });

        Ok(Response::new(
            futures::stream::select(load_info_stream, metrics_stream).boxed(),
        ))
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> Status {
    move || Status::invalid_argument(format!("Missing field '{field}'"))
}

/// Collects metrics from the plan in pre-order traversal order and sends them via the
/// coordinator channel oneshot.
fn send_metrics_via_channel(
    metrics_tx: Sender<TaskMetrics>,
    plan: &Arc<dyn ExecutionPlan>,
    dt_ctx: DistributedTaskContext,
    task_data_metrics: &Arc<TaskDataMetrics>,
) {
    let mut pre_order_plan_metrics = vec![];
    let _ = plan.apply_with_dt_ctx(dt_ctx, |node, _| {
        pre_order_plan_metrics.push(
            node.metrics()
                .and_then(|m| df_metrics_set_to_proto(&m).ok())
                .unwrap_or_default(),
        );
        Ok(TreeNodeRecursion::Continue)
    });

    // Ignore send errors — the coordinator channel may have been dropped (e.g. query cancelled).
    let _ = metrics_tx.send(TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: Some(task_data_metrics.to_proto_metrics_set()),
    });
}
