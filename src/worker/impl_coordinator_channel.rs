use crate::common::{TreeNodeExt, discover_dynamic_filter_consumers};
use crate::events::{WorkerPlanRewriteEvent, WorkerPlanRewriteHandlers};
use crate::execution_plans::SamplerExec;
use crate::work_unit_feed::{RemoteWorkUnitFeedRegistry, set_work_unit_received_time};
use crate::worker::LocalWorkerContext;
use crate::worker::task_data::TaskDataMetrics;
use crate::{
    CoordinatorToWorkerMsg, DistributedCodec, DistributedConfig, DistributedExt,
    DistributedTaskContext, TaskData, TaskDynamicFilter, TaskDynamicFilters, TaskMetrics, Worker,
    WorkerQueryContext, WorkerToCoordinatorMsg,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, HashSet, Result, exec_datafusion_err, internal_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DeduplicatingProtoConverter, PhysicalPlanNodeExt,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt, TryStreamExt};
use http::HeaderMap;
use prost::Message;
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

impl Worker {
    pub async fn coordinator_channel(
        &self,
        headers: HeaderMap,
        mut stream: BoxStream<'static, Result<CoordinatorToWorkerMsg>>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        // The first message must be a SetPlanRequest.
        let Some(msg) = stream.try_next().await? else {
            return internal_err!("Empty Coordinator stream");
        };

        let CoordinatorToWorkerMsg::SetPlanRequest(request) = msg else {
            return internal_err!("First Coordinator message must be SetPlanRequest");
        };

        let key = request.task_key;

        let entry = self
            .task_data_entries
            .get_with(key, async { Default::default() })
            .await;

        let mut remote_work_unit_feed_registry = RemoteWorkUnitFeedRegistry::default();
        for decl in request.work_unit_feed_declarations {
            remote_work_unit_feed_registry.add(decl.id, decl.partitions);
        }

        let (metrics_tx, metrics_rx) = oneshot::channel();
        let (dynamic_filters_tx, dynamic_filters_rx) = oneshot::channel();
        let mut load_info_rxs = vec![];

        let task_data = || async {
            let mut cfg = SessionConfig::default()
                .with_extension(Arc::new(remote_work_unit_feed_registry.receivers))
                .with_extension(Arc::new(DistributedTaskContext {
                    task_index: request.task_key.task_number,
                    task_count: request.task_count,
                }))
                .with_extension(Arc::new(LocalWorkerContext {
                    task_data_entries: Arc::clone(&self.task_data_entries),
                    self_url: request.target_worker_url,
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
            let ev = WorkerPlanRewriteEvent {
                plan: proto_node.try_into_physical_plan_with_converter(
                    &task_ctx,
                    &codec,
                    &DeduplicatingProtoConverter::default(),
                )?,
                session_config: session_state.config(),
            };
            let plan = WorkerPlanRewriteHandlers::handle(ev)?.plan;
            load_info_rxs =
                SamplerExec::kick_off_first_sampler(Arc::clone(&plan), Arc::clone(&task_ctx))?;

            // Initialize partition count to the number of partitions in the stage
            Ok::<_, DataFusionError>(TaskData {
                base_plan: plan,
                final_plan: Arc::new(OnceLock::new()),
                task_ctx,
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
            .map_err(|e| exec_datafusion_err!("{e}"))?;

        let task_data = task_data_result.map_err(DataFusionError::Shared)?;

        // Continue reading remaining messages (work unit feed data) in the background.
        let mut work_unit_senders = Some(remote_work_unit_feed_registry.senders);
        let task_data_entries = Arc::clone(&self.task_data_entries);
        let dynamic_filter_ids: HashSet<_> = request.dynamic_filter_ids.iter().copied().collect();

        // This tokio task takes ownership of the final-report senders that keep the
        // worker->coordinator stream alive. As soon as this task ends, the runtime metrics and
        // final dynamic filters are sent back and the worker->coordinator stream ends. The flow
        // is the following:
        // 1. The query ends normally, as all Arrow RecordBatches are already streamed.
        // 2. In DistributedExec::execute(), the end query guard is dropped.
        // 3. In StageCoordinator::send_plan_task(), `end_stream_notifier` fires and the
        //    coordinator->worker channel is gracefully ended.
        // 4. The coordinator->worker channel EOS is received by this same function, ending the
        //    while loop inside this `tokio::spawn` below.
        // 5. The metrics and final dynamic filters are sent back in the worker->coordinator
        //    channel, and then that channel is closed.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            let mut stream = stream.map_ok(set_work_unit_received_time);
            while let Some(Ok(msg)) = stream.next().await {
                match msg {
                    CoordinatorToWorkerMsg::SetPlanRequest(_) => {
                        // SetPlanRequest should be the first already polled message in the stream,
                        // if some reached here it means that something is wrong.
                        continue;
                    }
                    CoordinatorToWorkerMsg::WorkUnitBatch(work_unit_batch) => {
                        let Some(work_unit_senders) = work_unit_senders.as_mut() else {
                            continue;
                        };
                        for wu in work_unit_batch.batch {
                            let id = wu.id;
                            let partition = wu.partition;
                            let Some(tx) = work_unit_senders.get(&(wu.id, partition)) else {
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
                    CoordinatorToWorkerMsg::WorkUnitEos => {
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
            let mut dynamic_filters = TaskDynamicFilters::default();
            if let Some(Ok(plan)) = task_data.final_plan.get() {
                let d_ctx = DistributedTaskContext {
                    task_index: key.task_number,
                    task_count: request.task_count,
                };
                let task_data_metrics = &task_data.task_data_metrics;
                task_data_metrics.mark_execution_finished();
                if let Some(metrics_tx) = metrics_tx {
                    send_metrics_via_channel(metrics_tx, plan, d_ctx, task_data_metrics);
                }
                dynamic_filters =
                    build_task_dynamic_filters(plan, &dynamic_filter_ids, &task_data.task_ctx)
                        .unwrap_or_default();
            }
            let _ = dynamic_filters_tx.send(dynamic_filters);
            task_data_entries.invalidate(&key).await
        });

        let load_info_stream = FuturesUnordered::from_iter(load_info_rxs)
            .filter_map(async |load_info_or_channel_dropped| {
                // This error can only happen if the LoadInfo sender was dropped, which is fine.
                let load_info = load_info_or_channel_dropped.ok()?;
                Some(WorkerToCoordinatorMsg::LoadInfo(load_info))
            })
            .chain(futures::stream::once(async move {
                WorkerToCoordinatorMsg::LoadInfoEos
            }));

        // Stream back metrics when the coordinator channel reaches EOS. At that point the
        // coordinator has closed the query-scoped request stream, so any remaining task state can
        // be finalized even if some partition streams were not dropped through the normal path.
        let metrics_stream = metrics_rx.into_stream();
        let metrics_stream = metrics_stream.filter_map(async |task_metrics_or_channel_dropped| {
            let task_metrics = task_metrics_or_channel_dropped.ok()?;
            Some(WorkerToCoordinatorMsg::TaskMetrics(task_metrics))
        });

        let dynamic_filters_stream = dynamic_filters_rx.into_stream().filter_map(
            async |dynamic_filters_or_channel_dropped| {
                let dynamic_filters = dynamic_filters_or_channel_dropped.ok()?;
                Some(WorkerToCoordinatorMsg::TaskDynamicFilters(dynamic_filters))
            },
        );

        Ok(futures::stream::select(
            load_info_stream,
            futures::stream::select(metrics_stream, dynamic_filters_stream),
        )
        .map(Ok)
        .boxed())
    }
}

fn build_task_dynamic_filters(
    plan: &Arc<dyn ExecutionPlan>,
    allowed_ids: &HashSet<u64>,
    task_ctx: &Arc<datafusion::execution::TaskContext>,
) -> Result<TaskDynamicFilters> {
    let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
    let mut filters = vec![];
    for consumer in discover_dynamic_filter_consumers(plan, Some(allowed_ids))? {
        // Serializing the complete DynamicFilterPhysicalExpr preserves both its current
        // predicate and its completion state through DataFusion's native proto hook.
        let expression = serialize_physical_expr(&consumer.expression, &codec)?;
        let Some(ExprType::DynamicFilter(dynamic_filter)) = expression.expr_type.as_ref() else {
            return internal_err!("discovered dynamic filter did not serialize as one");
        };
        // A cancelled or short-circuited task can leave filters incomplete. Do not report those
        // as final values for display.
        if dynamic_filter.is_complete {
            filters.push(TaskDynamicFilter {
                expression_id: consumer.id,
                expression: expression.encode_to_vec(),
            });
        }
    }
    Ok(TaskDynamicFilters { filters })
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
        pre_order_plan_metrics.push(node.metrics().unwrap_or_default());
        Ok(TreeNodeRecursion::Continue)
    });

    // Ignore send errors — the coordinator channel may have been dropped (e.g. query cancelled).
    let _ = metrics_tx.send(TaskMetrics {
        pre_order_plan_metrics,
        task_metrics: task_data_metrics.to_metrics_set(),
    });
}
